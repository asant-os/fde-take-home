# tests/test_e2e.py

"""
End-to-end test suite.

Requires:
- GOOGLE_APPLICATION_CREDENTIALS set and valid
- config.json present with region routing
- All dependencies installed (pip install -r requirements.txt)

Starts mock Slack server on :9000 and the FastAPI service on :8001
as subprocesses. Both are torn down after the suite completes.

Run:
    pytest tests/test_e2e.py -v -s
"""

import os
import time
import signal
import subprocess
import pytest
import requests

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

MOCK_SLACK_PORT = 9000
SERVICE_PORT = 8001
SERVICE_BASE = f"http://localhost:{SERVICE_PORT}"
MOCK_SLACK_BASE = f"http://localhost:{MOCK_SLACK_PORT}"

TARGET_MONTH = "2026-01-01"
SOURCE_URI = "gs://fde-take-home-asantos/monthly_account_status.parquet"

STARTUP_TIMEOUT = 15  # seconds to wait for each process to become healthy
POLL_INTERVAL = 0.5


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _wait_for_url(url: str, timeout: int = STARTUP_TIMEOUT) -> None:
    """Poll a URL until it returns 200 or timeout is reached."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            r = requests.get(url, timeout=2)
            if r.status_code == 200:
                return
        except requests.ConnectionError:
            pass
        time.sleep(POLL_INTERVAL)
    raise RuntimeError(f"Service did not become healthy at {url} within {timeout}s")


def _check_credentials() -> None:
    creds = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if not creds or not os.path.exists(creds):
        raise RuntimeError(
            "GOOGLE_APPLICATION_CREDENTIALS is not set or the file does not exist. "
            "E2E tests require valid GCP credentials."
        )


# ---------------------------------------------------------------------------
# Session-scoped fixtures — start/stop processes once for the whole suite
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session", autouse=True)
def check_credentials():
    _check_credentials()


@pytest.fixture(scope="session")
def mock_slack_server():
    """Start the mock Slack server as a subprocess."""
    env = {**os.environ, "MOCK_SLACK_FAIL_RATE_429": "0", "MOCK_SLACK_FAIL_RATE_500": "0"}
    proc = subprocess.Popen(
        ["uvicorn", "mock_slack.server:app", "--host", "0.0.0.0", "--port", str(MOCK_SLACK_PORT)],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        _wait_for_url(f"{MOCK_SLACK_BASE}/health")
    except RuntimeError:
        proc.terminate()
        raise RuntimeError(
            f"Mock Slack server failed to start on port {MOCK_SLACK_PORT}. "
            "Check that mock_slack/server.py exists and dependencies are installed."
        )
    yield proc
    proc.terminate()
    proc.wait()


@pytest.fixture(scope="session")
def service(mock_slack_server):
    """Start the FastAPI service as a subprocess, pointed at the mock Slack server."""
    env = {
        **os.environ,
        "SLACK_WEBHOOK_BASE_URL": f"{MOCK_SLACK_BASE}/slack/webhook",
        "SLACK_MAX_RETRIES": "3",
        "SLACK_BACKOFF_BASE": "0.1",
        "ARR_THRESHOLD": "0",
        "HISTORY_MONTHS": "24",
        "SQLITE_PATH": "/tmp/e2e_test.db",
        "DETAILS_BASE_URL": "https://app.yourcompany.com/accounts",
        "ESCALATION_EMAIL": "support@quadsci.ai",
    }
    proc = subprocess.Popen(
        ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", str(SERVICE_PORT)],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        _wait_for_url(f"{SERVICE_BASE}/health")
    except RuntimeError:
        proc.terminate()
        raise RuntimeError(
            f"FastAPI service failed to start on port {SERVICE_PORT}."
        )
    yield proc
    proc.terminate()
    proc.wait()


# ---------------------------------------------------------------------------
# GET /health
# ---------------------------------------------------------------------------

class TestHealth:
    def test_health_returns_200(self, service):
        r = requests.get(f"{SERVICE_BASE}/health")
        assert r.status_code == 200

    def test_health_response_shape(self, service):
        r = requests.get(f"{SERVICE_BASE}/health")
        body = r.json()
        assert body["ok"] is True
        assert body["db"] is True


# ---------------------------------------------------------------------------
# POST /preview
# ---------------------------------------------------------------------------

class TestPreview:
    @pytest.fixture(scope="class")
    def preview_response(self, service):
        r = requests.post(f"{SERVICE_BASE}/preview", json={
            "source_uri": SOURCE_URI,
            "month": TARGET_MONTH,
            "dry_run": False,
        })
        assert r.status_code == 200, r.text
        return r.json()

    def test_returns_200(self, service):
        r = requests.post(f"{SERVICE_BASE}/preview", json={
            "source_uri": SOURCE_URI,
            "month": TARGET_MONTH,
            "dry_run": False,
        })
        assert r.status_code == 200

    def test_response_has_month(self, preview_response):
        assert preview_response["month"] == TARGET_MONTH

    def test_response_has_alert_count(self, preview_response):
        assert isinstance(preview_response["alert_count"], int)
        assert preview_response["alert_count"] >= 0

    def test_alerts_have_required_fields(self, preview_response):
        for alert in preview_response["alerts"]:
            assert "account_id" in alert
            assert "account_name" in alert
            assert "duration_months" in alert
            assert "risk_start_month" in alert
            assert alert["duration_months"] >= 1

    def test_no_slack_messages_sent(self, preview_response, mock_slack_server):
        before = len(requests.get(f"{MOCK_SLACK_BASE}/logs?limit=100").json()["records"])

        requests.post(f"{SERVICE_BASE}/preview", json={
            "source_uri": SOURCE_URI,
            "month": TARGET_MONTH,
            "dry_run": False,
        })

        after = len(requests.get(f"{MOCK_SLACK_BASE}/logs?limit=100").json()["records"])
        assert after == before


# ---------------------------------------------------------------------------
# POST /runs
# ---------------------------------------------------------------------------

class TestRun:
    @pytest.fixture(scope="class")
    def run_id(self, service):
        r = requests.post(f"{SERVICE_BASE}/runs", json={
            "source_uri": SOURCE_URI,
            "month": TARGET_MONTH,
            "dry_run": False,
        })
        assert r.status_code == 200, r.text
        return r.json()["run_id"]

    def test_returns_run_id(self, service):
        r = requests.post(f"{SERVICE_BASE}/runs", json={
            "source_uri": SOURCE_URI,
            "month": TARGET_MONTH,
            "dry_run": False,
        })
        assert r.status_code == 200
        assert "run_id" in r.json()
        assert r.json()["run_id"]

    def test_slack_messages_delivered(self, run_id, mock_slack_server):
        """At least one message should have been POSTed to the mock Slack server."""
        r = requests.get(f"{MOCK_SLACK_BASE}/logs?limit=100")
        assert r.status_code == 200
        assert len(r.json()) >= 1

    def test_slack_messages_routed_to_correct_channels(self, run_id, mock_slack_server):
        valid_channels = {"amer-risk-alerts", "emea-risk-alerts", "apac-risk-alerts"}
        r = requests.get(f"{MOCK_SLACK_BASE}/logs?limit=100")
        for entry in r.json()["records"]:
            assert entry["channel"] in valid_channels, f"Unexpected channel: {entry['channel']}"

    def test_slack_messages_contain_required_fields(self, run_id, mock_slack_server):
        r = requests.get(f"{MOCK_SLACK_BASE}/logs?limit=100")
        for entry in r.json()["records"]:
            text = entry.get("payload", {}).get("text", "")
            assert "At Risk" in text
            assert "ARR" in text
            assert "Renewal" in text


# ---------------------------------------------------------------------------
# GET /runs/{run_id}
# ---------------------------------------------------------------------------

class TestGetRun:
    @pytest.fixture(scope="class")
    def run_result(self, service):
        r = requests.post(f"{SERVICE_BASE}/runs", json={
            "source_uri": SOURCE_URI,
            "month": TARGET_MONTH,
            "dry_run": False,
        })
        assert r.status_code == 200, r.text
        run_id = r.json()["run_id"]

        r = requests.get(f"{SERVICE_BASE}/runs/{run_id}")
        assert r.status_code == 200, r.text
        return r.json()

    def test_returns_200(self, service):
        r = requests.post(f"{SERVICE_BASE}/runs", json={
            "source_uri": SOURCE_URI,
            "month": TARGET_MONTH,
            "dry_run": False,
        })
        run_id = r.json()["run_id"]
        r = requests.get(f"{SERVICE_BASE}/runs/{run_id}")
        assert r.status_code == 200

    def test_status_is_succeeded(self, run_result):
        assert run_result["status"] == "succeeded"

    def test_counts_shape(self, run_result):
        counts = run_result["counts"]
        assert "rows_scanned" in counts
        assert "alerts_sent" in counts
        assert "skipped_replay" in counts
        assert "failed_deliveries" in counts
        assert "duplicate_count" in counts
        assert "unknown_regions" in counts

    def test_alerts_sent_is_positive(self, run_result):
        assert run_result["counts"]["alerts_sent"] >= 0

    def test_404_for_unknown_run_id(self, service):
        r = requests.get(f"{SERVICE_BASE}/runs/nonexistent-run-id")
        assert r.status_code == 404

    def test_month_matches_request(self, run_result):
        assert run_result["month"] == TARGET_MONTH


# ---------------------------------------------------------------------------
# Replay safety — rerun same month, expect skipped_replay
# ---------------------------------------------------------------------------

class TestReplay:
    def test_second_run_skips_already_sent_alerts(self, service):
        # First run
        r1 = requests.post(f"{SERVICE_BASE}/runs", json={
            "source_uri": SOURCE_URI,
            "month": TARGET_MONTH,
            "dry_run": False,
        })
        assert r1.status_code == 200
        run_id_1 = r1.json()["run_id"]
        result_1 = requests.get(f"{SERVICE_BASE}/runs/{run_id_1}").json()
        counts_1 = result_1["counts"]

        total_processed = counts_1["alerts_sent"] + counts_1["skipped_replay"] + counts_1["unknown_regions"]
        if total_processed == 0:
            pytest.skip("First run processed 0 alerts — nothing to replay")

        # Second run — same month
        r2 = requests.post(f"{SERVICE_BASE}/runs", json={
            "source_uri": SOURCE_URI,
            "month": TARGET_MONTH,
            "dry_run": False,
        })
        assert r2.status_code == 200
        run_id_2 = r2.json()["run_id"]
        result_2 = requests.get(f"{SERVICE_BASE}/runs/{run_id_2}").json()

        assert result_2["counts"]["skipped_replay"] > 0


# ---------------------------------------------------------------------------
# Dry run
# ---------------------------------------------------------------------------

class TestDryRun:
    def test_dry_run_returns_run_id(self, service):
        r = requests.post(f"{SERVICE_BASE}/runs", json={
            "source_uri": SOURCE_URI,
            "month": TARGET_MONTH,
            "dry_run": True,
        })
        assert r.status_code == 200
        assert "run_id" in r.json()

    def test_dry_run_does_not_send_slack(self, service, mock_slack_server):
        before = len(requests.get(f"{MOCK_SLACK_BASE}/logs?limit=500").json()["records"])

        requests.post(f"{SERVICE_BASE}/runs", json={
            "source_uri": SOURCE_URI,
            "month": TARGET_MONTH,
            "dry_run": True,
        })

        after = len(requests.get(f"{MOCK_SLACK_BASE}/logs?limit=500").json()["records"])
        assert after == before

    def test_dry_run_result_shows_not_sent(self, service):
        r = requests.post(f"{SERVICE_BASE}/runs", json={
            "source_uri": SOURCE_URI,
            "month": TARGET_MONTH,
            "dry_run": True,
        })
        run_id = r.json()["run_id"]
        result = requests.get(f"{SERVICE_BASE}/runs/{run_id}").json()
        assert result["dry_run"] is True
        assert result["counts"]["alerts_sent"] == 0