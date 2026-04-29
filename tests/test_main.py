# tests/test_main.py

import pytest
from datetime import date
from unittest.mock import MagicMock, patch
from fastapi.testclient import TestClient

from app.main import app
from app.dependencies import get_config, get_repo
from app.models import (
    AlertRecord, RunCounts, RunStatusResponse
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_alert(account_id="acct-1") -> AlertRecord:
    return AlertRecord(
        account_id=account_id,
        account_name="Acme Corp",
        account_region="AMER",
        month=date(2026, 1, 1),
        arr=50_000,
        renewal_date=date(2026, 6, 1),
        account_owner="jane@example.com",
        duration_months=2,
        risk_start_month=date(2025, 12, 1),
    )


def _make_run_status(run_id="run-123") -> RunStatusResponse:
    return RunStatusResponse(
        run_id=run_id,
        status="succeeded",
        month=date(2026, 1, 1),
        dry_run=False,
        counts=RunCounts(rows_scanned=5, alerts_sent=2),
        sample_alerts=[],
        sample_errors=[],
    )


MOCK_DEPS = {
    "app.main.get_config": MagicMock(),
    "app.main.get_repo": MagicMock(),
    "app.main.get_slack": MagicMock(),
    "app.main.get_email": MagicMock(),
}

RUN_REQUEST = {
    "source_uri": "file:///data.parquet",
    "month": "2026-01-01",
    "dry_run": False,
}


@pytest.fixture
def client():
    return TestClient(app)


# ---------------------------------------------------------------------------
# GET /health
# ---------------------------------------------------------------------------

class TestHealth:
    def test_returns_200(self, client):
        with patch("app.main.get_repo") as mock_get_repo:
            repo = MagicMock()
            repo.health_check.return_value = True
            mock_get_repo.return_value = repo
            response = client.get("/health")
        assert response.status_code == 200

    def test_returns_ok_and_db_true(self, client):
        with patch("app.main.get_repo") as mock_get_repo:
            repo = MagicMock()
            repo.health_check.return_value = True
            mock_get_repo.return_value = repo
            response = client.get("/health")
        assert response.json() == {"ok": True, "db": True}


# ---------------------------------------------------------------------------
# POST /runs
# ---------------------------------------------------------------------------

class TestCreateRun:
    def test_returns_200_with_run_id(self, client):
        with patch("app.main.AlertService") as MockService:
            MockService.return_value.run.return_value = "run-123"
            response = client.post("/runs", json=RUN_REQUEST)
        assert response.status_code == 200
        assert response.json() == {"run_id": "run-123"}

    def test_passes_correct_month_to_service(self, client):
        with patch("app.main.AlertService") as MockService:
            MockService.return_value.run.return_value = "run-123"
            client.post("/runs", json=RUN_REQUEST)
        _, kwargs = MockService.return_value.run.call_args
        assert kwargs["target_month"] == date(2026, 1, 1)

    def test_passes_dry_run_flag(self, client):
        with patch("app.main.AlertService") as MockService:
            MockService.return_value.run.return_value = "run-123"
            client.post("/runs", json={**RUN_REQUEST, "dry_run": True})
        _, kwargs = MockService.return_value.run.call_args
        assert kwargs["dry_run"] is True

    def test_returns_500_on_service_exception(self, client):
        with patch("app.main.AlertService") as MockService:
            MockService.return_value.run.side_effect = RuntimeError("storage error")
            response = client.post("/runs", json=RUN_REQUEST)
        assert response.status_code == 500
        assert "storage error" in response.json()["detail"]

    def test_returns_422_on_invalid_request(self, client):
        response = client.post("/runs", json={"source_uri": "file:///data.parquet"})
        assert response.status_code == 422


# ---------------------------------------------------------------------------
# GET /runs/{run_id}
# ---------------------------------------------------------------------------
class TestGetRun:
    def test_returns_200_with_run_status(self, client):
        repo = MagicMock()
        repo.get_run_status.return_value = _make_run_status()
        app.dependency_overrides[get_repo] = lambda: repo
        try:
            response = client.get("/runs/run-123")
        finally:
            app.dependency_overrides.pop(get_repo, None)
        assert response.status_code == 200
        assert response.json()["run_id"] == "run-123"
        assert response.json()["status"] == "succeeded"

    def test_returns_404_when_run_not_found(self, client):
        repo = MagicMock()
        repo.get_run_status.return_value = None
        app.dependency_overrides[get_repo] = lambda: repo
        try:
            response = client.get("/runs/nonexistent")
        finally:
            app.dependency_overrides.pop(get_repo, None)
        assert response.status_code == 404
        assert "nonexistent" in response.json()["detail"]

    def test_returns_500_on_unexpected_error(self, client):
        repo = MagicMock()
        repo.get_run_status.side_effect = RuntimeError("db error")
        app.dependency_overrides[get_repo] = lambda: repo
        try:
            response = client.get("/runs/run-123")
        finally:
            app.dependency_overrides.pop(get_repo, None)
        assert response.status_code == 500

# ---------------------------------------------------------------------------
# POST /preview
# ---------------------------------------------------------------------------

class TestPreview:
    def test_returns_200_with_alert_count(self, client):
        alerts = [_make_alert("acct-1"), _make_alert("acct-2")]
        with patch("app.main.AlertService") as MockService:
            mock_config = MagicMock()
            mock_config.sample_limit = 10
            MockService.return_value.preview.return_value = alerts
            with patch("app.main.get_config", return_value=mock_config):
                response = client.post("/preview", json=RUN_REQUEST)
        assert response.status_code == 200
        assert response.json()["alert_count"] == 2
        assert response.json()["month"] == "2026-01-01"

    def test_respects_sample_limit(self, client):
        alerts = [_make_alert(f"acct-{i}") for i in range(10)]
        mock_config = MagicMock()
        mock_config.sample_limit = 3

        with patch("app.main.AlertService") as MockService:
            MockService.return_value.preview.return_value = alerts
            app.dependency_overrides[get_config] = lambda: mock_config
            try:
                response = client.post("/preview", json=RUN_REQUEST)
            finally:
                app.dependency_overrides.pop(get_config, None)

        assert len(response.json()["alerts"]) == 3

    def test_returns_500_on_service_exception(self, client):
        with patch("app.main.AlertService") as MockService:
            MockService.return_value.preview.side_effect = RuntimeError("reader error")
            response = client.post("/preview", json=RUN_REQUEST)
        assert response.status_code == 500
        assert "reader error" in response.json()["detail"]