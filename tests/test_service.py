# tests/test_service.py

import pytest
from datetime import date, datetime, timezone
from unittest.mock import MagicMock, patch

from app.service import AlertService
from app.models import AlertRecord, ReaderResults, RunCounts


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_alert(
    account_id="acct-1",
    account_name="Acme Corp",
    account_region="AMER",
    month=date(2026, 1, 1),
    arr=50_000,
    duration_months=2,
    risk_start_month=date(2025, 12, 1),
) -> AlertRecord:
    return AlertRecord(
        account_id=account_id,
        account_name=account_name,
        account_region=account_region,
        month=month,
        arr=arr,
        renewal_date=date(2026, 6, 1),
        account_owner="jane@example.com",
        duration_months=duration_months,
        risk_start_month=risk_start_month,
    )


def _make_config(regions=None, history_months=12, arr_threshold=0, sample_limit=5):
    config = MagicMock()
    config.regions = regions or {"AMER": "amer-risk-alerts", "EMEA": "emea-risk-alerts"}
    config.history_months = history_months
    config.arr_threshold = arr_threshold
    config.sample_limit = sample_limit
    return config


def _make_send_result(status="sent", error=None):
    result = MagicMock()
    result.status = status
    result.error = error
    return result


def _make_service(repo=None, config=None, notifier=None, email_notifier=None):
    return AlertService(
        repo=repo or MagicMock(),
        config=config or _make_config(),
        notifier=notifier or MagicMock(),
        email_notifier=email_notifier or MagicMock(),
    )


def _update_counts_args(repo):
    """Extract (run_id, status, counts) from update_run_counts positional args."""
    args = repo.update_run_counts.call_args.args
    return args[0], args[1], args[2]


PATCH_OPEN_URI = "app.service.open_uri"
PATCH_PROCESS = "app.service.process_monthly_status"


# ---------------------------------------------------------------------------
# run() — happy path
# ---------------------------------------------------------------------------

class TestRunHappyPath:
    def test_returns_run_id(self):
        repo = MagicMock()
        repo.create_run.return_value = "run-123"
        repo.get_alert_outcome.return_value = None
        notifier = MagicMock()
        notifier.send.return_value = _make_send_result("sent")
        service = _make_service(repo=repo, notifier=notifier)

        with patch(PATCH_OPEN_URI, return_value=(MagicMock(), "fake/path")):
            with patch(PATCH_PROCESS, return_value=ReaderResults(alerts=[_make_alert()], duplicate_count=0, rows_scanned=0)):
                run_id = service.run("file:///data.parquet", date(2026, 1, 1), dry_run=False)

        assert run_id == "run-123"

    def test_alert_sent_and_outcome_persisted(self):
        repo = MagicMock()
        repo.create_run.return_value = "run-123"
        repo.get_alert_outcome.return_value = None
        notifier = MagicMock()
        notifier.send.return_value = _make_send_result("sent")
        service = _make_service(repo=repo, notifier=notifier)

        with patch(PATCH_OPEN_URI, return_value=(MagicMock(), "fake/path")):
            with patch(PATCH_PROCESS, return_value=ReaderResults(alerts=[_make_alert()], duplicate_count=0, rows_scanned=0)):
                service.run("file:///data.parquet", date(2026, 1, 1), dry_run=False)

        notifier.send.assert_called_once_with("amer-risk-alerts", notifier.format.return_value)
        repo.upsert_alert_outcome.assert_called_once()
        assert repo.upsert_alert_outcome.call_args.kwargs["status"] == "sent"
        assert repo.upsert_alert_outcome.call_args.kwargs["channel"] == "amer-risk-alerts"

    def test_run_marked_succeeded(self):
        repo = MagicMock()
        repo.create_run.return_value = "run-123"
        repo.get_alert_outcome.return_value = None
        notifier = MagicMock()
        notifier.send.return_value = _make_send_result("sent")
        service = _make_service(repo=repo, notifier=notifier)

        with patch(PATCH_OPEN_URI, return_value=(MagicMock(), "fake/path")):
            with patch(PATCH_PROCESS, return_value=ReaderResults(alerts=[_make_alert()], duplicate_count=0, rows_scanned=0)):
                service.run("file:///data.parquet", date(2026, 1, 1), dry_run=False)

        _, status, _ = _update_counts_args(repo)
        assert status == "succeeded"

    def test_alerts_sent_count_incremented(self):
        repo = MagicMock()
        repo.create_run.return_value = "run-123"
        repo.get_alert_outcome.return_value = None
        notifier = MagicMock()
        notifier.send.return_value = _make_send_result("sent")
        service = _make_service(repo=repo, notifier=notifier)

        with patch(PATCH_OPEN_URI, return_value=(MagicMock(), "fake/path")):
            with patch(PATCH_PROCESS, return_value=ReaderResults(
                alerts=[_make_alert("acct-1"), _make_alert("acct-2")],
                duplicate_count=0, rows_scanned=0,
            )):
                service.run("file:///data.parquet", date(2026, 1, 1), dry_run=False)

        _, _, counts = _update_counts_args(repo)
        assert counts.alerts_sent == 2


# ---------------------------------------------------------------------------
# run() — replay / idempotency
# ---------------------------------------------------------------------------

class TestRunReplay:
    def test_already_sent_alert_is_skipped(self):
        repo = MagicMock()
        repo.create_run.return_value = "run-123"
        existing = MagicMock()
        existing.status = "sent"
        repo.get_alert_outcome.return_value = existing
        notifier = MagicMock()
        service = _make_service(repo=repo, notifier=notifier)

        with patch(PATCH_OPEN_URI, return_value=(MagicMock(), "fake/path")):
            with patch(PATCH_PROCESS, return_value=ReaderResults(alerts=[_make_alert()], duplicate_count=0, rows_scanned=0)):
                service.run("file:///data.parquet", date(2026, 1, 1), dry_run=False)

        notifier.send.assert_not_called()

    def test_skipped_replay_count_incremented(self):
        repo = MagicMock()
        repo.create_run.return_value = "run-123"
        existing = MagicMock()
        existing.status = "sent"
        repo.get_alert_outcome.return_value = existing
        service = _make_service(repo=repo)

        with patch(PATCH_OPEN_URI, return_value=(MagicMock(), "fake/path")):
            with patch(PATCH_PROCESS, return_value=ReaderResults(alerts=[_make_alert()], duplicate_count=0, rows_scanned=0)):
                service.run("file:///data.parquet", date(2026, 1, 1), dry_run=False)

        _, _, counts = _update_counts_args(repo)
        assert counts.skipped_replay == 1

    def test_previously_failed_alert_is_retried(self):
        repo = MagicMock()
        repo.create_run.return_value = "run-123"
        existing = MagicMock()
        existing.status = "failed"
        repo.get_alert_outcome.return_value = existing
        notifier = MagicMock()
        notifier.send.return_value = _make_send_result("sent")
        service = _make_service(repo=repo, notifier=notifier)

        with patch(PATCH_OPEN_URI, return_value=(MagicMock(), "fake/path")):
            with patch(PATCH_PROCESS, return_value=ReaderResults(alerts=[_make_alert()], duplicate_count=0, rows_scanned=0)):
                service.run("file:///data.parquet", date(2026, 1, 1), dry_run=False)

        notifier.send.assert_called_once()


# ---------------------------------------------------------------------------
# run() — unknown region
# ---------------------------------------------------------------------------

class TestRunUnknownRegion:
    def test_no_slack_send_for_unknown_region(self):
        repo = MagicMock()
        repo.create_run.return_value = "run-123"
        repo.get_alert_outcome.return_value = None
        notifier = MagicMock()
        service = _make_service(repo=repo, notifier=notifier)

        with patch(PATCH_OPEN_URI, return_value=(MagicMock(), "fake/path")):
            with patch(PATCH_PROCESS, return_value=ReaderResults(alerts=[_make_alert(account_region="UNKNOWN")], duplicate_count=0, rows_scanned=0)):
                service.run("file:///data.parquet", date(2026, 1, 1), dry_run=False)

        notifier.send.assert_not_called()

    def test_unknown_region_outcome_persisted_as_failed(self):
        repo = MagicMock()
        repo.create_run.return_value = "run-123"
        repo.get_alert_outcome.return_value = None
        service = _make_service(repo=repo)

        with patch(PATCH_OPEN_URI, return_value=(MagicMock(), "fake/path")):
            with patch(PATCH_PROCESS, return_value=ReaderResults(alerts=[_make_alert(account_region="UNKNOWN")], duplicate_count=0, rows_scanned=0)):
                service.run("file:///data.parquet", date(2026, 1, 1), dry_run=False)

        assert repo.upsert_alert_outcome.call_args.kwargs["status"] == "failed"
        assert repo.upsert_alert_outcome.call_args.kwargs["error"] == "unknown_region"

    def test_null_region_treated_as_unknown(self):
        repo = MagicMock()
        repo.create_run.return_value = "run-123"
        repo.get_alert_outcome.return_value = None
        notifier = MagicMock()
        service = _make_service(repo=repo, notifier=notifier)

        with patch(PATCH_OPEN_URI, return_value=(MagicMock(), "fake/path")):
            with patch(PATCH_PROCESS, return_value=ReaderResults(alerts=[_make_alert(account_region=None)], duplicate_count=0, rows_scanned=0)):
                service.run("file:///data.parquet", date(2026, 1, 1), dry_run=False)

        notifier.send.assert_not_called()
        assert repo.upsert_alert_outcome.call_args.kwargs["error"] == "unknown_region"

    def test_unknown_region_count_incremented(self):
        repo = MagicMock()
        repo.create_run.return_value = "run-123"
        repo.get_alert_outcome.return_value = None
        service = _make_service(repo=repo)

        with patch(PATCH_OPEN_URI, return_value=(MagicMock(), "fake/path")):
            with patch(PATCH_PROCESS, return_value=ReaderResults(
                alerts=[_make_alert(account_region=None)],
                duplicate_count=0, rows_scanned=0,
            )):
                service.run("file:///data.parquet", date(2026, 1, 1), dry_run=False)

        _, _, counts = _update_counts_args(repo)
        assert counts.unknown_regions == 1

    def test_aggregated_email_sent_after_run(self):
        repo = MagicMock()
        repo.create_run.return_value = "run-123"
        repo.get_alert_outcome.return_value = None
        email_notifier = MagicMock()
        service = _make_service(repo=repo, email_notifier=email_notifier)
        alert = _make_alert(account_region=None)

        with patch(PATCH_OPEN_URI, return_value=(MagicMock(), "fake/path")):
            with patch(PATCH_PROCESS, return_value=ReaderResults(alerts=[alert], duplicate_count=0, rows_scanned=0)):
                service.run("file:///data.parquet", date(2026, 1, 1), dry_run=False)

        email_notifier.send_unknown_region_summary.assert_called_once()
        unknown_alerts = email_notifier.send_unknown_region_summary.call_args.args[0]
        assert alert in unknown_alerts


# ---------------------------------------------------------------------------
# run() — failed send
# ---------------------------------------------------------------------------

class TestRunFailedSend:
    def test_failed_send_outcome_persisted(self):
        repo = MagicMock()
        repo.create_run.return_value = "run-123"
        repo.get_alert_outcome.return_value = None
        notifier = MagicMock()
        notifier.send.return_value = _make_send_result("failed", error="max_retries_exceeded")
        service = _make_service(repo=repo, notifier=notifier)

        with patch(PATCH_OPEN_URI, return_value=(MagicMock(), "fake/path")):
            with patch(PATCH_PROCESS, return_value=ReaderResults(alerts=[_make_alert()], duplicate_count=0, rows_scanned=0)):
                service.run("file:///data.parquet", date(2026, 1, 1), dry_run=False)

        assert repo.upsert_alert_outcome.call_args.kwargs["status"] == "failed"
        assert repo.upsert_alert_outcome.call_args.kwargs["error"] == "max_retries_exceeded"

    def test_failed_delivery_count_incremented(self):
        repo = MagicMock()
        repo.create_run.return_value = "run-123"
        repo.get_alert_outcome.return_value = None
        notifier = MagicMock()
        notifier.send.return_value = _make_send_result("failed", error="max_retries_exceeded")
        service = _make_service(repo=repo, notifier=notifier)

        with patch(PATCH_OPEN_URI, return_value=(MagicMock(), "fake/path")):
            with patch(PATCH_PROCESS, return_value=ReaderResults(alerts=[_make_alert()], duplicate_count=0, rows_scanned=0)):
                service.run("file:///data.parquet", date(2026, 1, 1), dry_run=False)

        _, _, counts = _update_counts_args(repo)
        assert counts.failed_deliveries == 1

    def test_run_completes_even_if_all_sends_fail(self):
        """A failed Slack send must not abort the run — other alerts should still process."""
        repo = MagicMock()
        repo.create_run.return_value = "run-123"
        repo.get_alert_outcome.return_value = None
        notifier = MagicMock()
        notifier.send.return_value = _make_send_result("failed", error="500")
        service = _make_service(repo=repo, notifier=notifier)

        with patch(PATCH_OPEN_URI, return_value=(MagicMock(), "fake/path")):
            with patch(PATCH_PROCESS, return_value=ReaderResults(
                alerts=[_make_alert("acct-1"), _make_alert("acct-2")],
                duplicate_count=0, rows_scanned=0,
            )):
                service.run("file:///data.parquet", date(2026, 1, 1), dry_run=False)

        assert notifier.send.call_count == 2
        _, status, counts = _update_counts_args(repo)
        assert counts.failed_deliveries == 2
        assert status == "succeeded"


# ---------------------------------------------------------------------------
# run() — dry run
# ---------------------------------------------------------------------------

class TestRunDryRun:
    def test_no_slack_send_in_dry_run(self):
        repo = MagicMock()
        repo.create_run.return_value = "run-123"
        repo.get_alert_outcome.return_value = None
        notifier = MagicMock()
        service = _make_service(repo=repo, notifier=notifier)

        with patch(PATCH_OPEN_URI, return_value=(MagicMock(), "fake/path")):
            with patch(PATCH_PROCESS, return_value=ReaderResults(alerts=[_make_alert()], duplicate_count=0, rows_scanned=0)):
                service.run("file:///data.parquet", date(2026, 1, 1), dry_run=True)

        notifier.send.assert_not_called()

    def test_dry_run_outcome_persisted_as_not_sent(self):
        repo = MagicMock()
        repo.create_run.return_value = "run-123"
        repo.get_alert_outcome.return_value = None
        service = _make_service(repo=repo)

        with patch(PATCH_OPEN_URI, return_value=(MagicMock(), "fake/path")):
            with patch(PATCH_PROCESS, return_value=ReaderResults(alerts=[_make_alert()], duplicate_count=0, rows_scanned=0)):
                service.run("file:///data.parquet", date(2026, 1, 1), dry_run=True)

        assert repo.upsert_alert_outcome.call_args.kwargs["status"] == "not-sent"


# ---------------------------------------------------------------------------
# preview()
# ---------------------------------------------------------------------------

class TestPreview:
    def test_returns_alert_records(self):
        repo = MagicMock()
        repo.create_run.return_value = "run-123"
        repo.get_alert_outcome.return_value = None
        alert = _make_alert()
        service = _make_service(repo=repo)

        with patch(PATCH_OPEN_URI, return_value=(MagicMock(), "fake/path")):
            with patch(PATCH_PROCESS, return_value=ReaderResults(alerts=[alert], duplicate_count=0, rows_scanned=0)):
                result = service.preview("file:///data.parquet", date(2026, 1, 1))

        assert result == [alert]

    def test_no_slack_send_in_preview(self):
        repo = MagicMock()
        repo.create_run.return_value = "run-123"
        repo.get_alert_outcome.return_value = None
        notifier = MagicMock()
        service = _make_service(repo=repo, notifier=notifier)

        with patch(PATCH_OPEN_URI, return_value=(MagicMock(), "fake/path")):
            with patch(PATCH_PROCESS, return_value=ReaderResults(alerts=[_make_alert()], duplicate_count=0, rows_scanned=0)):
                service.preview("file:///data.parquet", date(2026, 1, 1))

        notifier.send.assert_not_called()

    def test_no_db_writes_in_preview(self):
        repo = MagicMock()
        repo.create_run.return_value = "run-123"
        repo.get_alert_outcome.return_value = None
        service = _make_service(repo=repo)

        with patch(PATCH_OPEN_URI, return_value=(MagicMock(), "fake/path")):
            with patch(PATCH_PROCESS, return_value=ReaderResults(alerts=[_make_alert()], duplicate_count=0, rows_scanned=0)):
                service.preview("file:///data.parquet", date(2026, 1, 1))

        repo.upsert_alert_outcome.assert_not_called()


# ---------------------------------------------------------------------------
# run() — unhandled exception path
# ---------------------------------------------------------------------------

class TestRunExceptionHandling:
    def test_run_marked_failed_on_exception(self):
        repo = MagicMock()
        repo.create_run.return_value = "run-123"
        service = _make_service(repo=repo)

        with patch(PATCH_OPEN_URI, side_effect=RuntimeError("storage unavailable")):
            with pytest.raises(RuntimeError):
                service.run("gs://bad-bucket/data.parquet", date(2026, 1, 1), dry_run=False)

        _, status, _ = _update_counts_args(repo)
        assert status == "failed"

    def test_exception_is_re_raised(self):
        repo = MagicMock()
        repo.create_run.return_value = "run-123"
        service = _make_service(repo=repo)

        with patch(PATCH_OPEN_URI, side_effect=RuntimeError("storage unavailable")):
            with pytest.raises(RuntimeError, match="storage unavailable"):
                service.run("gs://bad-bucket/data.parquet", date(2026, 1, 1), dry_run=False)