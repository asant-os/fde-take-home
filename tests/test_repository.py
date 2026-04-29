# tests/test_repository.py

import pytest
from datetime import date, datetime, timezone
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.repository import Base, Repository, AlertOutcomeRow
from app.models import AlertRecord, RunCounts


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def db():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    yield session
    session.close()


@pytest.fixture
def repo(db):
    return Repository(db)


def _make_alert(
    account_id="acct-1",
    month=date(2026, 1, 1),
    account_name="Acme Corp",
    account_region="AMER",
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


def _make_counts(**overrides) -> RunCounts:
    defaults = dict(
        rows_scanned=10,
        alerts_sent=2,
        skipped_replay=0,
        failed_deliveries=0,
        duplicate_count=1,
        unknown_regions=0,
    )
    return RunCounts(**{**defaults, **overrides})


# ---------------------------------------------------------------------------
# create_run
# ---------------------------------------------------------------------------

class TestCreateRun:
    def test_returns_a_run_id(self, repo, db):
        run_id = repo.create_run("file:///data.parquet", date(2026, 1, 1), dry_run=False)
        db.commit()
        assert run_id is not None
        assert len(run_id) > 0

    def test_run_is_persisted_with_running_status(self, repo, db):
        run_id = repo.create_run("file:///data.parquet", date(2026, 1, 1), dry_run=False)
        db.commit()
        run = repo.get_run(run_id)
        assert run.status == "running"

    def test_run_stores_source_uri_and_month(self, repo, db):
        run_id = repo.create_run("gs://bucket/data.parquet", date(2026, 3, 1), dry_run=True)
        db.commit()
        run = repo.get_run(run_id)
        assert run.source_uri == "gs://bucket/data.parquet"
        assert run.month == date(2026, 3, 1)
        assert run.dry_run is True

    def test_each_call_produces_unique_run_id(self, repo, db):
        id_1 = repo.create_run("file:///data.parquet", date(2026, 1, 1), dry_run=False)
        id_2 = repo.create_run("file:///data.parquet", date(2026, 1, 1), dry_run=False)
        db.commit()
        assert id_1 != id_2


# ---------------------------------------------------------------------------
# update_run_counts
# ---------------------------------------------------------------------------

class TestUpdateRunCounts:
    def test_updates_status_and_counts(self, repo, db):
        run_id = repo.create_run("file:///data.parquet", date(2026, 1, 1), dry_run=False)
        counts = _make_counts(alerts_sent=5, failed_deliveries=1)
        repo.update_run_counts(run_id, "succeeded", counts)
        db.commit()

        run = repo.get_run(run_id)
        assert run.status == "succeeded"
        assert run.alerts_sent == 5
        assert run.failed_deliveries == 1
        assert run.rows_scanned == 10
        assert run.completed_at is not None

    def test_sets_completed_at(self, repo, db):
        run_id = repo.create_run("file:///data.parquet", date(2026, 1, 1), dry_run=False)
        before = datetime.now(timezone.utc)
        repo.update_run_counts(run_id, "succeeded", _make_counts())
        db.commit()

        run = repo.get_run(run_id)
        # completed_at should be after we started the test
        completed = run.completed_at
        if completed.tzinfo is None:
            completed = completed.replace(tzinfo=timezone.utc)
        assert completed >= before

    def test_raises_if_run_not_found(self, repo):
        with pytest.raises(ValueError, match="Run not found"):
            repo.update_run_counts("nonexistent-id", "succeeded", _make_counts())


# ---------------------------------------------------------------------------
# get_run
# ---------------------------------------------------------------------------

class TestGetRun:
    def test_raises_if_run_not_found(self, repo):
        with pytest.raises(ValueError, match="Run not found"):
            repo.get_run("nonexistent-id")

    def test_returns_run_row(self, repo, db):
        run_id = repo.create_run("file:///data.parquet", date(2026, 1, 1), dry_run=False)
        db.commit()
        run = repo.get_run(run_id)
        assert run.run_id == run_id


# ---------------------------------------------------------------------------
# upsert_alert_outcome — insert path
# ---------------------------------------------------------------------------

class TestUpsertAlertOutcomeInsert:
    def test_inserts_new_outcome(self, repo, db):
        run_id = repo.create_run("file:///data.parquet", date(2026, 1, 1), dry_run=False)
        db.commit()
        alert = _make_alert()

        repo.upsert_alert_outcome(run_id, alert, channel="amer-risk-alerts", status="sent")
        db.commit()

        outcomes = repo.list_alert_outcomes(run_id)
        assert len(outcomes) == 1
        assert outcomes[0].status == "sent"
        assert outcomes[0].channel == "amer-risk-alerts"

    def test_stores_error_on_failed_status(self, repo, db):
        run_id = repo.create_run("file:///data.parquet", date(2026, 1, 1), dry_run=False)
        db.commit()
        alert = _make_alert()

        repo.upsert_alert_outcome(
            run_id, alert, channel=None, status="failed", error="unknown_region"
        )
        db.commit()

        outcomes = repo.list_alert_outcomes(run_id)
        assert outcomes[0].status == "failed"
        assert outcomes[0].error == "unknown_region"
        assert outcomes[0].channel is None

    def test_stores_alert_type_as_at_risk(self, repo, db):
        run_id = repo.create_run("file:///data.parquet", date(2026, 1, 1), dry_run=False)
        db.commit()
        repo.upsert_alert_outcome(run_id, _make_alert(), channel="amer-risk-alerts", status="sent")
        db.commit()

        outcomes = repo.list_alert_outcomes(run_id)
        assert outcomes[0].alert_type == "at_risk"


# ---------------------------------------------------------------------------
# upsert_alert_outcome — update path (idempotency)
# ---------------------------------------------------------------------------

class TestUpsertAlertOutcomeUpdate:
    def test_second_upsert_updates_existing_row(self, repo, db):
        run_id = repo.create_run("file:///data.parquet", date(2026, 1, 1), dry_run=False)
        db.commit()
        alert = _make_alert()

        repo.upsert_alert_outcome(run_id, alert, channel="amer-risk-alerts", status="failed")
        db.commit()

        repo.upsert_alert_outcome(run_id, alert, channel="amer-risk-alerts", status="sent")
        db.commit()

        outcomes = repo.list_alert_outcomes(run_id)
        # Still only one row
        assert len(outcomes) == 1
        assert outcomes[0].status == "sent"

    def test_does_not_create_duplicate_rows_on_replay(self, repo, db):
        run_id = repo.create_run("file:///data.parquet", date(2026, 1, 1), dry_run=False)
        db.commit()
        alert = _make_alert()

        for _ in range(3):
            repo.upsert_alert_outcome(run_id, alert, channel="amer-risk-alerts", status="sent")
            db.commit()

        outcomes = repo.list_alert_outcomes(run_id)
        assert len(outcomes) == 1

    def test_different_accounts_same_month_are_independent_rows(self, repo, db):
        run_id = repo.create_run("file:///data.parquet", date(2026, 1, 1), dry_run=False)
        db.commit()

        repo.upsert_alert_outcome(run_id, _make_alert(account_id="acct-1"), channel="amer-risk-alerts", status="sent")
        repo.upsert_alert_outcome(run_id, _make_alert(account_id="acct-2"), channel="amer-risk-alerts", status="sent")
        db.commit()

        outcomes = repo.list_alert_outcomes(run_id)
        assert len(outcomes) == 2

    def test_same_account_different_months_are_independent_rows(self, repo, db):
        run_id = repo.create_run("file:///data.parquet", date(2026, 1, 1), dry_run=False)
        db.commit()

        repo.upsert_alert_outcome(run_id, _make_alert(month=date(2026, 1, 1)), channel="amer-risk-alerts", status="sent")
        repo.upsert_alert_outcome(run_id, _make_alert(month=date(2026, 2, 1)), channel="amer-risk-alerts", status="sent")
        db.commit()

        outcomes = repo.list_alert_outcomes(run_id)
        assert len(outcomes) == 2


# ---------------------------------------------------------------------------
# get_alert_outcome
# ---------------------------------------------------------------------------

class TestGetAlertOutcome:
    def test_returns_none_when_not_found(self, repo):
        result = repo.get_alert_outcome("acct-1", date(2026, 1, 1))
        assert result is None

    def test_returns_existing_outcome(self, repo, db):
        run_id = repo.create_run("file:///data.parquet", date(2026, 1, 1), dry_run=False)
        db.commit()
        repo.upsert_alert_outcome(run_id, _make_alert(), channel="amer-risk-alerts", status="sent")
        db.commit()

        result = repo.get_alert_outcome("acct-1", date(2026, 1, 1))
        assert result is not None
        assert result.account_id == "acct-1"


# ---------------------------------------------------------------------------
# list_alert_outcomes
# ---------------------------------------------------------------------------

class TestListAlertOutcomes:
    def test_returns_empty_list_for_new_run(self, repo, db):
        run_id = repo.create_run("file:///data.parquet", date(2026, 1, 1), dry_run=False)
        db.commit()
        assert repo.list_alert_outcomes(run_id) == []

    def test_returns_only_outcomes_for_given_run(self, repo, db):
        run_id_1 = repo.create_run("file:///data.parquet", date(2026, 1, 1), dry_run=False)
        run_id_2 = repo.create_run("file:///data.parquet", date(2026, 2, 1), dry_run=False)
        db.commit()

        repo.upsert_alert_outcome(run_id_1, _make_alert(account_id="acct-1", month=date(2026, 1, 1)), channel="amer-risk-alerts", status="sent")
        repo.upsert_alert_outcome(run_id_2, _make_alert(account_id="acct-2", month=date(2026, 2, 1)), channel="amer-risk-alerts", status="sent")
        db.commit()

        assert len(repo.list_alert_outcomes(run_id_1)) == 1
        assert len(repo.list_alert_outcomes(run_id_2)) == 1


# ---------------------------------------------------------------------------
# health_check
# ---------------------------------------------------------------------------

class TestHealthCheck:
    def test_returns_true_with_live_db(self, repo):
        assert repo.health_check() is True