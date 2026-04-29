import uuid
from datetime import date, datetime, timezone

from sqlalchemy import Column, String, Boolean, Integer, DateTime, Date, UniqueConstraint, ForeignKey, text
from sqlalchemy.orm import declarative_base, Session

from app.models import AlertRecord, RunCounts, RunStatusResponse, AlertOutcome

Base = declarative_base()

class RunRow(Base):
    __tablename__ = "runs"

    run_id = Column(String, primary_key=True)
    source_uri = Column(String, nullable=False)
    month = Column(Date, nullable=False)
    status = Column(String, nullable=False)
    dry_run = Column(Boolean, nullable=False, default=False)
    rows_scanned = Column(Integer, default=0)
    alerts_sent = Column(Integer, default=0)
    skipped_replay = Column(Integer, default=0)
    failed_deliveries = Column(Integer, default=0)
    unknown_regions = Column(Integer, default=0)
    duplicate_count = Column(Integer, default=0)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    completed_at = Column(DateTime, nullable=True)

class AlertOutcomeRow(Base):
    __tablename__ = "alert_outcomes"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    run_id = Column(String, ForeignKey("runs.run_id"), nullable=False)
    account_id = Column(String, nullable=False)
    account_name = Column(String, nullable=False)
    month = Column(Date, nullable=False)
    alert_type = Column(String, nullable=False, default="at_risk")
    channel = Column(String, nullable=True)
    status = Column(String, nullable=False)  # sent / not_sent / failed
    error = Column(String, nullable=True)
    sent_at = Column(DateTime, nullable=True)

    __table_args__ = (
        UniqueConstraint("account_id", "month", "alert_type", name="uq_alert_idempotency"),
    )

class Repository:
    def __init__(self, db: Session):
        self.db = db

    def create_run(self, source_uri: str, month: date, dry_run: bool) -> str:
        run_id = str(uuid.uuid4())
        self.db.add(RunRow(
            run_id=run_id,
            source_uri=source_uri,
            month=month,
            status="running",
            dry_run=dry_run,
        ))

        return run_id

    def update_run_counts(self, run_id: str, status: str, counts: RunCounts) -> None:
        run = self.db.get(RunRow, run_id)
        if run is None:
            raise ValueError(f"Run not found {run_id}")
        
        run.status = status
        run.rows_scanned = counts.rows_scanned
        run.duplicate_count = counts.duplicate_count
        run.alerts_sent = counts.alerts_sent
        run.skipped_replay = counts.skipped_replay
        run.failed_deliveries = counts.failed_deliveries
        run.unknown_regions = counts.unknown_regions
        run.completed_at = datetime.now(timezone.utc)

    def get_run(self, run_id: str) -> RunRow | None:
        run = self.db.get(RunRow, run_id)
        if run is None:
            raise ValueError(f"Run not found {run_id}")
        
        return run

    def get_run_status(self, run_id: str, sample_limit: int) -> RunStatusResponse | None:
        try:
            run = self.get_run(run_id)
        except ValueError:
            return None
        
        outcomes = self.list_alert_outcomes(run_id)
        sample_alerts = [
            AlertOutcome(account_id=o.account_id, account_name=o.account_name, channel=o.channel, status=o.status)
            for o in outcomes if o.status == "sent"
        ][:sample_limit]

        sample_errors = [
            AlertOutcome(account_id=o.account_id, account_name=o.account_name, channel=o.channel, status=o.status, error=o.error)
            for o in outcomes if o.status == "failed"
        ][:sample_limit]

        return RunStatusResponse(
            run_id=run.run_id,
            status=run.status,
            month=run.month,
            dry_run=run.dry_run,
            counts=RunCounts(
                rows_scanned=run.rows_scanned,
                alerts_sent=run.alerts_sent,
                skipped_replay=run.skipped_replay,
                failed_deliveries=run.failed_deliveries,
                duplicate_count=run.duplicate_count,
                unknown_regions=run.unknown_regions,
            ),
            sample_alerts=sample_alerts,
            sample_errors=sample_errors,
        )

    def list_alert_outcomes(self, run_id: str) -> list[AlertOutcomeRow]:
        return self.db.query(AlertOutcomeRow).filter_by(run_id=run_id).all()

    def get_alert_outcome(
        self,
        account_id: str,
        month: date,
        alert_type: str = "at_risk",
    ) -> AlertOutcomeRow | None:
        return self.db.query(AlertOutcomeRow).filter_by(
                account_id=account_id,
                month=month,
                alert_type=alert_type,
            ).first()
        
    def upsert_alert_outcome(
        self,
        run_id: str,
        alert: AlertRecord,
        channel: str | None,
        status: str,
        error: str | None = None,
        sent_at: datetime | None = None,
    ) -> None:
        existing = self.get_alert_outcome(
            alert.account_id,
            alert.month,
            "at_risk",
        )

        if existing:
            existing.run_id = run_id
            existing.channel = channel
            existing.status = status
            existing.error = error
            existing.sent_at = sent_at
            return

        self.db.add(AlertOutcomeRow(
            run_id=run_id,
            account_id=alert.account_id,
            account_name=alert.account_name,
            month=alert.month,
            alert_type="at_risk",
            channel=channel,
            status=status,
            error=error,
            sent_at=sent_at,
        ))
    
    def health_check(self) -> bool:
        try:
            self.db.execute(text("SELECT 1"))
            return True
        except Exception:
            return False