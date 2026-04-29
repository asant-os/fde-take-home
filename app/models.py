from datetime import date
from typing import Optional
from pydantic import BaseModel

ACCOUNT_COLUMNS = [
    "account_id", "account_name", "account_region", "month",
    "status", "renewal_date", "account_owner", "arr", "updated_at",
]

ACCOUNT_HISTORY_COLUMNS = [
    "account_id", "month", "status", "updated_at",
]

class HealthResponse(BaseModel):
    ok: bool
    db: bool

class AlertRecord(BaseModel):
    account_id: str
    account_name: str
    account_region: str | None
    month: date
    arr: int | None
    renewal_date: date | None
    account_owner: str | None
    duration_months: int
    risk_start_month: date

class AlertOutcome(BaseModel):
    account_id: str
    account_name: Optional[str] = None
    channel: Optional[str] = None
    status: str
    error: Optional[str] = None

class ReaderResults(BaseModel):
    alerts: list[AlertRecord]
    rows_scanned: int
    duplicate_count: int

class RunRequest(BaseModel):
    source_uri: str
    month: str  # YYYY-MM-01
    dry_run: bool = False

class RunResponse(BaseModel):
    run_id: str

class RunCounts(BaseModel):
    rows_scanned: int = 0
    alerts_sent: int = 0
    skipped_replay: int = 0
    failed_deliveries: int = 0
    duplicate_count: int = 0
    unknown_regions: int = 0

class RunStatusResponse(BaseModel):
    run_id: str
    status: str
    month: date
    dry_run: bool
    counts: RunCounts
    sample_alerts: list[AlertOutcome] = []
    sample_errors: list[AlertOutcome] = []


class PreviewResponse(BaseModel):
    month: str
    alert_count: int
    alerts: list[AlertRecord]

class SendAlertResults(BaseModel):
    status: str
    attempts: int
    error: str | None = None