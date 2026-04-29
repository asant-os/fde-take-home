import os
import json
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

@dataclass
class Config:
    slack_webhook_base_url: str | None
    slack_webhook_url: str | None
    slack_max_retries: int
    slack_backoff_base: float
    slack_backoff_max: float
    arr_threshold: int
    history_months: int
    regions: dict[str, str]
    sqlite_path: str
    details_base_url: str
    escalation_email: str
    sample_limit: int

def load_config_and_env(config_file: str = "config.json") -> Config:
    load_dotenv()

    path = Path(config_file)
    file_config = json.loads(path.read_text()) if path.exists() else {}
    
    return Config(
        slack_webhook_base_url=os.getenv("SLACK_WEBHOOK_BASE_URL"),
        slack_webhook_url=os.getenv("SLACK_WEBHOOK_URL"),
        slack_max_retries=int(os.getenv("SLACK_MAX_RETRIES", "3")),
        slack_backoff_base=float(os.getenv("SLACK_BACKOFF_BASE", "1.0")),
        slack_backoff_max=float(os.getenv("SLACK_BACKOFF_MAX", "30.0")),
        arr_threshold=int(os.getenv("ARR_THRESHOLD", "50000")),
        history_months=int(os.getenv("HISTORY_MONTHS", "24")),
        regions=file_config.get("regions", {}),
        sqlite_path=os.getenv("SQLITE_PATH", "./risk_alerts.db"),
        details_base_url=os.getenv("DETAILS_BASE_URL", "https://app.yourcompany.com"),
        escalation_email=os.getenv("ESCALATION_EMAIL", "support@quadsci.ai"),
        sample_limit=int(os.getenv("SAMPLE_LIMIT", "5")),
    )