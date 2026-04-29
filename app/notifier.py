import time
import logging
import requests
from abc import ABC, abstractmethod
from app.models import AlertRecord, SendAlertResults

logger = logging.getLogger(__name__)

class AlertNotifier(ABC):
    @abstractmethod
    def send(self, channel: str, payload: dict) -> SendAlertResults:
        pass

    @abstractmethod
    def format(self, alert: AlertRecord) -> dict:
        pass

class EscalationNotifier(ABC):
    @abstractmethod
    def send_unknown_region_summary(self, alerts: list[AlertRecord], run_id: str) -> None:
        pass

class EmailNotifier(EscalationNotifier):
    def __init__(self, escalation_email: str):
        self.escalation_email = escalation_email

    def send_unknown_region_summary(self, alerts: list[AlertRecord], run_id: str) -> None:
        lines = [f"Unrouted accounts for run {run_id}:\n"]
        for a in alerts:
            lines.append(f"- {a.account_name} ({a.account_id}) | Region: {a.account_region}")

        body = "\n".join(lines)
        logger.warning(
            f"[EMAIL STUB] To: {self.escalation_email} | "
            f"Subject: [Risk Alerts] Unrouted accounts | "
            f"Body: {body}"
        )

class SlackNotifier(AlertNotifier):
    def __init__(
        self,
        base_url: str | None,
        single_webhook: str | None,
        details_url: str,
        max_retries: int,
        base_backoff: float,
        max_backoff: float = 30.0,
    ):
        self.base_url = base_url
        self.single_webhook = single_webhook
        self.details_url = details_url
        self.max_retries = max_retries
        self.base_backoff = base_backoff
        self.max_backoff = max_backoff

    def send(self, channel: str, payload: dict) -> SendAlertResults:
        url = self._resolve_url(channel)
        backoff = self.base_backoff
        attempt = 0

        while attempt < self.max_retries:
            attempt += 1
            try:
                response = requests.post(url, json=payload, timeout=10)
            except requests.RequestException as e:
                logger.warning(f"network error attempt={attempt} channel={channel} err={e}")
                if attempt >= self.max_retries:
                    return SendAlertResults(status="failed", attempts=attempt, error=f"network_error: {e}")
                
                time.sleep(backoff)
                backoff = min(backoff * 2, self.max_backoff)
                continue

            if response.status_code == 200:
                return SendAlertResults(status="sent", attempts=attempt)

            if response.status_code == 429 or response.status_code >= 500:
                retry_after = self._parse_retry_after(response)
                sleep_time = retry_after if retry_after is not None else backoff
                logger.info(
                    f"retryable status={response.status_code} attempt={attempt} "
                    f"channel={channel} sleep={sleep_time}s"
                )
                if attempt >= self.max_retries:
                    return SendAlertResults(status="failed", attempts=attempt, error=f"max_retries_exceeded_last_status_{response.status_code}")
                
                time.sleep(sleep_time)
                backoff = min(backoff * 2, self.max_backoff)
                continue

            # Non-retryable
            return SendAlertResults(status="failed", attempts=attempt, error=f"http_{response.status_code}")
        
        return SendAlertResults(status="failed", attempts=attempt, error="max_retries_exceeded")

    def _resolve_url(self, channel: str) -> str:
        if self.base_url:
            return f"{self.base_url}/{channel}"
        if self.single_webhook:
            return self.single_webhook
        raise RuntimeError("No Slack URL configured")
    
    @staticmethod
    def _parse_retry_after(response) -> float | None:
        header = response.headers.get("Retry-After")
        if header is None:
            return None
        try:
            return float(header)
        except (ValueError, TypeError):
            return None

    def format(self, alert: AlertRecord) -> dict:
        renewal = alert.renewal_date.isoformat() if alert.renewal_date else "Unknown"
        arr = f"${alert.arr:,}" if alert.arr is not None else "Unknown"
        owner_line = f"\nOwner: {alert.account_owner}" if alert.account_owner else ""

        text = (
            f"🚩 At Risk: {alert.account_name} ({alert.account_id})\n"
            f"Region: {alert.account_region}\n"
            f"At Risk for: {alert.duration_months} months "
            f"(since {alert.risk_start_month.isoformat()})\n"
            f"ARR: {arr}\n"
            f"Renewal date: {renewal}"
            f"{owner_line}\n"
            f"Details: {self.details_url}/{alert.account_id}"
        )
        return {"text": text}