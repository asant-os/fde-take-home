import logging
from datetime import date, datetime, timezone

from app.config import Config
from app.storage import open_uri
from app.reader import process_monthly_status
from app.repository import Repository
from app.notifier import AlertNotifier, EmailNotifier
from app.models import RunCounts, ReaderResults, RunStatusResponse, AlertOutcome, AlertRecord

logger = logging.getLogger(__name__)


class AlertService:
    def __init__(self, repo: Repository, config: Config, notifier: AlertNotifier, email_notifier: EmailNotifier):
        self.repo = repo
        self.config = config
        self.notifier = notifier
        self.email_notifier = email_notifier
    
    def run(self, source_uri: str, target_month: date, dry_run: bool) -> str:
        run_id = self.repo.create_run(source_uri, target_month, dry_run)

        try:
            results = self._process_file(source_uri, target_month)
            counts, unknown_region_alerts = self._process_run(run_id, dry_run, results)
            self.repo.update_run_counts(run_id, "succeeded", counts)
            logger.info(f"Run complete run_id={run_id} counts={counts}")
            self.email_notifier.send_unknown_region_summary(unknown_region_alerts, run_id)

        except Exception:
            self.repo.update_run_counts(run_id, "failed", RunCounts())
            logger.exception(f"Run failed run_id={run_id}")
            raise
        
        return run_id

    def preview(self, source_uri: str, target_month: date) -> list[AlertRecord]:
        results = self._process_file(source_uri, target_month)
        logger.info("Preview complete")
        return results.alerts

    def _process_file(self, source_uri, target_month) -> ReaderResults:
        fs, path = open_uri(source_uri)
        results = process_monthly_status(
            fs=fs,
            path=path,
            target_month=target_month,
            history_months=self.config.history_months,
            arr_threshold=self.config.arr_threshold,
        )

        return results
    
    def _process_run(self, run_id: str, dry_run: bool, results: ReaderResults, preview: bool = False) -> tuple[RunCounts, list]:
        run_counts = RunCounts(
            rows_scanned=results.rows_scanned, 
            duplicate_count=results.duplicate_count,
        )

        unknown_region_alerts = []

        for alert in results.alerts:
            # Check if alert has already been sent, if so increment skipped_replay counter and continue to next alert
            existing = self.repo.get_alert_outcome(alert.account_id, alert.month, "at_risk")
            if existing and existing.status == "sent":
                logger.info(f"Skipping replay account_id={alert.account_id}")
                run_counts.skipped_replay += 1
                continue

            # Check if the specific account region exists, if not, mark as unkown region
            channel = self.config.regions.get(alert.account_region) if alert.account_region else None
            if channel is None:
                logger.warning(
                    f"Unknown region account_id={alert.account_id} "
                    f"region={alert.account_region}"
                )

                if not preview:
                    self.repo.upsert_alert_outcome(
                        run_id=run_id,
                        alert=alert,
                        channel=None, 
                        status="failed", 
                        error="unknown_region", 
                        sent_at=None,
                    )

                unknown_region_alerts.append(alert)
                run_counts.unknown_regions += 1
                continue

            if dry_run or preview:
                if not preview:
                    self.repo.upsert_alert_outcome(
                        run_id=run_id, 
                        alert=alert, 
                        channel=channel, 
                        status="not-sent", 
                        error="", 
                        sent_at=None,
                    )
                continue

            payload = self.notifier.format(alert)
            send_result = self.notifier.send(channel, payload)

            if send_result.status == "sent":
                self.repo.upsert_alert_outcome(
                    run_id=run_id, 
                    alert=alert, 
                    channel=channel, 
                    status="sent",
                    sent_at=datetime.now(timezone.utc),
                )
                run_counts.alerts_sent += 1
            else:
                self.repo.upsert_alert_outcome(
                    run_id=run_id, 
                    alert=alert, 
                    channel=channel, 
                    status="failed",
                    error=send_result.error,
                    sent_at=None
                )
                run_counts.failed_deliveries += 1

        return run_counts, unknown_region_alerts