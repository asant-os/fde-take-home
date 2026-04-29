# tests/test_notifier.py

import pytest
from datetime import date
from unittest.mock import patch, MagicMock, PropertyMock

from app.notifier import SlackNotifier, EmailNotifier
from app.models import AlertRecord


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_notifier(
    base_url="http://localhost:9000/slack/webhook",
    single_webhook=None,
    details_url="https://app.example.com/accounts",
    max_retries=3,
    base_backoff=0.0,
    max_backoff=0.0,
) -> SlackNotifier:
    return SlackNotifier(
        base_url=base_url,
        single_webhook=single_webhook,
        details_url=details_url,
        max_retries=max_retries,
        base_backoff=base_backoff,
        max_backoff=max_backoff,
    )


def _make_alert(
    account_id="acct-1",
    account_name="Acme Corp",
    account_region="AMER",
    month=date(2026, 1, 1),
    arr=50_000,
    renewal_date=date(2026, 6, 1),
    account_owner="jane@example.com",
    duration_months=3,
    risk_start_month=date(2025, 10, 1),
) -> AlertRecord:
    return AlertRecord(
        account_id=account_id,
        account_name=account_name,
        account_region=account_region,
        month=month,
        arr=arr,
        renewal_date=renewal_date,
        account_owner=account_owner,
        duration_months=duration_months,
        risk_start_month=risk_start_month,
    )


def _mock_response(status_code=200, headers=None) -> MagicMock:
    response = MagicMock()
    response.status_code = status_code
    response.headers = headers or {}
    return response


# ---------------------------------------------------------------------------
# _resolve_url
# ---------------------------------------------------------------------------

class TestResolveUrl:
    def test_base_url_takes_precedence_over_single_webhook(self):
        notifier = _make_notifier(
            base_url="http://localhost:9000/slack/webhook",
            single_webhook="http://hooks.slack.com/single",
        )
        url = notifier._resolve_url("amer-risk-alerts")
        assert url == "http://localhost:9000/slack/webhook/amer-risk-alerts"

    def test_base_url_constructs_channel_url(self):
        notifier = _make_notifier(base_url="http://localhost:9000/slack/webhook")
        assert notifier._resolve_url("emea-risk-alerts") == "http://localhost:9000/slack/webhook/emea-risk-alerts"

    def test_falls_back_to_single_webhook(self):
        notifier = _make_notifier(
            base_url=None,
            single_webhook="http://hooks.slack.com/T123/B456/xyz",
        )
        assert notifier._resolve_url("any-channel") == "http://hooks.slack.com/T123/B456/xyz"

    def test_raises_if_neither_configured(self):
        notifier = _make_notifier(base_url=None, single_webhook=None)
        with pytest.raises(RuntimeError, match="No Slack URL configured"):
            notifier._resolve_url("amer-risk-alerts")


# ---------------------------------------------------------------------------
# send() — success
# ---------------------------------------------------------------------------

class TestSendSuccess:
    def test_returns_sent_status_on_200(self):
        notifier = _make_notifier()
        with patch("app.notifier.requests.post", return_value=_mock_response(200)):
            result = notifier.send("amer-risk-alerts", {"text": "alert"})
        assert result.status == "sent"

    def test_attempt_count_is_one_on_first_success(self):
        notifier = _make_notifier()
        with patch("app.notifier.requests.post", return_value=_mock_response(200)):
            result = notifier.send("amer-risk-alerts", {"text": "alert"})
        assert result.attempts == 1

    def test_posts_to_correct_url(self):
        notifier = _make_notifier(base_url="http://localhost:9000/slack/webhook")
        with patch("app.notifier.requests.post", return_value=_mock_response(200)) as mock_post:
            notifier.send("amer-risk-alerts", {"text": "alert"})
        mock_post.assert_called_once_with(
            "http://localhost:9000/slack/webhook/amer-risk-alerts",
            json={"text": "alert"},
            timeout=10,
        )


# ---------------------------------------------------------------------------
# send() — non-retryable failures
# ---------------------------------------------------------------------------

class TestSendNonRetryable:
    def test_returns_failed_on_4xx(self):
        notifier = _make_notifier()
        with patch("app.notifier.requests.post", return_value=_mock_response(400)):
            result = notifier.send("amer-risk-alerts", {"text": "alert"})
        assert result.status == "failed"
        assert "400" in result.error

    def test_no_retry_on_4xx(self):
        notifier = _make_notifier(max_retries=3)
        with patch("app.notifier.requests.post", return_value=_mock_response(403)) as mock_post:
            notifier.send("amer-risk-alerts", {"text": "alert"})
        assert mock_post.call_count == 1


# ---------------------------------------------------------------------------
# send() — retryable failures (429 / 5xx)
# ---------------------------------------------------------------------------

class TestSendRetryable:
    def test_retries_on_429(self):
        notifier = _make_notifier(max_retries=3)
        responses = [_mock_response(429), _mock_response(429), _mock_response(200)]
        with patch("app.notifier.requests.post", side_effect=responses):
            with patch("app.notifier.time.sleep"):
                result = notifier.send("amer-risk-alerts", {"text": "alert"})
        assert result.status == "sent"
        assert result.attempts == 3

    def test_retries_on_500(self):
        notifier = _make_notifier(max_retries=3)
        responses = [_mock_response(500), _mock_response(200)]
        with patch("app.notifier.requests.post", side_effect=responses):
            with patch("app.notifier.time.sleep"):
                result = notifier.send("amer-risk-alerts", {"text": "alert"})
        assert result.status == "sent"

    def test_fails_after_max_retries_exceeded(self):
        notifier = _make_notifier(max_retries=3)
        with patch("app.notifier.requests.post", return_value=_mock_response(500)):
            with patch("app.notifier.time.sleep"):
                result = notifier.send("amer-risk-alerts", {"text": "alert"})
        assert result.status == "failed"
        assert "max_retries_exceeded" in result.error

    def test_attempt_count_equals_max_retries_on_exhaustion(self):
        notifier = _make_notifier(max_retries=3)
        with patch("app.notifier.requests.post", return_value=_mock_response(500)):
            with patch("app.notifier.time.sleep"):
                result = notifier.send("amer-risk-alerts", {"text": "alert"})
        assert result.attempts == 3

    def test_honors_retry_after_header(self):
        notifier = _make_notifier(max_retries=2)
        responses = [
            _mock_response(429, headers={"Retry-After": "7"}),
            _mock_response(200),
        ]
        with patch("app.notifier.requests.post", side_effect=responses):
            with patch("app.notifier.time.sleep") as mock_sleep:
                notifier.send("amer-risk-alerts", {"text": "alert"})
        mock_sleep.assert_called_once_with(7.0)

    def test_falls_back_to_backoff_when_no_retry_after(self):
        notifier = _make_notifier(max_retries=2, base_backoff=2.0)
        responses = [_mock_response(429), _mock_response(200)]
        with patch("app.notifier.requests.post", side_effect=responses):
            with patch("app.notifier.time.sleep") as mock_sleep:
                notifier.send("amer-risk-alerts", {"text": "alert"})
        mock_sleep.assert_called_once_with(2.0)


# ---------------------------------------------------------------------------
# send() — network errors
# ---------------------------------------------------------------------------

class TestSendNetworkError:
    def test_retries_on_network_error(self):
        notifier = _make_notifier(max_retries=3)
        import requests as req
        side_effects = [req.RequestException("timeout"), _mock_response(200)]
        with patch("app.notifier.requests.post", side_effect=side_effects):
            with patch("app.notifier.time.sleep"):
                result = notifier.send("amer-risk-alerts", {"text": "alert"})
        assert result.status == "sent"

    def test_fails_after_max_network_errors(self):
        notifier = _make_notifier(max_retries=3)
        import requests as req
        with patch("app.notifier.requests.post", side_effect=req.RequestException("timeout")):
            with patch("app.notifier.time.sleep"):
                result = notifier.send("amer-risk-alerts", {"text": "alert"})
        assert result.status == "failed"
        assert "network_error" in result.error


# ---------------------------------------------------------------------------
# _parse_retry_after
# ---------------------------------------------------------------------------

class TestParseRetryAfter:
    def test_returns_float_from_numeric_header(self):
        response = _mock_response(headers={"Retry-After": "5"})
        assert SlackNotifier._parse_retry_after(response) == 5.0

    def test_returns_none_when_header_missing(self):
        response = _mock_response(headers={})
        assert SlackNotifier._parse_retry_after(response) is None

    def test_returns_none_for_non_numeric_value(self):
        response = _mock_response(headers={"Retry-After": "Fri, 01 Jan 2026 00:00:00 GMT"})
        assert SlackNotifier._parse_retry_after(response) is None


# ---------------------------------------------------------------------------
# format()
# ---------------------------------------------------------------------------

class TestFormat:
    def test_includes_account_name_and_id(self):
        notifier = _make_notifier()
        payload = notifier.format(_make_alert())
        assert "Acme Corp" in payload["text"]
        assert "acct-1" in payload["text"]

    def test_includes_region(self):
        notifier = _make_notifier()
        payload = notifier.format(_make_alert())
        assert "AMER" in payload["text"]

    def test_includes_duration_and_risk_start(self):
        notifier = _make_notifier()
        payload = notifier.format(_make_alert(duration_months=3, risk_start_month=date(2025, 10, 1)))
        assert "3 months" in payload["text"]
        assert "2025-10-01" in payload["text"]

    def test_includes_arr_formatted(self):
        notifier = _make_notifier()
        payload = notifier.format(_make_alert(arr=50_000))
        assert "$50,000" in payload["text"]

    def test_includes_renewal_date(self):
        notifier = _make_notifier()
        payload = notifier.format(_make_alert(renewal_date=date(2026, 6, 1)))
        assert "2026-06-01" in payload["text"]

    def test_renewal_date_unknown_when_none(self):
        notifier = _make_notifier()
        payload = notifier.format(_make_alert(renewal_date=None))
        assert "Unknown" in payload["text"]

    def test_arr_unknown_when_none(self):
        notifier = _make_notifier()
        payload = notifier.format(_make_alert(arr=None))
        assert "Unknown" in payload["text"]

    def test_includes_owner_when_present(self):
        notifier = _make_notifier()
        payload = notifier.format(_make_alert(account_owner="jane@example.com"))
        assert "jane@example.com" in payload["text"]

    def test_omits_owner_line_when_none(self):
        notifier = _make_notifier()
        payload = notifier.format(_make_alert(account_owner=None))
        assert "Owner" not in payload["text"]

    def test_includes_details_url(self):
        notifier = _make_notifier(details_url="https://app.example.com/accounts")
        payload = notifier.format(_make_alert(account_id="acct-1"))
        assert "https://app.example.com/accounts/acct-1" in payload["text"]

    def test_returns_dict_with_text_key(self):
        notifier = _make_notifier()
        payload = notifier.format(_make_alert())
        assert isinstance(payload, dict)
        assert "text" in payload


# ---------------------------------------------------------------------------
# EmailNotifier (stub)
# ---------------------------------------------------------------------------

class TestEmailNotifier:
    def test_logs_warning_with_recipient(self, caplog):
        import logging
        notifier = EmailNotifier(escalation_email="support@quadsci.ai")
        alerts = [_make_alert(account_id="acct-1", account_region="UNKNOWN")]

        with caplog.at_level(logging.WARNING, logger="app.notifier"):
            notifier.send_unknown_region_summary(alerts, run_id="run-123")

        assert "support@quadsci.ai" in caplog.text

    def test_logs_all_unrouted_accounts(self, caplog):
        import logging
        notifier = EmailNotifier(escalation_email="support@quadsci.ai")
        alerts = [
            _make_alert(account_id="acct-1", account_name="Acme"),
            _make_alert(account_id="acct-2", account_name="Globex"),
        ]

        with caplog.at_level(logging.WARNING, logger="app.notifier"):
            notifier.send_unknown_region_summary(alerts, run_id="run-123")

        assert "acct-1" in caplog.text
        assert "acct-2" in caplog.text

    def test_includes_run_id_in_log(self, caplog):
        import logging
        notifier = EmailNotifier(escalation_email="support@quadsci.ai")

        with caplog.at_level(logging.WARNING, logger="app.notifier"):
            notifier.send_unknown_region_summary([], run_id="run-abc-123")

        assert "run-abc-123" in caplog.text

    def test_no_exception_on_empty_alerts(self):
        notifier = EmailNotifier(escalation_email="support@quadsci.ai")
        notifier.send_unknown_region_summary([], run_id="run-123")  # should not raise