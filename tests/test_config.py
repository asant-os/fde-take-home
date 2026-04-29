# tests/test_config.py

import json
import pytest
from unittest.mock import patch
from pathlib import Path

from app.config import load_config_and_env


def _load_clean(config_file="nonexistent.json", env=None):
    """Load config with dotenv disabled and a clean environment."""
    with patch("app.config.load_dotenv"):
        with patch.dict("os.environ", env or {}, clear=True):
            return load_config_and_env(config_file=config_file)


# ---------------------------------------------------------------------------
# Defaults (no env, no config file)
# ---------------------------------------------------------------------------

class TestLoadConfigDefaults:
    def test_default_arr_threshold(self):
        assert _load_clean().arr_threshold == 50_000

    def test_default_history_months(self):
        assert _load_clean().history_months == 24

    def test_default_sample_limit(self):
        assert _load_clean().sample_limit == 5

    def test_slack_urls_default_to_none(self):
        config = _load_clean()
        assert config.slack_webhook_base_url is None
        assert config.slack_webhook_url is None

    def test_regions_default_to_empty_dict(self):
        assert _load_clean().regions == {}

    def test_default_slack_max_retries(self):
        assert _load_clean().slack_max_retries == 3

    def test_default_sqlite_path(self):
        assert _load_clean().sqlite_path == "./risk_alerts.db"


# ---------------------------------------------------------------------------
# Env overrides
# ---------------------------------------------------------------------------

class TestLoadConfigEnvOverrides:
    def test_arr_threshold_from_env(self):
        config = _load_clean(env={"ARR_THRESHOLD": "100000"})
        assert config.arr_threshold == 100_000

    def test_slack_webhook_base_url_from_env(self):
        config = _load_clean(env={"SLACK_WEBHOOK_BASE_URL": "http://localhost:9000/slack/webhook"})
        assert config.slack_webhook_base_url == "http://localhost:9000/slack/webhook"

    def test_slack_webhook_url_from_env(self):
        config = _load_clean(env={"SLACK_WEBHOOK_URL": "https://hooks.slack.com/T123/xyz"})
        assert config.slack_webhook_url == "https://hooks.slack.com/T123/xyz"

    def test_history_months_from_env(self):
        config = _load_clean(env={"HISTORY_MONTHS": "12"})
        assert config.history_months == 12

    def test_sample_limit_from_env(self):
        config = _load_clean(env={"SAMPLE_LIMIT": "10"})
        assert config.sample_limit == 10

    def test_slack_max_retries_from_env(self):
        config = _load_clean(env={"SLACK_MAX_RETRIES": "5"})
        assert config.slack_max_retries == 5

    def test_sqlite_path_from_env(self):
        config = _load_clean(env={"SQLITE_PATH": "/tmp/test.db"})
        assert config.sqlite_path == "/tmp/test.db"


# ---------------------------------------------------------------------------
# Config file (regions)
# ---------------------------------------------------------------------------

class TestLoadConfigFile:
    def test_loads_regions_from_config_file(self, tmp_path):
        config_file = tmp_path / "config.json"
        config_file.write_text(json.dumps({
            "regions": {
                "AMER": "amer-risk-alerts",
                "EMEA": "emea-risk-alerts",
                "APAC": "apac-risk-alerts",
            }
        }))
        config = _load_clean(config_file=str(config_file))
        assert config.regions == {
            "AMER": "amer-risk-alerts",
            "EMEA": "emea-risk-alerts",
            "APAC": "apac-risk-alerts",
        }

    def test_missing_config_file_gives_empty_regions(self):
        assert _load_clean().regions == {}

    def test_env_and_config_file_work_together(self, tmp_path):
        config_file = tmp_path / "config.json"
        config_file.write_text(json.dumps({"regions": {"AMER": "amer-risk-alerts"}}))
        config = _load_clean(config_file=str(config_file), env={"ARR_THRESHOLD": "75000"})
        assert config.arr_threshold == 75_000
        assert config.regions == {"AMER": "amer-risk-alerts"}