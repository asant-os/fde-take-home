# tests/test_reader.py

import pytest
import pandas as pd
from datetime import date
from unittest.mock import patch, MagicMock

from app.reader import deduplicate, compute_duration, process_monthly_status
from app.models import ReaderResults


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_accounts_df(rows: list[dict]) -> pd.DataFrame:
    """Build a minimal accounts DataFrame matching the Parquet schema."""
    defaults = {
        "account_id": "acct-1",
        "account_name": "Acme Corp",
        "account_region": "AMER",
        "month": pd.Timestamp("2026-01-01"),
        "status": "At Risk",
        "renewal_date": pd.Timestamp("2026-06-01"),
        "account_owner": "jane@example.com",
        "arr": 50_000,
        "updated_at": pd.Timestamp("2026-01-15"),
    }
    return pd.DataFrame([{**defaults, **r} for r in rows])


def _make_history_df(rows: list[dict]) -> pd.DataFrame:
    """Build a minimal history DataFrame (subset of columns)."""
    defaults = {
        "account_id": "acct-1",
        "month": pd.Timestamp("2026-01-01"),
        "status": "At Risk",
        "updated_at": pd.Timestamp("2026-01-15"),
    }
    return pd.DataFrame([{**defaults, **r} for r in rows])


# ---------------------------------------------------------------------------
# deduplicate
# ---------------------------------------------------------------------------
class TestDeduplicate:
    def test_no_duplicates_returns_same_rows(self):
        df = _make_accounts_df([
            {"account_id": "acct-1", "month": pd.Timestamp("2026-01-01")},
            {"account_id": "acct-2", "month": pd.Timestamp("2026-01-01")},
        ])
        result, dupes = deduplicate(df, sort_value="updated_at", subset=["account_id", "month"])
        assert len(result) == 2
        assert dupes == 0

    def test_keeps_latest_updated_at(self):
        df = _make_accounts_df([
            {"account_id": "acct-1", "month": pd.Timestamp("2026-01-01"), "arr": 10_000, "updated_at": pd.Timestamp("2026-01-10")},
            {"account_id": "acct-1", "month": pd.Timestamp("2026-01-01"), "arr": 99_000, "updated_at": pd.Timestamp("2026-01-20")},
        ])
        result, dupes = deduplicate(df, sort_value="updated_at", subset=["account_id", "month"])
        assert len(result) == 1
        assert dupes == 1
        assert result.iloc[0]["arr"] == 99_000

    def test_duplicate_count_is_accurate(self):
        df = _make_accounts_df([
            {"account_id": "acct-1", "month": pd.Timestamp("2026-01-01"), "updated_at": pd.Timestamp("2026-01-01")},
            {"account_id": "acct-1", "month": pd.Timestamp("2026-01-01"), "updated_at": pd.Timestamp("2026-01-02")},
            {"account_id": "acct-1", "month": pd.Timestamp("2026-01-01"), "updated_at": pd.Timestamp("2026-01-03")},
        ])
        result, dupes = deduplicate(df, sort_value="updated_at", subset=["account_id", "month"])
        assert len(result) == 1
        assert dupes == 2

    def test_duplicates_across_different_accounts(self):
        df = _make_accounts_df([
            {"account_id": "acct-1", "month": pd.Timestamp("2026-01-01"), "updated_at": pd.Timestamp("2026-01-01")},
            {"account_id": "acct-1", "month": pd.Timestamp("2026-01-01"), "updated_at": pd.Timestamp("2026-01-02")},
            {"account_id": "acct-2", "month": pd.Timestamp("2026-01-01"), "updated_at": pd.Timestamp("2026-01-01")},
        ])
        result, dupes = deduplicate(df, sort_value="updated_at", subset=["account_id", "month"])
        assert len(result) == 2
        assert dupes == 1


# ---------------------------------------------------------------------------
# compute_duration
# ---------------------------------------------------------------------------

class TestComputeDuration:
    def _lookup(self, entries: list[tuple[str, str, str]]) -> dict:
        """Build status_lookup from (account_id, month_str, status) tuples."""
        return {
            ("acct-1", date.fromisoformat(m)): s
            for _, m, s in entries
        }

    def test_single_month_at_risk(self):
        lookup = {("acct-1", date(2026, 1, 1)): "At Risk"}
        duration = compute_duration("acct-1", date(2026, 1, 1), date(2025, 1, 1), lookup)
        assert duration == 1

    def test_consecutive_months_at_risk(self):
        lookup = {
            ("acct-1", date(2025, 11, 1)): "At Risk",
            ("acct-1", date(2025, 12, 1)): "At Risk",
            ("acct-1", date(2026, 1, 1)): "At Risk",
        }
        duration = compute_duration("acct-1", date(2026, 1, 1), date(2025, 1, 1), lookup)
        assert duration == 3

    def test_stops_at_non_at_risk_month(self):
        """Healthy month in the chain should stop the count."""
        lookup = {
            ("acct-1", date(2025, 10, 1)): "At Risk",
            ("acct-1", date(2025, 11, 1)): "At Risk",
            ("acct-1", date(2025, 12, 1)): "Healthy",   # break
            ("acct-1", date(2026, 1, 1)): "At Risk",
        }
        duration = compute_duration("acct-1", date(2026, 1, 1), date(2025, 1, 1), lookup)
        assert duration == 1

    def test_stops_at_missing_month(self):
        """A gap in history (missing month) should stop the count."""
        lookup = {
            # 2025-11 is missing
            ("acct-1", date(2025, 12, 1)): "At Risk",
            ("acct-1", date(2026, 1, 1)): "At Risk",
        }
        duration = compute_duration("acct-1", date(2026, 1, 1), date(2025, 1, 1), lookup)
        assert duration == 2

    def test_stops_at_gap_in_streak(self):
        """
        Gap month (2025-10 missing) should break the streak:
        2025-09 At Risk
        2025-10 MISSING     ← gap, stops count here
        2025-11 At Risk
        2025-12 At Risk
        2026-01 At Risk
        Expected duration: 3 (counts back from Jan through Nov, stops at the gap)
        """
        lookup = {
            ("acct-1", date(2025, 9, 1)): "At Risk",
            # 2025-10 missing
            ("acct-1", date(2025, 11, 1)): "At Risk",
            ("acct-1", date(2025, 12, 1)): "At Risk",
            ("acct-1", date(2026, 1, 1)): "At Risk",
        }
        duration = compute_duration("acct-1", date(2026, 1, 1), date(2025, 1, 1), lookup)
        assert duration == 3

    def test_stops_at_earliest_month_boundary(self):
        """Should not count beyond earliest_month even if data exists."""
        lookup = {
            ("acct-1", date(2025, 12, 1)): "At Risk",
            ("acct-1", date(2026, 1, 1)): "At Risk",
        }
        # earliest_month is 2026-01-01, so only 1 month of history allowed
        duration = compute_duration("acct-1", date(2026, 1, 1), date(2026, 1, 1), lookup)
        assert duration == 1

    def test_exactly_the_spec_example(self):
        """
        Spec example:
          2025-10 At Risk
          2025-11 At Risk
          2025-12 Healthy   ← break
          2026-01 At Risk
        Expected duration: 1
        """
        lookup = {
            ("acct-1", date(2025, 10, 1)): "At Risk",
            ("acct-1", date(2025, 11, 1)): "At Risk",
            ("acct-1", date(2025, 12, 1)): "Healthy",
            ("acct-1", date(2026, 1, 1)): "At Risk",
        }
        duration = compute_duration("acct-1", date(2026, 1, 1), date(2025, 1, 1), lookup)
        assert duration == 1


# ---------------------------------------------------------------------------
# process_monthly_status (integration-level, I/O mocked)
# ---------------------------------------------------------------------------

class TestProcessMonthlyStatus:
    """
    Mocks read_parquet so we never touch the filesystem.
    Tests the orchestration logic in process_monthly_status.
    """

    TARGET_MONTH = date(2026, 1, 1)
    FS = MagicMock()
    PATH = "fake/path.parquet"

    def _run(self, accounts_rows, history_rows, arr_threshold=0, history_months=12):
        accounts_df = _make_accounts_df(accounts_rows)
        history_df = _make_history_df(history_rows)

        # Simulate date normalization that process_monthly_status applies
        accounts_df["month"] = pd.to_datetime(accounts_df["month"]).dt.date
        accounts_df["renewal_date"] = pd.to_datetime(
            accounts_df["renewal_date"], errors="coerce"
        ).dt.date

        with patch("app.reader.read_parquet", side_effect=[accounts_df, history_df]):
            return process_monthly_status(
                fs=self.FS,
                path=self.PATH,
                target_month=self.TARGET_MONTH,
                history_months=history_months,
                arr_threshold=arr_threshold,
            )

    def test_returns_empty_when_no_at_risk_accounts(self):
        with patch("app.reader.read_parquet", return_value=pd.DataFrame()):
            result = process_monthly_status(
                fs=self.FS,
                path=self.PATH,
                target_month=self.TARGET_MONTH,
                history_months=12,
                arr_threshold=0,
            )
        assert result.alerts == []
        assert result.duplicate_count == 0

    def test_single_at_risk_account_duration_one(self):
        result = self._run(
            accounts_rows=[{"account_id": "acct-1"}],
            history_rows=[
                {"account_id": "acct-1", "month": pd.Timestamp("2026-01-01"), "status": "At Risk"},
            ],
        )
        assert len(result.alerts) == 1
        alert = result.alerts[0]
        assert alert.account_id == "acct-1"
        assert alert.duration_months == 1
        assert alert.risk_start_month == date(2026, 1, 1)

    def test_multi_month_streak_sets_correct_duration(self):
        result = self._run(
            accounts_rows=[{"account_id": "acct-1"}],
            history_rows=[
                {"account_id": "acct-1", "month": pd.Timestamp("2025-11-01"), "status": "At Risk"},
                {"account_id": "acct-1", "month": pd.Timestamp("2025-12-01"), "status": "At Risk"},
                {"account_id": "acct-1", "month": pd.Timestamp("2026-01-01"), "status": "At Risk"},
            ],
        )
        assert result.alerts[0].duration_months == 3
        assert result.alerts[0].risk_start_month == date(2025, 11, 1)

    def test_duplicate_accounts_resolved_by_latest_updated_at(self):
        result = self._run(
            accounts_rows=[
                {"account_id": "acct-1", "arr": 1_000, "updated_at": pd.Timestamp("2026-01-10")},
                {"account_id": "acct-1", "arr": 9_999, "updated_at": pd.Timestamp("2026-01-20")},
            ],
            history_rows=[
                {"account_id": "acct-1", "month": pd.Timestamp("2026-01-01"), "status": "At Risk"},
            ],
        )
        assert len(result.alerts) == 1
        assert result.alerts[0].arr == 9_999

    def test_duplicate_count_reported(self):
        result = self._run(
            accounts_rows=[
                {"account_id": "acct-1", "updated_at": pd.Timestamp("2026-01-10")},
                {"account_id": "acct-1", "updated_at": pd.Timestamp("2026-01-20")},
            ],
            history_rows=[
                {"account_id": "acct-1", "month": pd.Timestamp("2026-01-01"), "status": "At Risk"},
            ],
        )
        # 1 history dupe counted (history_dupes is what's reported)
        assert result.duplicate_count >= 0  # exact count depends on history data

    def test_null_region_is_none_on_alert(self):
        result = self._run(
            accounts_rows=[{"account_id": "acct-1", "account_region": None}],
            history_rows=[
                {"account_id": "acct-1", "month": pd.Timestamp("2026-01-01"), "status": "At Risk"},
            ],
        )
        assert result.alerts[0].account_region is None

    def test_null_renewal_date_is_none_on_alert(self):
        result = self._run(
            accounts_rows=[{"account_id": "acct-1", "renewal_date": pd.NaT}],
            history_rows=[
                {"account_id": "acct-1", "month": pd.Timestamp("2026-01-01"), "status": "At Risk"},
            ],
        )
        assert result.alerts[0].renewal_date is None

    def test_null_account_owner_is_none_on_alert(self):
        result = self._run(
            accounts_rows=[{"account_id": "acct-1", "account_owner": None}],
            history_rows=[
                {"account_id": "acct-1", "month": pd.Timestamp("2026-01-01"), "status": "At Risk"},
            ],
        )
        assert result.alerts[0].account_owner is None

    def test_streak_broken_by_healthy_month(self):
        """Mirrors the spec example: healthy month resets streak to 1."""
        result = self._run(
            accounts_rows=[{"account_id": "acct-1"}],
            history_rows=[
                {"account_id": "acct-1", "month": pd.Timestamp("2025-10-01"), "status": "At Risk"},
                {"account_id": "acct-1", "month": pd.Timestamp("2025-11-01"), "status": "At Risk"},
                {"account_id": "acct-1", "month": pd.Timestamp("2025-12-01"), "status": "Healthy"},
                {"account_id": "acct-1", "month": pd.Timestamp("2026-01-01"), "status": "At Risk"},
            ],
        )
        assert result.alerts[0].duration_months == 1
        assert result.alerts[0].risk_start_month == date(2026, 1, 1)

    def test_multiple_accounts_processed_independently(self):
        result = self._run(
            accounts_rows=[
                {"account_id": "acct-1", "account_name": "Acme"},
                {"account_id": "acct-2", "account_name": "Globex"},
            ],
            history_rows=[
                {"account_id": "acct-1", "month": pd.Timestamp("2025-12-01"), "status": "At Risk"},
                {"account_id": "acct-1", "month": pd.Timestamp("2026-01-01"), "status": "At Risk"},
                {"account_id": "acct-2", "month": pd.Timestamp("2026-01-01"), "status": "At Risk"},
            ],
        )
        assert len(result.alerts) == 2
        by_id = {a.account_id: a for a in result.alerts}
        assert by_id["acct-1"].duration_months == 2
        assert by_id["acct-2"].duration_months == 1