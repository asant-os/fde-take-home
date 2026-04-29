import pyarrow.parquet as pq
import pyarrow.fs as pafs
import pandas as pd
from datetime import date
from dateutil.relativedelta import relativedelta

from app.models import AlertRecord, ReaderResults, ACCOUNT_COLUMNS, ACCOUNT_HISTORY_COLUMNS

def read_parquet(
        fs: pafs.FileSystem,
        path: str,
        columns: list[str],
        filters: list,
) -> pd.DataFrame:
    
    table = pq.read_table(
        path,
        filesystem=fs,
        columns=columns,
        filters=filters
    )

    return table.to_pandas()

def get_parquet_rows_scanned(fs: pafs.FileSystem, path: str, target_month: date) -> int:
    meta = pq.read_metadata(path, filesystem=fs)
    return meta.num_rows  # total rows in file

def deduplicate(df: pd.DataFrame, sort_value: str, subset: list[str]) -> tuple[pd.DataFrame, int]:
    before = len(df)
    df_sorted = df.sort_values(sort_value, ascending=False)
    df_deduped = df_sorted.drop_duplicates(subset=subset, keep="first")
    return df_deduped, before - len(df_deduped)

def compute_duration(
    account_id: str,
    target_month: date,
    earliest_month: date,
    status_lookup: dict[tuple[str, date], str],
) -> int:
    duration = 1
    idx = target_month - relativedelta(months=1)

    while idx >= earliest_month:
        status = status_lookup.get((account_id, idx))
        if status != "At Risk":
            break
        duration += 1
        idx -= relativedelta(months=1)

    return duration

def process_monthly_status(
        fs: pafs.FileSystem,
        path: str,
        target_month: date,
        history_months: int,
        arr_threshold: int,
) -> ReaderResults:

    accounts_df = read_parquet(
        fs=fs,
        path=path,
        columns=ACCOUNT_COLUMNS,
        filters=[
            ("month", "==", pd.Timestamp(target_month)),
            ("status", "==", "At Risk"),
            ("arr", ">=", arr_threshold),  
        ]
    )

    if accounts_df.empty:
        return ReaderResults(alerts=[], duplicate_count=0)
    
    accounts_df["month"] = pd.to_datetime(accounts_df["month"]).dt.date
    accounts_df["renewal_date"] = pd.to_datetime(accounts_df["renewal_date"], errors="coerce").dt.date

    accounts_df, account_dupes = deduplicate(accounts_df, sort_value="updated_at", subset=["account_id", "month"])
    account_ids = accounts_df["account_id"].tolist()

    earliest_month = target_month - relativedelta(months=history_months)
    history_df = read_parquet(
        fs=fs,
        path=path,
        columns=ACCOUNT_HISTORY_COLUMNS,
        filters=[
            ("account_id", "in", account_ids),
            ("month", ">=", pd.Timestamp(earliest_month)),
            ("month", "<=", pd.Timestamp(target_month))
        ]
    )

    history_df["month"] = pd.to_datetime(history_df["month"]).dt.date

    rows_scanned = len(history_df)
    history_df, history_dupes = deduplicate(history_df, sort_value="updated_at", subset=["account_id", "month"])
    total_duplicates = history_dupes

    status_lookup = {
        (row.account_id, row.month): row.status
        for row in history_df.itertuples()
    }

    alerts = []
    for row in accounts_df.itertuples():
        duration = compute_duration(
            row.account_id, target_month, earliest_month, status_lookup
        )
        risk_start_month = target_month - relativedelta(months=duration - 1)

        alerts.append(AlertRecord(
            account_id=row.account_id,
            account_name=row.account_name,
            account_region=row.account_region if pd.notna(row.account_region) else None,
            month=target_month,
            arr=int(row.arr) if pd.notna(row.arr) else None,
            renewal_date=row.renewal_date if pd.notna(row.renewal_date) else None,
            account_owner=row.account_owner if pd.notna(row.account_owner) else None,
            duration_months=duration,
            risk_start_month=risk_start_month,
        ))

    return ReaderResults(alerts=alerts, rows_scanned=rows_scanned, duplicate_count=total_duplicates) 