(.venv) adriano@DESKTOP-CD36J78:~/fde-take-home/fde-take-home$ curl -s -X POST http://localhost:8000/preview \
  -H "Content-Type: application/json" \
  -d '{"source_uri": "gs://fde-take-home-asantos/monthly_account_status.parquet", "month": "2026-01-01", "dry_run": false}' \
  | python3 -m json.tool
{
    "month": "2026-01-01",
    "alert_count": 141,
    "alerts": [
        {
            "account_id": "a00636",
            "account_name": "Account 0636",
            "account_region": "EMEA",
            "month": "2026-01-01",
            "arr": 10211,
            "renewal_date": "2026-06-01",
            "account_owner": "owner36@example.com",
            "duration_months": 2,
            "risk_start_month": "2025-12-01"
        },
        {
            "account_id": "a00076",
            "account_name": "Account 0076",
            "account_region": "APAC",
            "month": "2026-01-01",
            "arr": 57133,
            "renewal_date": null,
            "account_owner": "owner26@example.com",
            "duration_months": 1,
            "risk_start_month": "2026-01-01"
        },
        {
            "account_id": "a00570",
            "account_name": "Account 0570",
            "account_region": "AMER",
            "month": "2026-01-01",
            "arr": 26640,
            "renewal_date": "2026-05-01",
            "account_owner": "owner20@example.com",
            "duration_months": 4,
            "risk_start_month": "2025-10-01"
        },
        {
            "account_id": "a00377",
            "account_name": "Account 0377",
            "account_region": "EMEA",
            "month": "2026-01-01",
            "arr": 85178,
            "renewal_date": "2026-03-01",
            "account_owner": "owner27@example.com",
            "duration_months": 1,
            "risk_start_month": "2026-01-01"
        },
        {
            "account_id": "a00288",
            "account_name": "Account 0288",
            "account_region": "EMEA",
            "month": "2026-01-01",
            "arr": 56745,
            "renewal_date": null,
            "account_owner": "owner38@example.com",
            "duration_months": 5,
            "risk_start_month": "2025-09-01"
        }
    ]
}
(.venv) adriano@DESKTOP-CD36J78:~/fde-take-home/fde-take-home$ curl -s -X POST http://localhost:8000/runs \
  -H "Content-Type: application/json" \
  -d '{"source_uri": "gs://fde-take-home-asantos/monthly_account_status.parquet", "month": "2026-01-01", "dry_run": false}' \
  | python3 -m json.tool
{
    "run_id": "8cc57b57-f506-45bc-80bc-4c253fba794e"
}
(.venv) adriano@DESKTOP-CD36J78:~/fde-take-home/fde-take-home$ curl -s http://localhost:8000/runs/8cc57b57-f506-45bc-80bc-4c253fba794e | python3 -m json.tool
{
    "run_id": "8cc57b57-f506-45bc-80bc-4c253fba794e",
    "status": "succeeded",
    "month": "2026-01-01",
    "dry_run": false,
    "counts": {
        "rows_scanned": 1868,
        "alerts_sent": 0,
        "skipped_replay": 137,
        "failed_deliveries": 0,
        "duplicate_count": 58,
        "unknown_regions": 4
    },
    "sample_alerts": [],
    "sample_errors": [
        {
            "account_id": "a00090",
            "account_name": "Account 0090",
            "channel": null,
            "status": "failed",
            "error": "unknown_region"
        },
        {
            "account_id": "a00559",
            "account_name": "Account 0559",
            "channel": null,
            "status": "failed",
            "error": "unknown_region"
        },
        {
            "account_id": "a00593",
            "account_name": "Account 0593",
            "channel": null,
            "status": "failed",
            "error": "unknown_region"
        },
        {
            "account_id": "a00769",
            "account_name": "Account 0769",
            "channel": null,
            "status": "failed",
            "error": "unknown_region"
        }
    ]
}

(.venv) adriano@DESKTOP-CD36J78:~/fde-take-home/fde-take-home$ curl -s "http://localhost:9000/logs?limit=10" | python3 -c "
import json, sys
data = json.load(sys.stdin)
for r in data['records']:
    if r['status_code'] == 200:
        print(f\"[{r['ts']}] #{r['channel']}\")
        print(r['payload']['text'])
        print()
"
[2026-04-29T15:43:55.252734+00:00] #emea-risk-alerts
🚩 At Risk: Account 0338 (a00338)
Region: EMEA
At Risk for: 6 months (since 2025-08-01)
ARR: $87,056
Renewal date: 2026-04-01
Owner: owner38@example.com
Details: https://app.yourcompany.com/accounts/a00338

[2026-04-29T15:43:55.256212+00:00] #amer-risk-alerts
🚩 At Risk: Account 0205 (a00205)
Region: AMER
At Risk for: 1 months (since 2026-01-01)
ARR: $30,029
Renewal date: 2026-06-01
Owner: owner05@example.com
Details: https://app.yourcompany.com/accounts/a00205

[2026-04-29T15:43:55.259269+00:00] #emea-risk-alerts
🚩 At Risk: Account 0732 (a00732)
Region: EMEA
At Risk for: 1 months (since 2026-01-01)
ARR: $63,420
Renewal date: 2026-05-01
Details: https://app.yourcompany.com/accounts/a00732

[2026-04-29T15:43:55.263739+00:00] #apac-risk-alerts
🚩 At Risk: Account 0714 (a00714)
Region: APAC
At Risk for: 1 months (since 2026-01-01)
ARR: $96,057
Renewal date: 2026-05-01
Owner: owner14@example.com
Details: https://app.yourcompany.com/accounts/a00714

[2026-04-29T15:43:55.268558+00:00] #emea-risk-alerts
🚩 At Risk: Account 0537 (a00537)
Region: EMEA
At Risk for: 1 months (since 2026-01-01)
ARR: $10,302
Renewal date: Unknown
Owner: owner37@example.com
Details: https://app.yourcompany.com/accounts/a00537

[2026-04-29T15:43:55.272291+00:00] #emea-risk-alerts
🚩 At Risk: Account 0515 (a00515)
Region: EMEA
At Risk for: 4 months (since 2025-10-01)
ARR: $35,628
Renewal date: 2026-03-01
Owner: owner15@example.com
Details: https://app.yourcompany.com/accounts/a00515

[2026-04-29T15:43:55.275469+00:00] #emea-risk-alerts
🚩 At Risk: Account 0054 (a00054)
Region: EMEA
At Risk for: 2 months (since 2025-12-01)
ARR: $12,026
Renewal date: 2026-02-01
Owner: owner04@example.com
Details: https://app.yourcompany.com/accounts/a00054

[2026-04-29T15:43:55.279104+00:00] #amer-risk-alerts
🚩 At Risk: Account 0278 (a00278)
Region: AMER
At Risk for: 7 months (since 2025-07-01)
ARR: $84,411
Renewal date: 2026-03-01
Owner: owner28@example.com
Details: https://app.yourcompany.com/accounts/a00278

[2026-04-29T15:43:55.283064+00:00] #emea-risk-alerts
🚩 At Risk: Account 0682 (a00682)
Region: EMEA
At Risk for: 2 months (since 2025-12-01)
ARR: $86,214
Renewal date: 2026-04-01
Owner: owner32@example.com
Details: https://app.yourcompany.com/accounts/a00682

[2026-04-29T15:43:55.288139+00:00] #amer-risk-alerts
🚩 At Risk: Account 0167 (a00167)
Region: AMER
At Risk for: 1 months (since 2026-01-01)
ARR: $92,461
Renewal date: 2026-06-01
Owner: owner17@example.com
Details: https://app.yourcompany.com/accounts/a00167

(.venv) adriano@DESKTOP-CD36J78:~/fde-take-home/fde-take-home$ curl -s -X POST http://localhost:8000/runs \
  -H "Content-Type: application/json" \
  -d '{"source_uri": "gs://fde-take-home-asantos/monthly_account_status.parquet", "month": "2026-01-01", "dry_run": false}' \
  | python3 -m json.tool
{
    "run_id": "0273f4fd-0a5b-498f-90d0-857419a3898d"
}
(.venv) adriano@DESKTOP-CD36J78:~/fde-take-home/fde-take-home$ curl -s http://localhost:8000/runs/0273f4fd-0a5b-498f-90d0-857419a3898d | python3 -m json.tool
{
    "run_id": "0273f4fd-0a5b-498f-90d0-857419a3898d",
    "status": "succeeded",
    "month": "2026-01-01",
    "dry_run": false,
    "counts": {
        "rows_scanned": 1868,
        "alerts_sent": 0,
        "skipped_replay": 137,
        "failed_deliveries": 0,
        "duplicate_count": 58,
        "unknown_regions": 4
    },
    "sample_alerts": [],
    "sample_errors": [
        {
            "account_id": "a00090",
            "account_name": "Account 0090",
            "channel": null,
            "status": "failed",
            "error": "unknown_region"
        },
        {
            "account_id": "a00559",
            "account_name": "Account 0559",
            "channel": null,
            "status": "failed",
            "error": "unknown_region"
        },
        {
            "account_id": "a00593",
            "account_name": "Account 0593",
            "channel": null,
            "status": "failed",
            "error": "unknown_region"
        },
        {
            "account_id": "a00769",
            "account_name": "Account 0769",
            "channel": null,
            "status": "failed",
            "error": "unknown_region"
        }
    ]
}