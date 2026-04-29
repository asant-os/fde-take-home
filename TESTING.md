# Testing

## Running Tests

```bash
pytest tests/ -v
```

## Coverage

| File | Module |
|------|--------|
| `test_reader.py` | `app/reader.py` |
| `test_repository.py` | `app/repository.py` |
| `test_service.py` | `app/service.py` |
| `test_storage.py` | `app/storage.py` |
| `test_notifier.py` | `app/notifier.py` |
| `test_main.py` | `app/main.py` |
| `test_config.py` | `app/config.py` |

## Approach

Tests are unit-scoped — each module is tested in isolation with external dependencies mocked. The exception is `test_repository.py`, which uses a real in-memory SQLite database (`sqlite:///:memory:`) because mocking SQLAlchemy wouldn't actually verify that queries or constraints work.

Slack HTTP calls and `time.sleep` are mocked in notifier tests so retries run instantly. Storage tests mock GCS/S3 constructors to avoid credential resolution.

## Key Behaviors Tested

**Reader**: deduplication (latest `updated_at` wins), duration calculation, gap/healthy-month handling, null field mapping. The spec example (Healthy month resets streak to 1) is tested explicitly.

**Repository**: CRUD for runs and alert outcomes, idempotency constraint on `(account_id, month, alert_type)`, replay behavior.

**Service**: happy path send, skipped replay, unknown/null region handling, dry run, preview (no DB writes), failed send (run still completes), exception path marks run failed.

**Notifier**: URL resolution (`base_url` takes precedence over `single_webhook`), retry on 429/5xx, `Retry-After` header honored, network error handling, message format.

**Config**: `load_dotenv` is patched to a no-op in all tests so `.env` values don't leak into default assertions.

**API**: HTTP status codes, request parsing, error mapping (404/422/500). `AlertService` is mocked — business logic isn't re-tested here.