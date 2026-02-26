# pyapiary

A clean, modular set of Python connectors and utilities for working with both **APIs** and **DBMS backends**, unified by a centralized `Broker` abstraction and a consistent interface. Designed for easy testing, code reuse, and plug-and-play extensibility.

## ⚠️ **Deprecation notice**

`pyapiary` is the successor to the `ppp-connectors` package.
The original `ppp-connectors` package has been frozen at its final 1.1.13 release
and will continue to remain available on PyPI for existing users.
New development and releases are published under the `pyapiary` package name.

## 📚 Table of Contents

- [Installation](#-installation)
- [API Connectors](#-api-connectors)
  - [Shared Features](#-shared-features)
  - [Sync Example (URLScan)](#-sync-example-urlscan)
  - [Async Example (URLScan)](#-async-example-urlscan)
  - [Customizing API Requests with `**kwargs`](#customizing-api-requests-with-kwargs)
  - [Proxy Awareness](#proxy-awareness)
  - [SSL Verification and Per-Request Options](#ssl-verification-and-per-request-options)
- [DBMS Connectors](#-dbms-connectors)
  - [MongoDB](#mongodb)
  - [Elasticsearch](#elasticsearch)
  - [ODBC](#odbc-eg-postgres-teradata)
  - [Splunk](#splunk)
- [Testing](#-testing)
  - [Unit tests](#-unit-tests)
  - [Integration tests](#-integration-tests)
  - [Suppress warnings](#-suppress-warnings)
- [Contributing / Adding a Connector](#-contributing--adding-a-connector)
- [Dev Environment](#-dev-environment)
- [Secrets and Redaction](#-secrets-and-redaction)
- [Summary](#-summary)



---

## 📦 Installation

```bash
pip install ppp-connectors
```

Copy the `.env.example` to `.env` for local development:

```bash
cp dev_env/.env.example dev_env/.env
```

Environment variables are loaded automatically via the `combine_env_configs()` helper.

---

## 🔌 API Connectors

All API connectors inherit from a common `Broker` abstraction that comes in two flavors:

- `Broker` for synchronous usage
- `AsyncBroker` for asynchronous usage

Each API connector has both a sync and async version (e.g., `URLScanConnector` and `AsyncURLScanConnector`) with **identical method names** and consistent behavior. Additionally, both support context-management with `with` and `async with`.

### 🧰 Shared Features

- Accept API credentials via env vars or constructor args (`load_env_vars=True`)
- Unified interface: `.get()`, `.post()`, etc.
- Custom headers, query params, and body data via `**kwargs`
- Logging, retry/backoff support
- Proxy and SSL configuration
- Optional VCR integration for tests

> Choose the version based on your environment:
> - Use `URLScanConnector` in CLI scripts and sync jobs
> - Use `AsyncURLScanConnector` in FastAPI or async pipelines

---

### 🌐 Sync Example (URLScan)
``` python
from pyapiary.api_connectors.urlscan import URLScanConnector

scanner = URLScanConnector(load_env_vars=True)
result = scanner.scan(url="https://example.com")
print(result.json())
```
---

### ⚡ Async Example (URLScan)
``` python
import asyncio
from pyapiary.api_connectors.urlscan import AsyncURLScanConnector

async def main():
   scanner = AsyncURLScanConnector(load_env_vars=True)
   response = await scanner.scan(url="https://example.com")
   print(await response.json())

asyncio.run(main())
```

### Customizing API Requests with **kwargs

All connector methods accept arbitrary keyword arguments using `**kwargs`. These arguments are passed directly to the underlying `httpx` request methods, enabling support for any feature available in [`httpx`](https://www.python-httpx.org/api/#request) — including custom headers, query parameters, timeouts, authentication, and more. Additionally, for APIs that accept arbitrary fields in their request body (like `URLScan`), these can also be passed as part of `**kwargs` and will be merged into the outgoing request. This enables full control over how API requests are constructed without needing to modify connector internals.

#### Example (URLScan with custom headers and params)

```python
result = scanner.scan(
    url="https://example.com",
    visibility="unlisted",
    headers={"X-Custom-Header": "my-value"},
    params={"pretty": "true"}
)
```

This pattern allows flexibility without needing to subclass or modify the connector.

### Proxy Awareness

API connectors inherit from the `Broker` class and support flexible proxy configuration for outgoing HTTP requests. You can set proxies in multiple ways:
- a single `proxy` parameter (applies to all requests),
- a per-scheme `mounts` parameter (e.g., separate proxies for `http` and `https` as a dictionary),
- or environment variables (from `.env` or OS environment, specifically `HTTP_PROXY` and `HTTPS_PROXY`).
> 🧠 **Note for async connectors:** Per-scheme `mounts` are not supported by `httpx.AsyncClient`. If you pass `mounts` to an async connector, it will raise a `ValueError`. Use the `proxy` argument or rely on environment variables (`load_env_vars=True`) instead.

**Proxy precedence:**
`mounts` > `proxy` > environment source (`.env` via `load_env_vars=True`, else OS environment if `trust_env=True`) > none.

- If you provide explicit `mounts`, these override all other proxy settings.
- If you set `proxy`, it overrides environment proxies but is overridden by `mounts`.
- If neither is set, and `load_env_vars=True`, proxy settings are loaded from `.env` via `combine_env_configs()`.
    - If both `.env` and OS environment have the same variable, OS environment takes precedence.
- If no explicit proxy or mounts are set but `trust_env=True`, HTTPX will use OS environment proxy settings (including `NO_PROXY`).

**Examples:**

*Using a single proxy:*
```python
from pyapiary.api_connectors.urlscan import URLScanConnector
conn = URLScanConnector(proxy="http://myproxy:8080")
```

*Using per-scheme mounts:*
```python
conn = URLScanConnector(mounts={"https://": "http://myproxy:8080", "http://": "http://myproxy2:8888"})
```

*Loading proxy from `.env`:*
```python
# .env file contains: HTTP_PROXY="http://myproxy:8080"
conn = URLScanConnector(load_env_vars=True)
# Uses HTTP_PROXY from .env even if not in OS environment.
```

**Note:** Any changes to proxy settings require re-instantiating the connector for changes to take effect.

### SSL Verification and Per-Request Options

You can now pass any `httpx.Client` keyword arguments (such as `verify=False`, `http2=True`) when instantiating a connector. These options will be applied to all requests made by that connector.

Additionally, per-request keyword arguments can be passed to methods like `.get()`, `.post()`, etc., and will be forwarded to `httpx.Client.request` for that single call.

Setting `verify=False` disables SSL verification and can be useful for testing against servers with self-signed certificates, but **should not be used in production** unless you understand the security implications.

**Examples:**

*Disable SSL verification at the connector level:*
```python
conn = URLScanConnector(verify=False)
response = conn.get("https://self-signed.badssl.com/")
print(response.status_code)
```

*Disable SSL verification for a single request:*
```python
conn = URLScanConnector()
response = conn.get("https://self-signed.badssl.com/", verify=False)
print(response.status_code)
```

*Enable HTTP/2:*
```python
conn = URLScanConnector(http2=True)
response = conn.get("https://nghttp2.org/httpbin/get")
print(response.http_version)
```

---

## 🗃️ DBMS Connectors

Each database connector follows a class-based pattern and supports reusable sessions, query helpers, and in some cases bulk helpers (e.g., `insert_many`, `bulk_insert`, etc.).

### MongoDB

Note: `query(...)` is deprecated in favor of `find(filter=..., projection=..., batch_size=...)`. The `query` method remains as a compatibility alias and logs a deprecation warning.

Sync connector
```python
from pyapiary.dbms_connectors.mongo import MongoConnector

# Recommended: use as a context manager (auto-closes)
with MongoConnector(
    "mongodb://localhost:27017",
    username="root",
    password="example",
    auth_retry_attempts=3,
    auth_retry_wait=1.0,
) as conn:
    # Clean up prior test docs
    conn.delete_many("mydb", "mycol", {"_sample": True})

    # Insert and upsert
    conn.insert_many("mydb", "mycol", [{"_id": 1, "foo": "bar", "_sample": True}])
    conn.upsert_many(
        "mydb",
        "mycol",
        [{"_id": 1, "foo": "baz", "_sample": True}, {"_id": 2, "foo": "qux", "_sample": True}],
        unique_key="_id",
    )

    # Find with projection and paging
    for doc in conn.find("mydb", "mycol", filter={"_sample": True}, projection={"_id": 1, "foo": 1}, batch_size=100):
        print(doc)

    # Distinct values
    vals = conn.distinct("mydb", "mycol", key="foo", filter={"_sample": True})
    print(vals)

# Manual lifecycle control is also supported
conn = MongoConnector("mongodb://localhost:27017")
try:
    list(conn.find("mydb", "mycol", filter={}))
finally:
    conn.close()
```

Async connector
```python
import asyncio
from pyapiary.dbms_connectors.mongo_async import AsyncMongoConnector

async def main():
    async with AsyncMongoConnector(
        "mongodb://localhost:27017",
        username="root",
        password="example",
        auth_retry_attempts=3,
        auth_retry_wait=1.0,
    ) as conn:
        await conn.delete_many("mydb", "mycol", {"_sample": True})
        await conn.insert_many("mydb", "mycol", [{"_id": 1, "foo": "bar", "_sample": True}])
        await conn.upsert_many(
            "mydb", "mycol",
            [{"_id": 1, "foo": "baz", "_sample": True}],
            unique_key="_id",
        )
        async for doc in conn.find("mydb", "mycol", filter={"_sample": True}, projection={"_id": 1, "foo": 1}):
            print(doc)
        vals = await conn.distinct("mydb", "mycol", key="foo", filter={"_sample": True})
        print(vals)

asyncio.run(main())
```

### Elasticsearch

```python
# The query method returns a generator; use list() or iterate to access results
from pyapiary.dbms_connectors.elasticsearch import ElasticsearchConnector

conn = ElasticsearchConnector(["http://localhost:9200"])
results = list(conn.query("my-index", {"query": {"match_all": {}}}))
for doc in results:
    print(doc)
```

### ODBC (e.g., Postgres, Teradata)

For automatic connection handling, use `ODBCConnector` as a context manager

```python
from pyapiary.dbms_connectors.odbc import ODBCConnector

with ODBCConnector("DSN=PostgresLocal;UID=postgres;PWD=postgres") as db:
   rows = conn.query("SELECT * FROM my_table")
   print(list(rows))
```

If you'd like to keep manual control, you can still use the `.close()` method

```python
from pyapiary.dbms_connectors.odbc import ODBCConnector

conn = ODBCConnector("DSN=PostgresLocal;UID=postgres;PWD=postgres")
rows = conn.query("SELECT * FROM my_table")
print(list(rows))
conn.close()
```

### Splunk

```python
from pyapiary.dbms_connectors.splunk import SplunkConnector

conn = SplunkConnector("localhost", 8089, "admin", "admin123", scheme="https", verify=False)
results = conn.query("search index=_internal | head 5")
```

---

## 🧪 Testing

### ✅ Unit tests

- Located in `tests/<connector_name>/test_unit_<connector>.py`
- Use mocking (`MagicMock`, `patch`) to avoid hitting external APIs
- Async connectors use `pytest-asyncio` and require tests to be decorated with `@pytest.mark.asyncio`

### 🔁 Integration tests

- Use [VCR.py](https://github.com/kevin1024/vcrpy) to record HTTP interactions
- Cassettes stored in: `tests/<connector_name>/cassettes/`
- Automatically redact secrets (API keys, tokens, etc.)
- Marked with `@pytest.mark.integration`

```bash
pytest -m integration
```

### 🧼 Suppress warnings

Add this to `pytest.ini`:

```ini
[pytest]
markers =
    integration: marks integration tests
```

---

## 🧑‍💻 Contributing / Adding a Connector

To add a new connector:

1. **Module**: Place your module in:
   - `src/pyapiary/api_connectors/` for API-based integrations
   - `src/pyapiary/dbms_connectors/` for database-style connectors

2. **Base class**:
   - Use the `Broker` class for APIs
   - Use the appropriate DBMS connector template for DBMSs

3. **Auth**: Pull secrets using `combine_env_configs()` to support `.env`, environment variables, and CI/CD injection.

4. **Testing**:
   - Add unit tests in: `tests/<name>/test_unit_<connector>.py`
   - Add integration tests in: `tests/<name>/test_integration_<connector>.py`
   - Save cassettes in: `tests/<name>/cassettes/`

5. **Docs**:
   - Add an example usage to this `README.md`
   - Document all methods with docstrings
   - Ensure your connector supports logging if `enable_logging=True` is passed

6. **Export**:
   - Optionally expose your connector via `__init__.py` for easier importing

---

## 🛠️ Dev Environment

```bash
git clone https://github.com/robd518/pyapiary.git
cd pyapiary

cp .env.example .env

python -m venv .venv
source .venv/bin/activate

poetry install  # if using poetry, or `pip install -e .[dev]`

pytest           # run all tests
black .          # format code
flake8 .         # linting
```

---

## 🔐 Secrets and Redaction

Sensitive values like API keys are redacted using the `AUTH_PARAM_REDACT` list in `conftest.py`. This ensures `.yaml` cassettes don’t leak credentials.

Redacted fields include:
- Query/body fields like `api_key`, `key`, `token`
- Header fields like `Authorization`, `X-API-Key`
- URI query parameters

---

## ✅ Summary

- Centralized request broker for all APIs
- Full support for both sync and async API connectors with consistent method signatures
- Robust DBMS connectors
- Easy-to-write unit and integration tests with automatic redaction
- Environment-agnostic configuration system
- VCR-powered CI-friendly test suite
