import httpx
import pytest
from unittest.mock import AsyncMock, patch

from pyapiary.api_connectors.domaintools import AsyncDomainToolsConnector


@pytest.mark.asyncio
async def test_async_init_with_api_key():
    connector = AsyncDomainToolsConnector(api_key="test_key")
    assert connector.api_key == "test_key"
    assert connector.headers["X-API-KEY"] == "test_key"
    await connector.session.aclose()


@pytest.mark.asyncio
async def test_async_init_with_env_key():
    with patch.dict("os.environ", {"DOMAINTOOLS_API_KEY": "env_key"}, clear=True):
        connector = AsyncDomainToolsConnector(load_env_vars=True)
        assert connector.api_key == "env_key"
        assert connector.headers["X-API-KEY"] == "env_key"
        await connector.session.aclose()


@pytest.mark.asyncio
async def test_async_init_missing_key():
    with patch.dict("os.environ", {}, clear=True):
        with pytest.raises(ValueError, match="API key is required for DomainTools"):
            AsyncDomainToolsConnector()


@patch("pyapiary.api_connectors.domaintools.AsyncDomainToolsConnector.get", new_callable=AsyncMock)
@pytest.mark.asyncio
async def test_async_parsed_whois(mock_get):
    request = httpx.Request("GET", "https://api.domaintools.com/v1/example.com/whois/parsed")
    mock_get.return_value = httpx.Response(200, request=request, json={"ok": True})

    connector = AsyncDomainToolsConnector(api_key="test_key")
    result = await connector.parsed_whois("example.com")

    mock_get.assert_awaited_once_with("v1/example.com/whois/parsed")
    assert isinstance(result, httpx.Response)
    await connector.session.aclose()


@patch("pyapiary.api_connectors.domaintools.AsyncDomainToolsConnector.get", new_callable=AsyncMock)
@pytest.mark.asyncio
async def test_async_iris_investigate_valid_params(mock_get):
    request = httpx.Request("GET", "https://api.domaintools.com/v1/iris-investigate")
    mock_get.return_value = httpx.Response(200, request=request, json={"records": []})

    connector = AsyncDomainToolsConnector(api_key="test_key")
    result = await connector.iris_investigate(domain="domaintools.com", active="true")

    mock_get.assert_awaited_once_with(
        "/v1/iris-investigate",
        params={"domain": "domaintools.com", "active": "true"},
    )
    assert isinstance(result, httpx.Response)
    await connector.session.aclose()


@pytest.mark.asyncio
async def test_async_iris_investigate_requires_at_least_one_param():
    connector = AsyncDomainToolsConnector(api_key="test_key")

    with pytest.raises(ValueError, match="At least one Iris Investigate parameter is required"):
        await connector.iris_investigate()

    await connector.session.aclose()


@pytest.mark.asyncio
async def test_async_iris_investigate_rejects_invalid_param():
    connector = AsyncDomainToolsConnector(api_key="test_key")

    with pytest.raises(ValueError, match="Invalid Iris Investigate parameters"):
        await connector.iris_investigate(not_a_real_param="value")

    await connector.session.aclose()
