import pytest
import httpx
from unittest.mock import patch, AsyncMock
from pyapiary.api_connectors.ipqs import AsyncIPQSConnector


@pytest.mark.asyncio
async def test_async_init_with_api_key():
    connector = AsyncIPQSConnector(api_key="test_key")
    assert connector.api_key == "test_key"
    assert connector.headers["Content-Type"] == "application/x-www-form-urlencoded"


@pytest.mark.asyncio
async def test_async_init_with_env_key():
    with patch.dict("os.environ", {"IPQS_API_KEY": "env_key"}):
        connector = AsyncIPQSConnector(load_env_vars=True)
        assert connector.api_key == "env_key"
        assert connector.headers["Content-Type"] == "application/x-www-form-urlencoded"


@pytest.mark.asyncio
async def test_async_init_missing_key():
    with patch.dict("os.environ", {}, clear=True):
        with pytest.raises(ValueError, match="API key is required"):
            AsyncIPQSConnector()


@patch("pyapiary.api_connectors.ipqs.AsyncIPQSConnector.post", new_callable=AsyncMock)
@pytest.mark.asyncio
async def test_async_malicious_url(mock_post):
    import json

    request = httpx.Request("POST", "https://ipqualityscore.com/api/json/url/")
    payload = {"success": True, "domain": "example.com"}
    mock_response = httpx.Response(
        200,
        request=request,
        content=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
    )
    mock_post.return_value = mock_response

    connector = AsyncIPQSConnector(api_key="test_key")
    response = await connector.malicious_url("example.com", strictness=1)

    assert isinstance(response, httpx.Response)
    assert response.status_code == 200
    assert response.json() == payload
    mock_post.assert_awaited_once_with(
        "/url/",
        data={"url": "example.com", "key": "test_key", "strictness": 1}
    )
