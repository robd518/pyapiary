import httpx
import pytest
from unittest.mock import patch
from pyapiary.api_connectors.ipqs import IPQSConnector


def test_init_with_api_key():
    connector = IPQSConnector(api_key="test_key")
    assert connector.api_key == "test_key"
    assert connector.headers["Content-Type"] == "application/x-www-form-urlencoded"


def test_init_with_env_key():
    with patch.dict("os.environ", {"IPQS_API_KEY": "env_key"}):
        connector = IPQSConnector(load_env_vars=True)
        assert connector.api_key == "env_key"


def test_init_missing_key():
    with patch.dict("os.environ", {}, clear=True):
        with pytest.raises(ValueError, match="API key is required"):
            IPQSConnector()


@patch("pyapiary.api_connectors.ipqs.IPQSConnector.post")
def test_malicious_url(mock_post):
    # Build a real httpx.Response to match the new return type
    import json

    request = httpx.Request("POST", "https://www.ipqualityscore.com/api/json/url/")
    payload = {"success": True, "domain": "example.com"}
    mock_response = httpx.Response(
        200,
        request=request,
        content=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
    )
    mock_post.return_value = mock_response

    connector = IPQSConnector(api_key="test_key")
    result = connector.malicious_url("example.com")

    mock_post.assert_called_once_with(
        "/url/",
        data={"url": "example.com", "key": "test_key"},
    )
    assert isinstance(result, httpx.Response)
    assert result.json() == payload
