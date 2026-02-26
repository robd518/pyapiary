import httpx
import pytest
from unittest.mock import patch

from pyapiary.api_connectors.domaintools import DomainToolsConnector


def test_init_with_api_key():
    connector = DomainToolsConnector(api_key="test_key")
    assert connector.api_key == "test_key"
    assert connector.headers["X-API-KEY"] == "test_key"


def test_init_with_env_key():
    with patch.dict("os.environ", {"DOMAINTOOLS_API_KEY": "env_key"}, clear=True):
        connector = DomainToolsConnector(load_env_vars=True)
        assert connector.api_key == "env_key"
        assert connector.headers["X-API-KEY"] == "env_key"


def test_init_missing_key():
    with patch.dict("os.environ", {}, clear=True):
        with pytest.raises(ValueError, match="API key is required for DomainTools"):
            DomainToolsConnector()


@patch("pyapiary.api_connectors.domaintools.DomainToolsConnector.get")
def test_parsed_whois(mock_get):
    request = httpx.Request("GET", "https://api.domaintools.com/v1/example.com/whois/parsed")
    mock_get.return_value = httpx.Response(200, request=request, json={"ok": True})

    connector = DomainToolsConnector(api_key="test_key")
    result = connector.parsed_whois("example.com")

    mock_get.assert_called_once_with("v1/example.com/whois/parsed")
    assert isinstance(result, httpx.Response)
    assert result.json() == {"ok": True}


@patch("pyapiary.api_connectors.domaintools.DomainToolsConnector.get")
def test_reverse_ip(mock_get):
    request = httpx.Request("GET", "https://api.domaintools.com/v1/8.8.8.8/reverse-ip")
    mock_get.return_value = httpx.Response(200, request=request, json={"ok": True})

    connector = DomainToolsConnector(api_key="test_key")
    result = connector.reverse_ip("8.8.8.8", exclude_total_count=True)

    mock_get.assert_called_once_with("v1/8.8.8.8/reverse-ip", exclude_total_count=True)
    assert isinstance(result, httpx.Response)


@patch("pyapiary.api_connectors.domaintools.DomainToolsConnector.get")
def test_reverse_nameserver(mock_get):
    request = httpx.Request("GET", "https://api.domaintools.com/v1/ns1.example.com/name-server-domains")
    mock_get.return_value = httpx.Response(200, request=request, json={"ok": True})

    connector = DomainToolsConnector(api_key="test_key")
    result = connector.reverse_nameserver("ns1.example.com")

    mock_get.assert_called_once_with("v1/ns1.example.com/name-server-domains")
    assert isinstance(result, httpx.Response)


@patch("pyapiary.api_connectors.domaintools.DomainToolsConnector.get")
def test_iris_investigate_valid_params(mock_get):
    request = httpx.Request("GET", "https://api.domaintools.com/v1/iris-investigate")
    mock_get.return_value = httpx.Response(200, request=request, json={"records": []})

    connector = DomainToolsConnector(api_key="test_key")
    result = connector.iris_investigate(domain="domaintools.com", active="true")

    mock_get.assert_called_once_with(
        "/v1/iris-investigate",
        params={"domain": "domaintools.com", "active": "true"},
    )
    assert isinstance(result, httpx.Response)
    assert result.json() == {"records": []}


def test_iris_investigate_requires_at_least_one_param():
    connector = DomainToolsConnector(api_key="test_key")

    with pytest.raises(ValueError, match="At least one Iris Investigate parameter is required"):
        connector.iris_investigate()


def test_iris_investigate_rejects_invalid_param():
    connector = DomainToolsConnector(api_key="test_key")

    with pytest.raises(ValueError, match="Invalid Iris Investigate parameters"):
        connector.iris_investigate(not_a_real_param="value")
