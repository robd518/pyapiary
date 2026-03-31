import httpx
import pytest
from pyapiary.api_connectors.ipqs import IPQSConnector

@pytest.mark.integration
def test_ipqs_malicious_url_vcr(vcr_cassette):
    with vcr_cassette.use_cassette("test_ipqs_malicious_url_vcr"):
        connector = IPQSConnector(load_env_vars=True, enable_logging=True)
        result = connector.malicious_url("github.com")

        assert isinstance(result, httpx.Response)
        assert "domain" in result.json()
        assert result.json()["domain"] == "github.com"


@pytest.mark.integration
def test_ipqs_phone_validation_vcr(vcr_cassette):
    with vcr_cassette.use_cassette("test_ipqs_phone_validation_vcr"):
        connector = IPQSConnector(load_env_vars=True, enable_logging=True)
        result = connector.phone_validation("+12024567041")

        assert isinstance(result, httpx.Response)
        assert "formatted" in result.json()
        assert result.json()["formatted"] == "+12024567041"
