import os

import httpx
import pytest

from pyapiary.api_connectors.domaintools import DomainToolsConnector
from pyapiary.helpers import combine_env_configs


def _require_domaintools_key():
    config = combine_env_configs()
    if not (os.getenv("DOMAINTOOLS_API_KEY") or config.get("DOMAINTOOLS_API_KEY")):
        pytest.skip("DOMAINTOOLS_API_KEY is required for DomainTools integration tests")


@pytest.mark.integration
def test_domaintools_parsed_whois_vcr(vcr_cassette):
    _require_domaintools_key()

    with vcr_cassette.use_cassette("test_domaintools_parsed_whois_vcr"):
        connector = DomainToolsConnector(load_env_vars=True, enable_logging=True)
        try:
            result = connector.parsed_whois("domaintools.com")
        except httpx.ConnectError as exc:
            pytest.skip(f"Network/DNS unavailable for DomainTools cassette recording: {exc}")

        assert isinstance(result, httpx.Response)
        assert result.status_code == 200
        assert isinstance(result.json(), dict)


@pytest.mark.integration
def test_domaintools_iris_investigate_vcr(vcr_cassette):
    _require_domaintools_key()

    with vcr_cassette.use_cassette("test_domaintools_iris_investigate_vcr"):
        connector = DomainToolsConnector(load_env_vars=True, enable_logging=True)
        try:
            result = connector.iris_investigate(domain="domaintools.com")
        except httpx.ConnectError as exc:
            pytest.skip(f"Network/DNS unavailable for DomainTools cassette recording: {exc}")

        assert isinstance(result, httpx.Response)
        assert result.status_code == 200
        assert isinstance(result.json(), dict)
