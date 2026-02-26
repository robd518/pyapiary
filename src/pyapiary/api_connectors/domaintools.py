import httpx
from typing import Optional, Mapping, Any
from pyapiary.api_connectors.broker import Broker, AsyncBroker, bubble_broker_init_signature, log_method_call

IRIS_INVESTIGATE_ALLOWED_PARAMS = {
    'active',
    'adsense',
    'baidu_analytics',
    'contact_name',
    'contact_phone',
    'contact_street',
    'create_date',
    'domain',
    'email',
    'email_dns_soa',
    'email_domain',
    'expiration_date',
    'facebook',
    'first_seen_since',
    'first_seen_within',
    'google_analytics',
    'google_analytics_4',
    'google_tag_manager',
    'historical_email',
    'historical_free_text',
    'historical_registrant',
    'hotjar',
    'iana_id',
    'ip',
    'ip_country_code',
    'mailserver_domain',
    'mailserver_host',
    'mailserver_ip',
    'matomo',
    'nameserver_domain',
    'nameserver_host',
    'nameserver_ip',
    'not_tagged_with_all',
    'not_tagged_with_any',
    'rank',
    'redirect_domain',
    'registrant',
    'registrant_org',
    'registrar',
    'risk_score',
    'search_hash',
    'server_type',
    'ssl_alt_names',
    'ssl_common_name',
    'ssl_duration',
    'ssl_email',
    'ssl_hash',
    'ssl_issuer_common_name',
    'ssl_not_after',
    'ssl_not_before',
    'ssl_org',
    'ssl_subject',
    'statcounter_project',
    'statcounter_security',
    'tagged_with_all',
    'tagged_with_any',
    'tld',
    'website_title',
    'whois',
    'yandex_metrica'
}

def _validate_iris_investigate_params(params: Mapping[str, Any]) -> None:
    if not params:
        raise ValueError("At least one Iris Investigate parameter is required.")
    invalid = set(params) - IRIS_INVESTIGATE_ALLOWED_PARAMS
    if invalid:
        raise ValueError(f"Invalid Iris Investigate parameters: {sorted(invalid)}")



@bubble_broker_init_signature()
class DomainToolsConnector(Broker):
    def __init__(self, api_key: Optional[str] = None, **kwargs):
        super().__init__(base_url="https://api.domaintools.com", **kwargs)

        self.api_key = api_key or self.env_config.get("DOMAINTOOLS_API_KEY")
        if not self.api_key:
            raise ValueError("API key is required for DomainTools")
        self.headers.update({"X-API-KEY": self.api_key})

    @log_method_call
    def iris_investigate(self, **kwargs) -> httpx.Response:
        """Iris Investigate supports a set of base search parameters and filter
        parameters. Base search parameters can be used on their own or in
        combination with each other, while filter parameters refine the base search.
        Documentation can be found here. https://docs.domaintools.com/api/iris/investigate/search/

        Args:
            **kwargs: Instead of a domain name, you can provide one or more search
            fields to the API, such as IP address, SSL hash, email, or more, and
            Iris Investigate will return any domain name with a record that matches
            those parameters. This enables "reverse" searching on one or more fields
            with a single API endpoint.

        Returns:
            httpx.Response: The httpx response object.
        """
        _validate_iris_investigate_params(kwargs)
        return self.get("/v1/iris-investigate", params=kwargs)

    @log_method_call
    def parsed_whois(self, query: str, **kwargs) -> httpx.Response:
        """The Parsed WHOIS API provides parsed information extracted from the
        raw WHOIS record. The API is optimized to quickly retrieve the WHOIS
        record, group important data together and return a well-structured
        format. The Parsed WHOIS API is ideal for anyone wishing to search for,
        index, or cross-reference data from one or multiple WHOIS records.

        Args:
            query (str): A valid domain name or IP address.

            **kwargs: Additional keyword arguments to pass to the request as parameters

        Returns:
            httpx.Response: The httpx response object.
        """
        return self.get(f"v1/{query}/whois/parsed", **kwargs)

    @log_method_call
    def reverse_ip(self, query: str, **kwargs) -> httpx.Response:
        """The Reverse IP API provides a list of domain names that share the same
        Internet host (i.e. the same IP address). You can request an IP address
        directly, or you can provide a domain name; if you provide a domain name,
        the API will respond with the list of other domains that share the same IP.

        Args:
            query (str): A valid domain name.

           **kwargs: Additional keyword arguments to pass to the request as parameters

        Returns:
            httpx.Response: The httpx response object.
        """
        return self.get(f"v1/{query}/reverse-ip", **kwargs)

    @log_method_call
    def reverse_nameserver(self, query: str, **kwargs) -> httpx.Response:
        """The Reverse Name Server API provides a list of domain names that share
        the same primary or secondary name server. You can provide a domain name
        and the API will provide the list of domain names pointed to the same
        name servers as those listed as the primary and secondary name servers
        on the domain name you requested.

        Args:
            query (str): The name server hostname to query

            **kwargs: Additional keyword arguments to pass to the request as parameters

        Returns:
            httpx.Response: The httpx response object.
        """
        return self.get(f"v1/{query}/name-server-domains", **kwargs)


class AsyncDomainToolsConnector(AsyncBroker):
    def __init__(self, api_key: Optional[str] = None, **kwargs):
        super().__init__(base_url="https://api.domaintools.com", **kwargs)

        self.api_key = api_key or self.env_config.get("DOMAINTOOLS_API_KEY")
        if not self.api_key:
            raise ValueError("API key is required for DomainTools")
        self.headers.update({"X-API-KEY": self.api_key})

    @log_method_call
    async def iris_investigate(self, **kwargs) -> httpx.Response:
        """Iris Investigate supports a set of base search parameters and filter
        parameters. Base search parameters can be used on their own or in
        combination with each other, while filter parameters refine the base search.
        Documentation can be found here. https://docs.domaintools.com/api/iris/investigate/search/

        Args:
            **kwargs: Instead of a domain name, you can provide one or more search
            fields to the API, such as IP address, SSL hash, email, or more, and
            Iris Investigate will return any domain name with a record that matches
            those parameters. This enables "reverse" searching on one or more fields
            with a single API endpoint.

        Returns:
            httpx.Response: The httpx response object.
        """
        _validate_iris_investigate_params(kwargs)
        return await self.get("/v1/iris-investigate", params=kwargs)

    @log_method_call
    async def parsed_whois(self, query: str, **kwargs) -> httpx.Response:
        """The Parsed WHOIS API provides parsed information extracted from the
        raw WHOIS record. The API is optimized to quickly retrieve the WHOIS
        record, group important data together and return a well-structured
        format. The Parsed WHOIS API is ideal for anyone wishing to search for,
        index, or cross-reference data from one or multiple WHOIS records.

        Args:
            query (str): A valid domain name or IP address.

            **kwargs: Additional keyword arguments to pass to the request as parameters

        Returns:
            httpx.Response: The httpx response object.
        """
        return await self.get(f"v1/{query}/whois/parsed", **kwargs)

    @log_method_call
    async def reverse_ip(self, query: str, **kwargs) -> httpx.Response:
        """The Reverse IP API provides a list of domain names that share the same
        Internet host (i.e. the same IP address). You can request an IP address
        directly, or you can provide a domain name; if you provide a domain name,
        the API will respond with the list of other domains that share the same IP.

        Args:
            query (str): A valid domain name.

           **kwargs: Additional keyword arguments to pass to the request as parameters

        Returns:
            httpx.Response: The httpx response object.
        """
        return await self.get(f"v1/{query}/reverse-ip", **kwargs)

    @log_method_call
    async def reverse_nameserver(self, query: str, **kwargs) -> httpx.Response:
        """The Reverse Name Server API provides a list of domain names that share
        the same primary or secondary name server. You can provide a domain name
        and the API will provide the list of domain names pointed to the same
        name servers as those listed as the primary and secondary name servers
        on the domain name you requested.

        Args:
            query (str): The name server hostname to query

            **kwargs: Additional keyword arguments to pass to the request as parameters

        Returns:
            httpx.Response: The httpx response object.
        """
        return await self.get(f"v1/{query}/name-server-domains", **kwargs)