import httpx
from typing import Optional
from pyapiary.api_connectors.broker import Broker, AsyncBroker, bubble_broker_init_signature, log_method_call

@bubble_broker_init_signature()
class IPQSConnector(Broker):
    """
    A connector for the IPQualityScore Malicious URL Scanner API.

    This class provides a typed interface to interact with IPQS's malicious URL
    scan endpoint. It handles API key management, header setup, and request routing
    through the shared Broker infrastructure.

    Attributes:
        api_key (str): The API key used to authenticate with IPQS.
    """
    def __init__(self, api_key: Optional[str] = None, **kwargs):
        super().__init__(base_url="https://ipqualityscore.com/api/json", **kwargs)

        self.api_key = api_key or self.env_config.get("IPQS_API_KEY")
        if not self.api_key:
            raise ValueError("API key is required for IPQSConnector")
        self.headers.update({"Content-Type": "application/x-www-form-urlencoded"})

    @log_method_call
    def malicious_url(self, query: str, **kwargs) -> httpx.Response:
        """
        Scan a URL using IPQualityScore's Malicious URL Scanner API.

        Args:
            query (str): The URL to scan.
            **kwargs: Optional parameters like 'strictness' or 'fast' to influence scan behavior.

        Returns:
            httpx.Response: the httpx.Response object
        """
        return self.post("/url/", data={"url": query, "key": self.api_key, **kwargs})


@bubble_broker_init_signature()
class AsyncIPQSConnector(AsyncBroker):
    """
    Async version of IPQSConnector using AsyncBroker infrastructure.
    """
    def __init__(self, api_key: Optional[str] = None, **kwargs):
        super().__init__(base_url="https://ipqualityscore.com/api/json", **kwargs)

        self.api_key = api_key or self.env_config.get("IPQS_API_KEY")
        if not self.api_key:
            raise ValueError("API key is required for AsyncIPQSConnector")
        self.headers.update({"Content-Type": "application/x-www-form-urlencoded"})

    @log_method_call
    async def malicious_url(self, query: str, **kwargs) -> httpx.Response:
        """
        Asynchronously scan a URL using IPQualityScore's Malicious URL Scanner API.

        Args:
            query (str): The URL to scan.
            **kwargs: Optional parameters like 'strictness' or 'fast' to influence scan behavior.

        Returns:
            httpx.Response: the httpx.Response object
        """
        return await self.post("/url/", data={"url": query, "key": self.api_key, **kwargs})
