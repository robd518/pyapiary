import httpx
from httpx import Auth
from typing import Optional, Dict, Any, Union, Iterable, Callable, ParamSpec, TypeVar, Type
from tenacity import retry, stop_after_attempt, wait_exponential, RetryError, retry_if_exception, AsyncRetrying
from pyapiary.helpers import setup_logger, combine_env_configs
from functools import wraps
import inspect
import os
from types import TracebackType


P = ParamSpec("P")
R = TypeVar("R")

def log_method_call(func: Callable[P, R]) -> Callable[P, R]:
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        caller = func.__name__
        sig = inspect.signature(func)
        bound = sig.bind(self, *args, **kwargs)
        bound.apply_defaults()
        call_args = {k: v for k, v in bound.arguments.items() if k != "self"}
        query_value = call_args.get("query")

        if query_value is not None:
            self._log(f"{caller} called with query: {query_value}")
        elif call_args:
            # Fall back to a compact arg summary for methods that don't use a
            # positional/keyword `query` parameter (e.g., kwargs-only search APIs).
            summary_parts = []
            for key, value in call_args.items():
                if key == "kwargs" and isinstance(value, dict):
                    summary_parts.append(f"kwargs_keys={sorted(value.keys())}")
                elif key == "params" and isinstance(value, dict):
                    summary_parts.append(f"params_keys={sorted(value.keys())}")
                else:
                    summary_parts.append(f"{key}={value!r}")
            self._log(f"{caller} called with {', '.join(summary_parts)}")
        else:
            self._log(f"{caller} called")
        return func(self, *args, **kwargs)
    return wrapper


def bubble_broker_init_signature(*, exclude: Iterable[str] = ("base_url",)):
    """
    Class decorator that augments a connector subclass' __init__ signature with
    parameters from Broker.__init__ for better IDE/tab-completion hints.

    Usage:
        from pyapiary.api_connectors.broker import Broker, bubble_broker_init_signature

        @bubble_broker_init_signature()
        class MyConnector(Broker):
            def __init__(self, api_key: str | None = None, **kwargs):
                super().__init__(base_url="https://example.com", **kwargs)
                ...

    Notes:
        - This affects *introspection only* (via __signature__). Runtime behavior is unchanged.
        - Subclass-specific parameters remain first (e.g., api_key), followed by Broker params.
        - `base_url` is excluded by default since subclasses set it themselves.
        - The subclass' **kwargs (if present) is preserved at the end so httpx.Client kwargs
          can still be passed through.
    """
    def _decorate(cls):
        sub_init = cls.__init__
        broker_init = Broker.__init__

        sub_sig = inspect.signature(sub_init)
        broker_sig = inspect.signature(broker_init)

        new_params = []
        saw_var_kw = None

        # Keep subclass params first; remember its **kwargs if present
        for p in sub_sig.parameters.values():
            if p.kind is inspect.Parameter.VAR_KEYWORD:
                saw_var_kw = p
            else:
                new_params.append(p)

        present = {p.name for p in new_params}

        # Append Broker params (skip self, excluded, already-present, and **kwargs)
        for name, p in list(broker_sig.parameters.items())[1:]:
            if name in exclude or name in present:
                continue
            if p.kind is inspect.Parameter.VAR_KEYWORD:
                continue
            new_params.append(p)

        # Re-append subclass **kwargs (or add a generic one to keep flexibility)
        if saw_var_kw is not None:
            new_params.append(saw_var_kw)
        else:
            new_params.append(
                inspect.Parameter(
                    "client_kwargs",
                    kind=inspect.Parameter.VAR_KEYWORD,
                )
            )

        cls.__init__.__signature__ = inspect.Signature(parameters=new_params)
        return cls

    return _decorate


class SharedConnectorBase:
    """
    Shared base class for Broker and AsyncBroker.
    Houses reusable logic (constructor, logging, proxy config, retry predicate).
    """
    def __init__(
        self,
        base_url: str,
        headers: Optional[Dict[str, str]] = None,
        enable_logging: bool = False,
        enable_backoff: bool = False,
        timeout: int = 10,
        load_env_vars: bool = False,
        trust_env: bool = True,
        proxy: Optional[str] = None,
        mounts: Optional[Dict[str, httpx.HTTPTransport]] = None,
        **client_kwargs,
    ):
        self.base_url = base_url.rstrip('/')
        self.logger = setup_logger(self.__class__.__name__) if enable_logging else None
        self.enable_backoff = enable_backoff
        self.timeout = timeout
        self.headers = headers or {}
        self.trust_env = trust_env
        self.proxy = proxy
        self.mounts = mounts
        self.env_config = combine_env_configs() if load_env_vars else {}
        self._client_kwargs = dict(client_kwargs) if client_kwargs else {}

    def _log(self, message: str):
        if self.logger:
            self.logger.info(message)

    def _collect_proxy_config(self) -> tuple[Optional[str], Optional[Dict[str, httpx.HTTPTransport]]]:
        source_env: Optional[Dict[str, str]] = None
        if isinstance(self.env_config, dict) and len(self.env_config) > 0:
            source_env = {k: v for k, v in self.env_config.items() if isinstance(k, str) and isinstance(v, str)}
        elif self.trust_env:
            source_env = dict(os.environ)
        else:
            return None, None

        def _get(key: str) -> Optional[str]:
            return source_env.get(key) or source_env.get(key.lower())

        all_proxy = _get("ALL_PROXY")
        http_proxy = _get("HTTP_PROXY")
        https_proxy = _get("HTTPS_PROXY")

        if http_proxy and https_proxy and http_proxy != https_proxy:
            return None, {
                "http://": httpx.HTTPTransport(proxy=http_proxy),
                "https://": httpx.HTTPTransport(proxy=https_proxy),
            }
        single = all_proxy or https_proxy or http_proxy
        if single:
            return single, None
        return None, None

    @staticmethod
    def _default_retry_exc(exc: BaseException) -> bool:
        if isinstance(exc, httpx.HTTPStatusError):
            r = exc.response
            if r is not None:
                return r.status_code == 429 or 500 <= r.status_code < 600
        return isinstance(exc, (
            httpx.ConnectError,
            httpx.ReadTimeout,
            httpx.WriteError,
            httpx.RemoteProtocolError,
            httpx.PoolTimeout,
        ))


class Broker(SharedConnectorBase):
    """
    A base HTTP client that provides structured request handling, logging, retries, and optional environment config loading.
    Designed to be inherited by specific API connector classes.
    """
    def __init__(
        self,
        base_url: str,
        headers: Optional[Dict[str, str]] = None,
        enable_logging: bool = False,
        enable_backoff: bool = False,
        timeout: int = 10,
        load_env_vars: bool = False,
        trust_env: bool = True,
        proxy: Optional[str] = None,
        mounts: Optional[Dict[str, httpx.HTTPTransport]] = None,
        **client_kwargs,
    ):
        super().__init__(
            base_url=base_url,
            headers=headers,
            enable_logging=enable_logging,
            enable_backoff=enable_backoff,
            timeout=timeout,
            load_env_vars=load_env_vars,
            trust_env=trust_env,
            proxy=proxy,
            mounts=mounts,
            **client_kwargs,
        )

        client_options = dict(self._client_kwargs)
        client_options.pop("timeout", None)

        client_args = {
            "timeout": self.timeout,
            "trust_env": self.trust_env,
            **client_options,
        }

        if self.mounts:
            client_args["mounts"] = self.mounts
        elif self.proxy:
            client_args["proxy"] = self.proxy
        else:
            env_proxy, env_mounts = self._collect_proxy_config()
            if env_mounts:
                self.mounts = env_mounts
                client_args["mounts"] = self.mounts
            elif env_proxy:
                self.proxy = env_proxy
                client_args["proxy"] = self.proxy
            elif not self.trust_env:
                client_args["trust_env"] = False

        self.session = httpx.Client(**client_args)

    def __enter__(self) -> "Broker":
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[TracebackType],
    ) -> None:
        self.session.close()

    def _make_request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        json: Optional[Dict[str, Any]] = None,
        auth: Optional[Union[tuple, Auth]] = None,
        headers: Optional[Dict[str, str]] = None,
        retry_kwargs: Optional[Dict[str, Any]] = None,
        **request_kwargs,
    ) -> httpx.Response:
        url = f"{self.base_url}/{endpoint.lstrip('/')}"

        def do_request() -> httpx.Response:
            resp = self.session.request(
                method=method,
                url=url,
                headers=headers or self.headers,
                params=params,
                json=json,
                auth=auth,
                **request_kwargs,
            )
            resp.raise_for_status()
            return resp

        call = do_request
        if self.enable_backoff:
            rk = dict(retry_kwargs or {})
            if "retry" not in rk:
                rk["retry"] = retry_if_exception(self._default_retry_exc)
            if "stop" not in rk:
                rk["stop"] = stop_after_attempt(3)
            if "wait" not in rk:
                rk["wait"] = wait_exponential(multiplier=1, min=2, max=10)
            call = retry(reraise=True, **rk)(do_request)

        try:
            return call()
        except RetryError as re:
            last = re.last_attempt.exception()
            self._log(f"Retry failed: {last}")
            raise
        except httpx.HTTPStatusError as he:
            self._log(f"HTTP error: {he}")
            raise

    def get(self, endpoint: str, params: Optional[Dict[str, Any]] = None, **kwargs) -> httpx.Response:
        return self._make_request("GET", endpoint, params=params, **kwargs)

    def post(self, endpoint: str, json: Optional[Dict[str, Any]] = None, **kwargs) -> httpx.Response:
        return self._make_request("POST", endpoint, json=json, **kwargs)


class AsyncBroker(SharedConnectorBase):
    """
    Async HTTP client connector. Provides async _make_request, get, and post.
    """
    def __init__(
        self,
        base_url: str,
        headers: Optional[Dict[str, str]] = None,
        enable_logging: bool = False,
        enable_backoff: bool = False,
        timeout: int = 10,
        load_env_vars: bool = False,
        trust_env: bool = True,
        proxy: Optional[str] = None,
        mounts: Optional[Dict[str, httpx.HTTPTransport]] = None,
        **client_kwargs,
    ):
        super().__init__(
            base_url=base_url,
            headers=headers,
            enable_logging=enable_logging,
            enable_backoff=enable_backoff,
            timeout=timeout,
            load_env_vars=load_env_vars,
            trust_env=trust_env,
            proxy=proxy,
            mounts=mounts,
            **client_kwargs,
        )

        if self.mounts:
            raise ValueError("The 'mounts' parameter is not supported in AsyncBroker but "
                             "you can still use 'proxy' or 'trust_env' if 'HTTP_PROXY' or "
                             "'HTTPS_PROXY' are in your system environment variables ")

        resolved_proxy = self.proxy or self._collect_proxy_config()[0]

        self.session = httpx.AsyncClient(
            timeout=self.timeout,
            proxy=resolved_proxy,
            trust_env=self.trust_env,
            **self._client_kwargs,
        )

    async def __aenter__(self) -> "AsyncBroker":
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[TracebackType],
    ) -> None:
        await self.session.aclose()

    async def _make_request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        json: Optional[Dict[str, Any]] = None,
        auth: Optional[Union[tuple, Auth]] = None,
        headers: Optional[Dict[str, str]] = None,
        retry_kwargs: Optional[Dict[str, Any]] = None,
        **request_kwargs,
    ) -> httpx.Response:
        url = f"{self.base_url}/{endpoint.lstrip('/')}"

        async def do_request() -> httpx.Response:
            resp = await self.session.request(
                method=method,
                url=url,
                headers=headers or self.headers,
                params=params,
                json=json,
                auth=auth,
                **request_kwargs,
            )
            resp.raise_for_status()
            return resp

        call = do_request
        if self.enable_backoff:

            rk = dict(retry_kwargs or {})
            retry_pred = rk.get("retry", retry_if_exception(self._default_retry_exc))
            stop_cond = rk.get("stop", stop_after_attempt(3))
            wait_cond = rk.get("wait", wait_exponential(multiplier=1, min=2, max=10))

            async def retry_wrapper():
                async for attempt in AsyncRetrying(reraise=True, retry=retry_pred, stop=stop_cond, wait=wait_cond):
                    with attempt:
                        return await do_request()
            call = retry_wrapper

        try:
            return await call()
        except RetryError as re:
            last = re.last_attempt.exception()
            self._log(f"Retry failed: {last}")
            raise
        except httpx.HTTPStatusError as he:
            self._log(f"HTTP error: {he}")
            raise

    async def get(self, endpoint: str, params: Optional[Dict[str, Any]] = None, **kwargs) -> httpx.Response:
        return await self._make_request("GET", endpoint, params=params, **kwargs)

    async def post(self, endpoint: str, json: Optional[Dict[str, Any]] = None, **kwargs) -> httpx.Response:
        return await self._make_request("POST", endpoint, json=json, **kwargs)
