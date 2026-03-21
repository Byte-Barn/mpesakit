"""MpesaAsyncHttpClient: An asynchronous client for making HTTP requests to the M-Pesa API."""

import logging
import uuid
from typing import Dict, Any, Optional
import httpx

from mpesakit.errors import MpesaError, MpesaApiException
from .http_client import AsyncHttpClient
from tenacity import(
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_random_exponential,
    before_sleep_log,
)

from .mpesa_http_client import retry_enabled, handle_request_error, handle_retry_exception

logger = logging.getLogger(__name__)

mpesa_retry_condition = retry_if_exception_type(
    (httpx.ConnectError, httpx.ConnectTimeout,httpx.TimeoutException ,httpx.ReadTimeout)
)
class MpesaAsyncHttpClient(AsyncHttpClient):
    """An asynchronous client for making HTTP requests to the M-Pesa API.

    This client handles asynchronous GET and POST requests using the httpx library.
    It supports both sandbox and production environments.

    Attributes:
        base_url (str): The base URL for the M-Pesa API.
    """

    base_url: str
    _client: httpx.AsyncClient

    def __init__(self, env: str = "sandbox"):
        """Initializes the MpesaAsyncHttpClient with the specified environment."""
        self.base_url = self._resolve_base_url(env)
        self._client = httpx.AsyncClient(base_url=self.base_url)

    def _resolve_base_url(self, env: str) -> str:
        if env.lower() == "production":
            return "https://api.safaricom.co.ke"
        return "https://sandbox.safaricom.co.ke"


    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._client.aclose()


    async def _raw_post(
        self,
        url: str,
        json: Dict[str, Any],
        headers: Dict[str, str],
        timeout: int = 10,
    ) -> httpx.Response:
        """Low-level asynchronous POST request - may raise httpx exceptions."""
        return await self._client.post(
            url,
            json=json,
            headers=headers,
            timeout=timeout,
        )

    @retry(
        retry=mpesa_retry_condition,
        wait=wait_random_exponential(multiplier=5, max=8),
        stop=stop_after_attempt(3),
        retry_error_callback=handle_retry_exception,
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )
    async def _retryable_post(
    self,
    url: str,
    json: Dict[str, Any],
    headers: Dict[str, str],
    timeout: int = 10,
) -> httpx.Response:
     return await self._client.post(
        url,
        json=json,
        headers=headers,
        timeout=timeout,
    )

    async def post(
        self,
        url: str,
        json: Dict[str, Any],
        headers: Optional[Dict[str, str]] = None,
        timeout: int = 10,
        idempotent:bool = False,
    ) -> Dict[str, Any]:
        """Sends a asynchronous POST request to the M-Pesa API.

        Args:
            url (str): The URL path for the request.
            json (Dict[str, Any]): The JSON payload for the request body.
            headers (Dict[str, str]): The HTTP headers for the request.
            timeout (int): The timeout for the request in seconds.
            idempotent (bool): Add an idempotency key for safe retries.

        Returns:
            Dict[str, Any]: The JSON response from the API.
        """
        headers=dict(headers or {})

        if idempotent and "X-Idempotency-Key" not in headers:
            headers["X-Idempotency-Key"] = str(uuid.uuid4())

        response: httpx.Response | None = None

        try:
            if idempotent:
                response = await self._retryable_post(url, json, headers, timeout)
            else:
                response = await self._raw_post(url, json, headers, timeout)
            handle_request_error(response)
            return response.json()
        except httpx.ConnectTimeout as e:
            raise MpesaApiException(
                MpesaError(
                    error_code="REQUEST_TIMEOUT",
                    error_message=str(e)
                )
            ) from e

        except httpx.TimeoutException as e:
            raise MpesaApiException(
                MpesaError(
                    error_code="REQUEST_TIMEOUT",
                    error_message=str(e)
                )
            ) from e
        except httpx.ConnectError as e:
            raise MpesaApiException(
                MpesaError(
                    error_code="CONNECTION_ERROR",
                    error_message=str(e)
                )
            ) from e

        except (httpx.RequestError, ValueError) as e:
            raise MpesaApiException(
                MpesaError(
                    error_code="REQUEST_FAILED",
                    error_message=str(e),
                    status_code=getattr(response, "status_code", None),
                    raw_response=getattr(response, "text", None),
                )
            ) from e


    @retry(
        retry=retry_enabled(enabled=True),
        wait=wait_random_exponential(multiplier=5, max=8),
        stop=stop_after_attempt(3),
        retry_error_callback=handle_retry_exception,
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )
    async def _raw_get(
        self,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        timeout: int = 10,
    ) -> httpx.Response:
        """Low-level GET request - may raise httpx exceptions."""
        if headers is None:
            headers = {}

        return await self._client.get(
            url,
            params=params,
            headers=headers,
            timeout=timeout,
        )

    async def get(
        self,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        timeout: int = 10,
    ) -> Dict[str, Any]:
        """Sends a GET request to the M-Pesa API.

        Args:
            url (str): The URL path for the request.
            params (Optional[Dict[str, Any]]): The URL parameters.
            headers (Optional[Dict[str, str]]): The HTTP headers.
            timeout (int): The timeout for the request in seconds.

        Returns:
            Dict[str, Any]: The JSON response from the API.
        """
        response: httpx.Response | None = None

        try:
            response = await self._raw_get(url, params, headers, timeout)
            handle_request_error(response)
            return response.json()

        except (httpx.RequestError, ValueError) as e:
            raise MpesaApiException(
                MpesaError(
                    error_code="REQUEST_FAILED",
                    error_message=str(e),
                    status_code=getattr(response, "status_code", None),
                    raw_response=getattr(response, "text", None),
                )
            ) from e

    async def aclose(self):
        """Manually close the underlying httpx client connection pool."""
        await self._client.aclose()
