"""MpesaAsyncHttpClient: An asynchronous client for making HTTP requests to the M-Pesa API."""

import logging
from typing import Dict, Any, Optional

import httpx
from tenacity import before_sleep_log, retry, wait_random_exponential

from mpesakit.errors import MpesaError, MpesaApiException
from ._retry import (
    DEFAULT_MAX_RETRIES,
    handle_retry_exception,
    retry_enabled,
    stop_after_instance_max_retries,
)
from .http_client import AsyncHttpClient

logger = logging.getLogger(__name__)


class MpesaAsyncHttpClient(AsyncHttpClient):
    """An asynchronous client for making HTTP requests to the M-Pesa API.

    This client handles asynchronous GET and POST requests using the httpx library.
    It supports both sandbox and production environments.

    Attributes:
        base_url (str): The base URL for the M-Pesa API.
    """

    base_url: str
    max_retries: int
    _client: httpx.AsyncClient

    def __init__(
        self,
        env: str = "sandbox",
        trust_env: bool = True,
        max_retries: int = DEFAULT_MAX_RETRIES,
    ):
        """Initializes the MpesaAsyncHttpClient with the specified environment.

        Args:
            env (str): The environment to connect to ('sandbox' or 'production').
            trust_env (bool): Whether to trust environment proxy/CA settings.
            max_retries (int): Number of attempts made for a request (including
                the first try) before giving up on transient network errors.
        """
        self.base_url = self._resolve_base_url(env)
        self.max_retries = max_retries
        self._client = httpx.AsyncClient(base_url=self.base_url, trust_env=trust_env)

    def _resolve_base_url(self, env: str) -> str:
        if env.lower() == "production":
            return "https://api.safaricom.co.ke"
        return "https://sandbox.safaricom.co.ke"

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._client.aclose()

    @retry(
        retry=retry_enabled(enabled=True),
        wait=wait_random_exponential(multiplier=5, max=8),
        stop=stop_after_instance_max_retries,
        retry_error_callback=handle_retry_exception,
        before_sleep=before_sleep_log(logger, logging.WARNING),
    )
    async def _raw_post(
        self,
        url: str,
        json: Dict[str, Any],
        headers: Dict[str, str],
        timeout: int = 10,
    ) -> httpx.Response:
        """Low-level async POST request - may raise httpx exceptions."""
        return await self._client.post(url, json=json, headers=headers, timeout=timeout)

    async def post(
        self, url: str, json: Dict[str, Any], headers: Dict[str, str]
    ) -> Dict[str, Any]:
        """Sends an asynchronous POST request to the M-Pesa API."""
        try:
            response = await self._raw_post(url, json, headers)

            try:
                response_data = response.json()
            except ValueError:
                response_data = {"errorMessage": response.text.strip() or ""}

            if not response.is_success:
                error_message = response_data.get("errorMessage", "")
                raise MpesaApiException(
                    MpesaError(
                        error_code=f"HTTP_{response.status_code}",
                        error_message=error_message,
                        status_code=response.status_code,
                        raw_response=response_data,
                    )
                )

            return response_data

        except httpx.HTTPError as e:
            raise MpesaApiException(
                MpesaError(
                    error_code="REQUEST_FAILED",
                    error_message=f"HTTP request failed: {str(e)}",
                    status_code=None,
                    raw_response=None,
                )
            ) from e

    @retry(
        retry=retry_enabled(enabled=True),
        wait=wait_random_exponential(multiplier=5, max=8),
        stop=stop_after_instance_max_retries,
        retry_error_callback=handle_retry_exception,
        before_sleep=before_sleep_log(logger, logging.WARNING),
    )
    async def _raw_get(
        self,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        timeout: int = 10,
    ) -> httpx.Response:
        """Low-level async GET request - may raise httpx exceptions."""
        if headers is None:
            headers = {}
        return await self._client.get(url, params=params, headers=headers, timeout=timeout)

    async def get(
        self,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """Sends an asynchronous GET request to the M-Pesa API."""
        try:
            response = await self._raw_get(url, params, headers)

            try:
                response_data = response.json()
            except ValueError:
                response_data = {"errorMessage": response.text.strip() or ""}

            if not response.is_success:
                error_message = response_data.get("errorMessage", "")
                raise MpesaApiException(
                    MpesaError(
                        error_code=f"HTTP_{response.status_code}",
                        error_message=error_message,
                        status_code=response.status_code,
                        raw_response=response_data,
                    )
                )

            return response_data

        except httpx.HTTPError as e:
            raise MpesaApiException(
                MpesaError(
                    error_code="REQUEST_FAILED",
                    error_message=f"HTTP request failed: {str(e)}",
                    status_code=None,
                    raw_response=None,
                )
            ) from e

    async def aclose(self):
        """Manually close the underlying httpx client connection pool."""
        await self._client.aclose()
