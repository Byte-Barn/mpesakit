"""MpesaHttpClient: A client for making HTTP requests to the M-Pesa API.

Handles GET and POST requests with error handling for common HTTP issues.
"""

import logging
from typing import Any, Dict, Optional
from urllib.parse import urljoin

import httpx
from tenacity import before_sleep_log, retry, wait_random_exponential

from mpesakit.errors import MpesaApiException, MpesaError

from ._retry import (
    DEFAULT_MAX_RETRIES,
    handle_retry_exception,
    retry_enabled,
    stop_after_instance_max_retries,
)
from .http_client import HttpClient

logger = logging.getLogger(__name__)


def handle_request_error(response: httpx.Response):
    """Handles non-successful HTTP responses.

    This function is now responsible for converting HTTP status codes
    and JSON parsing errors into MpesaApiException.
    """
    if response.is_success:
        return
    try:
        response_data = response.json()
    except ValueError:
        response_data = {"errorMessage": response.text.strip() or ""}

    error_message = response_data.get("errorMessage", "")
    raise MpesaApiException(
        MpesaError(
            error_code=f"HTTP_{response.status_code}",
            error_message=error_message,
            status_code=response.status_code,
            raw_response=response_data,
        )
    )


class MpesaHttpClient(HttpClient):
    """A client for making HTTP requests to the M-Pesa API."""

    base_url: str
    max_retries: int
    _client: Optional[httpx.Client] = None

    def __init__(
        self,
        env: str = "sandbox",
        use_session: bool = False,
        trust_env: bool = True,
        max_retries: int = DEFAULT_MAX_RETRIES,
    ):
        """Initializes the MpesaHttpClient instance.

        Args:
            env (str): The environment to connect to ('sandbox' or 'production').
            use_session (bool): Whether to use a persistent client.
            trust_env (bool): Whether to trust environment proxy/CA settings.
            max_retries (int): Number of attempts made for a request (including
                the first try) before giving up on transient network errors.
        """
        self.base_url = self._resolve_base_url(env)
        self.max_retries = max_retries
        if use_session:
            self._client = httpx.Client(trust_env=trust_env)

    def _resolve_base_url(self, env: str) -> str:
        if env.lower() == "production":
            return "https://api.safaricom.co.ke"
        return "https://sandbox.safaricom.co.ke"

    @retry(
        retry=retry_enabled(enabled=True),
        wait=wait_random_exponential(multiplier=5, max=8),
        stop=stop_after_instance_max_retries,
        retry_error_callback=handle_retry_exception,
        before_sleep=before_sleep_log(logger, logging.WARNING),
    )
    def _raw_post(
        self, url: str, json: Dict[str, Any], headers: Dict[str, str], timeout: int = 10
    ) -> httpx.Response:
        """Low-level POST request - may raise httpx exceptions."""
        full_url = urljoin(self.base_url, url)
        if self._client:
            return self._client.post(
                full_url, json=json, headers=headers, timeout=timeout
            )
        else:
            with httpx.Client() as client:
                return client.post(
                    full_url, json=json, headers=headers, timeout=timeout
                )

    def post(
        self, url: str, json: Dict[str, Any], headers: Dict[str, str], timeout: int = 10
    ) -> Dict[str, Any]:
        """Sends a POST request to the M-Pesa API.

        Args:
            url (str): The URL path for the request.
            json (Dict[str, Any]): The JSON payload for the request body.
            headers (Dict[str, str]): The HTTP headers for the request.
            timeout (int): The timeout for the request in seconds.

        Returns:
            Dict[str, Any]: The JSON response from the API.
        """
        response: httpx.Response | None = None
        try:
            response = self._raw_post(url, json, headers, timeout)
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

    @retry(
        retry=retry_enabled(enabled=True),
        wait=wait_random_exponential(multiplier=5, max=8),
        stop=stop_after_instance_max_retries,
        retry_error_callback=handle_retry_exception,
        before_sleep=before_sleep_log(logger, logging.WARNING),
    )
    def _raw_get(
        self,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        timeout: int = 10,
    ) -> httpx.Response:
        """Low-level GET request - may raise httpx exceptions."""
        if headers is None:
            headers = {}
        full_url = urljoin(self.base_url, url)
        if self._client:
            return self._client.get(
                full_url, params=params, headers=headers, timeout=timeout
            )
        else:
            with httpx.Client() as client:
                return client.get(
                    full_url, params=params, headers=headers, timeout=timeout
                )

    def get(
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
            response = self._raw_get(url, params, headers, timeout)
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

    def close(self) -> None:
        """Closes the persistent client if it exists."""
        if self._client:
            self._client.close()
            self._client = None

    def __enter__(self) -> "MpesaHttpClient":
        """Context manager entry point."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit point. Closes the client."""
        self.close()
