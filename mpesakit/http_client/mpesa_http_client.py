"""MpesaHttpClient: A client for making HTTP requests to the M-Pesa API.

Handles GET and POST requests with error handling for common HTTP issues.
"""

import logging
from typing import Any, Dict, Optional
from urllib.parse import urljoin

import requests
from tenacity import (
    RetryCallState,
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_random_exponential,
)

from mpesakit.errors import MpesaApiException, MpesaError

from .http_client import HttpClient

logger = logging.getLogger(__name__)


def handle_request_error(response: requests.Response):
    """Handles non-successful HTTP responses.

    This function is now responsible for converting HTTP status codes
    and JSON parsing errors into MpesaApiException.
    """
    try:
        response_data = response.json()
    except ValueError:
        response_data = {"errorMessage": response.text.strip() or ""}

    if not response.ok:
        error_message = response_data.get("errorMessage", "")
        raise MpesaApiException(
            MpesaError(
                error_code=f"HTTP_{response.status_code}",
                error_message=error_message,
                status_code=response.status_code,
                raw_response=response_data,
            )
        )


def handle_retry_exception(retry_state: RetryCallState):
    """Custom hook to handle exceptions after all retries fail.

    It raises a custom MpesaApiException with the appropriate error code.
    """
    if retry_state.outcome:
        exception = retry_state.outcome.exception()

        if isinstance(exception, requests.exceptions.Timeout):
            raise MpesaApiException(
                MpesaError(error_code="REQUEST_TIMEOUT", error_message=str(exception))
            ) from exception
        elif isinstance(exception, requests.exceptions.ConnectionError):
            raise MpesaApiException(
                MpesaError(error_code="CONNECTION_ERROR", error_message=str(exception))
            ) from exception

        raise MpesaApiException(
            MpesaError(error_code="REQUEST_FAILED", error_message=str(exception))
        ) from exception

    raise MpesaApiException(
        MpesaError(
            error_code="REQUEST_FAILED",
            error_message="An unknown retry error occurred.",
        )
    )


def retry_enabled(enabled: bool):
    """Factory function to conditionally enable retries.

    Args:
        enabled (bool): Whether to enable retry logic.

    Returns:
        A retry condition function.
    """
    base_retry = retry_if_exception_type(
        requests.exceptions.Timeout
    ) | retry_if_exception_type(requests.exceptions.ConnectionError)

    def _retry(retry_state):
        if not enabled:
            return False
        return base_retry(retry_state)

    return _retry


class MpesaHttpClient(HttpClient):
    """A client for making HTTP requests to the M-Pesa API."""

    base_url: str
    _session: Optional[requests.Session] = None

    def __init__(self, env: str = "sandbox", use_session: bool = False):
        """Initializes the MpesaHttpClient instance.

        Args:
            env (str): The environment to connect to ('sandbox' or 'production').
            use_session (bool): Whether to use a persistent session.
        """
        self.base_url = self._resolve_base_url(env)
        if use_session:
            self._session = requests.Session()
            self._session.trust_env = False

    def _resolve_base_url(self, env: str) -> str:
        if env.lower() == "production":
            return "https://api.safaricom.co.ke"
        return "https://sandbox.safaricom.co.ke"

    @retry(
        retry=retry_enabled(enabled=True),
        wait=wait_random_exponential(multiplier=5, max=8),
        stop=stop_after_attempt(3),
        retry_error_callback=handle_retry_exception,
        before_sleep=before_sleep_log(logger, logging.WARNING),
    )
    def _raw_post(
        self, url: str, json: Dict[str, Any], headers: Dict[str, str], timeout: int = 10
    ) -> requests.Response:
        """Low-level POST request - may raise requests exceptions."""
        full_url = urljoin(self.base_url, url)
        if self._session:
            return self._session.post(
                full_url, json=json, headers=headers, timeout=timeout
            )
        else:
            return requests.post(full_url, json=json, headers=headers, timeout=timeout)

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
        response: requests.Response | None = None
        try:
            response = self._raw_post(url, json, headers, timeout)
            handle_request_error(response)
            return response.json()
        except (requests.RequestException, ValueError) as e:
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
    )
    def _raw_get(
        self,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> requests.Response:
        """Low-level GET request - may raise requests exceptions."""
        if headers is None:
            headers = {}
        full_url = urljoin(self.base_url, url)
        if self._session:
            return self._session.get(
                full_url, params=params, headers=headers, timeout=10
            )
        else:
            return requests.get(full_url, params=params, headers=headers, timeout=10)

    def get(
        self,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """Sends a GET request to the M-Pesa API.

        Args:
            url (str): The URL path for the request.
            params (Optional[Dict[str, Any]]): The URL parameters.
            headers (Optional[Dict[str, str]]): The HTTP headers.

        Returns:
            Dict[str, Any]: The JSON response from the API.
        """
        response: requests.Response | None = None
        try:
            response = self._raw_get(url, params, headers)
            handle_request_error(response)
            return response.json()
        except (requests.RequestException, ValueError) as e:
            raise MpesaApiException(
                MpesaError(
                    error_code="REQUEST_FAILED",
                    error_message=str(e),
                    status_code=getattr(response, "status_code", None),
                    raw_response=getattr(response, "text", None),
                )
            ) from e

    def close(self) -> None:
        """Closes the persistent session if it exists."""
        if self._session:
            self._session.close()
            self._session = None

    def __enter__(self) -> "MpesaHttpClient":
        """Context manager entry point."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit point. Closes the session."""
        self.close()
