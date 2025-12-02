"""MpesaHttpClient: A client for making HTTP requests to the M-Pesa API.

Handles GET and POST requests with error handling for common HTTP issues.
"""

from typing import Dict, Any, Optional
import httpx

from mpesakit.errors import MpesaError, MpesaApiException
from .http_client import HttpClient


class MpesaHttpClient(HttpClient):
    """A client for making HTTP requests to the M-Pesa API.

    This client handles GET and POST requests, including error handling for common HTTP issues.
    It supports both sandbox and production environments.

    Attributes:
        base_url (str): The base URL for the M-Pesa API, depending on the environment.
    """

    base_url: str

    def __init__(self, env: str = "sandbox"):
        """Initializes the MpesaHttpClient with the specified environment.

        Args:
            env (str): The environment to use, either 'sandbox' or 'production'.
                Defaults to 'sandbox'.
        """
        self.base_url = self._resolve_base_url(env)

    def _resolve_base_url(self, env: str) -> str:
        if env.lower() == "production":
            return "https://api.safaricom.co.ke"
        return "https://sandbox.safaricom.co.ke"

    def post(
        self, url: str, json: Dict[str, Any], headers: Dict[str, str]
    ) -> Dict[str, Any]:
        """Sends a POST request to the M-Pesa API.

        Args:
            url (str): The endpoint URL to send the POST request to.
            json (Dict[str, Any]): The JSON payload to include in the request body.
            headers (Dict[str, str]): The headers to include in the request.

        Returns:
            Dict[str, Any]: The JSON response from the M-Pesa API.

        Raises:
            MpesaApiException: If the request fails or returns an error response.
        """
        try:
            full_url = f"{self.base_url}{url}"
            with httpx.Client(timeout=10) as client:
                response = client.post(full_url, json=json, headers=headers)

            try:
                response_data = response.json()
            except ValueError:
                response_data = {"errorMessage": response.text.strip() or ""}

            if response.status_code >= 400:
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

        except httpx.TimeoutException:
            raise MpesaApiException(
                MpesaError(
                    error_code="REQUEST_TIMEOUT",
                    error_message="Request to Mpesa timed out.",
                    status_code=None,
                )
            )
        except httpx.RequestError as e:
            raise MpesaApiException(
                MpesaError(
                    error_code="REQUEST_FAILED",
                    error_message=f"HTTP request failed: {str(e)}",
                    status_code=None,
                    raw_response=None,
                )
            )

    def get(
        self,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """Sends a GET request to the M-Pesa API.

        Args:
            url (str): The endpoint URL to send the GET request to.
            params (Optional[Dict[str, Any]]): The query parameters to include in the request.
            headers (Optional[Dict[str, str]]): The headers to include in the request.

        Returns:
            Dict[str, Any]: The JSON response from the M-Pesa API.

        Raises:
            MpesaApiException: If the request fails or returns an error response.
        """
        try:
            if headers is None:
                headers = {}
            full_url = f"{self.base_url}{url}"

            with httpx.Client(timeout=10) as client:
                response = client.get(full_url, params=params, headers=headers)

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

        except httpx.TimeoutException:
            raise MpesaApiException(
                MpesaError(
                    error_code="REQUEST_TIMEOUT",
                    error_message="Request to Mpesa timed out.",
                    status_code=None,
                )
            )
        except httpx.RequestError as e:
            raise MpesaApiException(
                MpesaError(
                    error_code="REQUEST_FAILED",
                    error_message=f"HTTP request failed: {str(e)}",
                    status_code=None,
                    raw_response=None,
                )
            )
