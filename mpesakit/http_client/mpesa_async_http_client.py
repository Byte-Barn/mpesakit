"""MpesaAsyncHttpClient: An asynchronous client for making HTTP requests to the M-Pesa API."""

from typing import Dict, Any, Optional
import httpx

from mpesakit.errors import MpesaError, MpesaApiException
from .http_client import AsyncHttpClient


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


    async def post(
        self, url: str, json: Dict[str, Any], headers: Dict[str, str]
    ) -> Dict[str, Any]:
        """Sends an asynchronous POST request to the M-Pesa API."""
        try:

            response = await self._client.post(
                url, json=json, headers=headers, timeout=10
            )


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
        except httpx.ConnectError:
            raise MpesaApiException(
                MpesaError(
                    error_code="CONNECTION_ERROR",
                    error_message="Failed to connect to Mpesa API. Check network or URL.",
                    status_code=None,
                )
            )
        except httpx.HTTPError as e:

            raise MpesaApiException(
                MpesaError(
                    error_code="REQUEST_FAILED",
                    error_message=f"HTTP request failed: {str(e)}",
                    status_code=None,
                    raw_response=None,
                )
            )

    async def get(
        self,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """Sends an asynchronous GET request to the M-Pesa API."""
        try:
            if headers is None:
                headers = {}

            response = await self._client.get(
                url, params=params, headers=headers, timeout=10
            )

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
        except httpx.ConnectError:
            raise MpesaApiException(
                MpesaError(
                    error_code="CONNECTION_ERROR",
                    error_message="Failed to connect to Mpesa API. Check network or URL.",
                    status_code=None,
                )
            )
        except httpx.HTTPError as e:
            raise MpesaApiException(
                MpesaError(
                    error_code="REQUEST_FAILED",
                    error_message=f"HTTP request failed: {str(e)}",
                    status_code=None,
                    raw_response=None,
                )
            )

    async def aclose(self):
        """Manually close the underlying httpx client connection pool."""
        await self._client.aclose()
