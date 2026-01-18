"""B2BExpressCheckout: Handles M-Pesa B2B Express Checkout API interactions.

This module provides functionality to initiate a B2B Express Checkout USSD Push transaction and handle result/timeout notifications
using the M-Pesa API. Requires a valid access token for authentication and uses the HttpClient for HTTP requests.
"""

from pydantic import BaseModel, ConfigDict

from mpesakit.auth import AsyncTokenManager, TokenManager
from mpesakit.http_client import AsyncHttpClient, HttpClient

from .schemas import (
    B2BExpressCheckoutRequest,
    B2BExpressCheckoutResponse,
)


class B2BExpressCheckout(BaseModel):
    """Represents the B2B Express Checkout API client for M-Pesa operations.

    https://developer.safaricom.co.ke/APIs/B2BExpressCheckout

    Attributes:
        http_client (HttpClient): HTTP client for making requests to the M-Pesa API.
        token_manager (TokenManager): Manages access tokens for authentication.
    """

    http_client: HttpClient
    token_manager: TokenManager

    model_config = ConfigDict(arbitrary_types_allowed=True)

    def ussd_push(
        self, request: B2BExpressCheckoutRequest
    ) -> B2BExpressCheckoutResponse:
        """Initiates a B2B Express Checkout USSD Push transaction.

        Args:
            request (B2BExpressCheckoutRequest): The B2B Express Checkout request data.

        Returns:
            B2BExpressCheckoutResponse: Response from the M-Pesa API.
        """
        url = "/v1/ussdpush/get-msisdn"
        headers = {
            "Authorization": f"Bearer {self.token_manager.get_token()}",
            "Content-Type": "application/json",
        }
        response_data = self.http_client.post(
            url, json=request.model_dump(mode="json"), headers=headers
        )
        return B2BExpressCheckoutResponse(**response_data)


class AsyncB2BExpressCheckout(BaseModel):
    """Represents the async B2B Express Checkout API client for M-Pesa operations.

    Attributes:
        http_client (AsyncHttpClient): Async HTTP client for making requests to the M-Pesa API.
        token_manager (AsyncTokenManager): Async token manager for authentication.
    """

    http_client: AsyncHttpClient
    token_manager: AsyncTokenManager

    model_config = ConfigDict(arbitrary_types_allowed=True)

    async def ussd_push(
        self, request: B2BExpressCheckoutRequest
    ) -> B2BExpressCheckoutResponse:
        """Initiates a B2B Express Checkout USSD Push transaction asynchronously.

        Args:
            request (B2BExpressCheckoutRequest): The B2B Express Checkout request data.

        Returns:
            B2BExpressCheckoutResponse: Response from the M-Pesa API.
        """
        url = "/v1/ussdpush/get-msisdn"
        headers = {
            "Authorization": f"Bearer {await self.token_manager.get_token()}",
            "Content-Type": "application/json",
        }
        response_data = await self.http_client.post(
            url, json=request.model_dump(mode="json"), headers=headers
        )
        return B2BExpressCheckoutResponse(**response_data)
