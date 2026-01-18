"""B2CAccountTopUp: Handles M-Pesa B2C Account Topup API interactions.

This module provides functionality to initiate a B2C Account Topup transaction and handle result/timeout notifications
using the M-Pesa API. Requires a valid access token for authentication and uses the HttpClient for HTTP requests.
"""

from pydantic import BaseModel, ConfigDict

from mpesakit.auth import AsyncTokenManager, TokenManager
from mpesakit.http_client import AsyncHttpClient, HttpClient

from .schemas import (
    B2CAccountTopUpRequest,
    B2CAccountTopUpResponse,
)


class B2CAccountTopUp(BaseModel):
    """Represents the B2C Account TopUp API client for M-Pesa operations.

    https://developer.safaricom.co.ke/APIs/B2CAccountTopUp

    Attributes:
        http_client (HttpClient): HTTP client for making requests to the M-Pesa API.
        token_manager (TokenManager): Manages access tokens for authentication.
    """

    http_client: HttpClient
    token_manager: TokenManager

    model_config = ConfigDict(arbitrary_types_allowed=True)

    def topup(self, request: B2CAccountTopUpRequest) -> B2CAccountTopUpResponse:
        """Initiates a B2C Account TopUp transaction.

        Args:
            request (B2CAccountTopUpRequest): The B2C Account TopUp request data.

        Returns:
            B2CAccountTopUpResponse: Response from the M-Pesa API.
        """
        url = "/mpesa/b2b/v1/paymentrequest"
        headers = {
            "Authorization": f"Bearer {self.token_manager.get_token()}",
            "Content-Type": "application/json",
        }
        response_data = self.http_client.post(
            url, json=request.model_dump(mode="json"), headers=headers
        )
        return B2CAccountTopUpResponse(**response_data)


class AsyncB2CAccountTopUp(BaseModel):
    """Represents the async B2C Account TopUp API client for M-Pesa operations.

    Attributes:
        http_client (AsyncHttpClient): Async HTTP client for making requests to the M-Pesa API.
        token_manager (AsyncTokenManager): Async token manager for authentication.
    """

    http_client: AsyncHttpClient
    token_manager: AsyncTokenManager

    model_config = ConfigDict(arbitrary_types_allowed=True)

    async def topup(self, request: B2CAccountTopUpRequest) -> B2CAccountTopUpResponse:
        """Initiates a B2C Account TopUp transaction asynchronously.

        Args:
            request (B2CAccountTopUpRequest): The B2C Account TopUp request data.

        Returns:
            B2CAccountTopUpResponse: Response from the M-Pesa API.
        """
        url = "/mpesa/b2b/v1/paymentrequest"
        token = await self.token_manager.get_token()
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }
        response_data = await self.http_client.post(
            url, json=request.model_dump(mode="json"), headers=headers
        )
        return B2CAccountTopUpResponse(**response_data)
