"""Transaction Status: Handles M-Pesa Transaction Status API interactions.

This module provides functionality to query transaction status and handle result/timeout notifications
using the M-Pesa API. Requires a valid access token for authentication and uses the HttpClient for HTTP requests.
"""

from pydantic import BaseModel, ConfigDict

from mpesakit.auth import AsyncTokenManager, TokenManager
from mpesakit.http_client import AsyncHttpClient, HttpClient

from .schemas import (
    TransactionStatusRequest,
    TransactionStatusResponse,
)


class TransactionStatus(BaseModel):
    """Represents the Transaction Status API client for M-Pesa operations.

    https://developer.safaricom.co.ke/APIs/TransactionStatus

    Attributes:
        http_client (HttpClient): HTTP client for making requests to the M-Pesa API.
        token_manager (TokenManager): Manages access tokens for authentication.
    """

    http_client: HttpClient
    token_manager: TokenManager

    model_config = ConfigDict(arbitrary_types_allowed=True)

    def query(self, request: TransactionStatusRequest) -> TransactionStatusResponse:
        """Queries the status of a transaction.

        Args:
            request (TransactionStatusRequest): The transaction status query request.

        Returns:
            TransactionStatusResponse: Response from the M-Pesa API.
        """
        url = "/mpesa/transactionstatus/v1/query"
        headers = {
            "Authorization": f"Bearer {self.token_manager.get_token()}",
            "Content-Type": "application/json",
        }
        response_data = self.http_client.post(
            url, json=request.model_dump(by_alias=True), headers=headers
        )
        return TransactionStatusResponse(**response_data)


class AsyncTransactionStatus(BaseModel):
    """Represents the async Transaction Status API client for M-Pesa operations.

    Attributes:
        http_client (AsyncHttpClient): Async HTTP client for making requests to the M-Pesa API.
        token_manager (AsyncTokenManager): Async token manager for authentication.
    """

    http_client: AsyncHttpClient
    token_manager: AsyncTokenManager

    model_config = ConfigDict(arbitrary_types_allowed=True)

    async def query(
        self, request: TransactionStatusRequest
    ) -> TransactionStatusResponse:
        """Queries the status of a transaction asynchronously.

        Args:
            request (TransactionStatusRequest): The transaction status query request.

        Returns:
            TransactionStatusResponse: Response from the M-Pesa API.
        """
        url = "/mpesa/transactionstatus/v1/query"
        headers = {
            "Authorization": f"Bearer {await self.token_manager.get_token()}",
            "Content-Type": "application/json",
        }
        response_data = await self.http_client.post(
            url, json=request.model_dump(by_alias=True), headers=headers
        )
        return TransactionStatusResponse(**response_data)
