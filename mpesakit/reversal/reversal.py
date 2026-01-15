"""Reversal: Handles M-Pesa Transaction Reversal API interactions.

This module provides functionality to initiate a transaction reversal and handle result/timeout notifications
using the M-Pesa API. Requires a valid access token for authentication and uses the HttpClient for HTTP requests.
"""

from pydantic import BaseModel, ConfigDict
from mpesakit.auth import TokenManager
from mpesakit.http_client import HttpClient


from .schemas import (
    ReversalRequest,
    ReversalResponse,
)


class Reversal(BaseModel):
    """Represents the Transaction Reversal API client for M-Pesa operations.

    https://developer.safaricom.co.ke/APIs/Reversal

    Attributes:
        http_client (HttpClient): HTTP client for making requests to the M-Pesa API.
        token_manager (TokenManager): Manages access tokens for authentication.
    """

    http_client: HttpClient
    token_manager: TokenManager

    model_config = ConfigDict(arbitrary_types_allowed=True)

    def reverse(self, request: ReversalRequest) -> ReversalResponse:
        """Initiates a transaction reversal.

        Args:
            request (ReversalRequest): The reversal request data.

        Returns:
            ReversalResponse: Response from the M-Pesa API.
        """
        url = "/mpesa/reversal/v1/request"
        headers = {
            "Authorization": f"Bearer {self.token_manager.get_token()}",
            "Content-Type": "application/json",
        }
        response_data = self.http_client.post(url, json=request.model_dump(by_alias=True), headers=headers)
        return ReversalResponse(**response_data)
