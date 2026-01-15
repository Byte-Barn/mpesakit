"""Account Balance: Handles M-Pesa Account Balance API interactions.

This module provides functionality to query account balance and handle result/timeout notifications
using the M-Pesa API. Requires a valid access token for authentication and uses the HttpClient for HTTP requests.
"""

from pydantic import BaseModel, ConfigDict
from mpesakit.auth import TokenManager
from mpesakit.http_client import HttpClient

from .schemas import (
    AccountBalanceRequest,
    AccountBalanceResponse,
)


class AccountBalance(BaseModel):
    """Represents the Account Balance API client for M-Pesa operations.

    https://developer.safaricom.co.ke/APIs/AccountBalance

    Attributes:
        http_client (HttpClient): HTTP client for making requests to the M-Pesa API.
        token_manager (TokenManager): Manages access tokens for authentication.
    """

    http_client: HttpClient
    token_manager: TokenManager

    model_config = ConfigDict(arbitrary_types_allowed=True)

    def query(self, request: AccountBalanceRequest) -> AccountBalanceResponse:
        """Queries the account balance.

        Args:
            request (AccountBalanceRequest): The account balance query request.

        Returns:
            AccountBalanceResponse: Response from the M-Pesa API.
        """
        url = "/mpesa/accountbalance/v1/query"
        headers = {
            "Authorization": f"Bearer {self.token_manager.get_token()}",
            "Content-Type": "application/json",
        }
        response_data = self.http_client.post(url, json=request.model_dump(by_alias=True), headers=headers)
        return AccountBalanceResponse(**response_data)
