"""TaxRemittance: Handles M-Pesa Tax Remittance API interactions.

This module provides functionality to initiate a tax remittance transaction and handle result/timeout notifications
using the M-Pesa API. Requires a valid access token for authentication and uses the HttpClient for HTTP requests.
"""

from pydantic import BaseModel, ConfigDict
from mpesakit.auth import TokenManager
from mpesakit.http_client import HttpClient

from .schemas import (
    TaxRemittanceRequest,
    TaxRemittanceResponse,
)


class TaxRemittance(BaseModel):
    """Represents the Tax Remittance API client for M-Pesa operations.

    https://developer.safaricom.co.ke/APIs/TaxRemittance

    Attributes:
        http_client (HttpClient): HTTP client for making requests to the M-Pesa API.
        token_manager (TokenManager): Manages access tokens for authentication.
    """

    http_client: HttpClient
    token_manager: TokenManager

    model_config = ConfigDict(arbitrary_types_allowed=True)

    def remittance(self, request: TaxRemittanceRequest) -> TaxRemittanceResponse:
        """Initiates a tax remittance transaction.

        Args:
            request (TaxRemittanceRequest): The tax remittance request data.

        Returns:
            TaxRemittanceResponse: Response from the M-Pesa API.
        """
        url = "/mpesa/b2b/v1/remittax"
        headers = {
            "Authorization": f"Bearer {self.token_manager.get_token()}",
            "Content-Type": "application/json",
        }
        response_data = self.http_client.post(url, json=request.model_dump(by_alias=True), headers=headers)
        return TaxRemittanceResponse(**response_data)
