"""B2P: Handles M-Pesa B2Pochi Business to Pochi La Biashara API interactions.

This module provides functionality to initiate B2Pochi payments using the M-Pesa API.
Requires a valid access token for authentication and uses the HttpClient for HTTP requests.
"""

from pydantic import BaseModel, ConfigDict

from mpesakit.auth import TokenManager
from mpesakit.http_client import HttpClient

from .schemas import (
    B2PRequest,
    B2PResponse,
)


class B2P(BaseModel):
    """Represents the B2Pochi API client for M-Pesa Business to Pochi operations.

    https://developer.safaricom.co.ke/APIs/BusinessToPochi

    Attributes:
        http_client (HttpClient): HTTP client for making requests to the M-Pesa API.
        token_manager (TokenManager): Manages access tokens for authentication.
    """

    http_client: HttpClient
    token_manager: TokenManager

    model_config = ConfigDict(arbitrary_types_allowed=True)

    def send_payment(self, request: B2PRequest) -> B2PResponse:
        """Initiates a B2Pochi payment request.

        Args:
            request B2PRequestt: The payment request details.

        Returns:
            B2PResponse: Response from the M-Pesa API after payment initiation.
        """
        url = "/mpesa/b2pochi/v1/paymentrequest"
        headers = {
            "Authorization": f"Bearer {self.token_manager.get_token()}",
            "Content-Type": "application/json",
        }
        response_data = self.http_client.post(
            url,
            json=request.model_dump(by_alias=True, exclude_none=True),
            headers=headers,
        )
        return B2PResponse(**response_data)
