"""B2C: Handles M-Pesa B2C (Business to Customer) API interactions.

This module provides functionality to initiate B2C payments using the M-Pesa API.
Requires a valid access token for authentication and uses the HttpClient for HTTP requests.
"""

from pydantic import BaseModel, ConfigDict
from mpesakit.auth import TokenManager
from mpesakit.http_client import HttpClient

from .schemas import (
    B2CRequest,
    B2CResponse,
)


class B2C(BaseModel):
    """Represents the B2C API client for M-Pesa Business to Customer operations.

    https://developer.safaricom.co.ke/APIs/BusinessToCustomerPayment

    Attributes:
        http_client (HttpClient): HTTP client for making requests to the M-Pesa API.
        token_manager (TokenManager): Manages access tokens for authentication.
    """

    http_client: HttpClient
    token_manager: TokenManager

    model_config = ConfigDict(arbitrary_types_allowed=True)

    def send_payment(self, request: B2CRequest) -> B2CResponse:
        """Initiates a B2C payment request.

        Args:
            request (B2CRequest): The payment request details.

        Returns:
            B2CResponse: Response from the M-Pesa API after payment initiation.
        """
        url = "/mpesa/b2c/v3/paymentrequest"
        headers = {
            "Authorization": f"Bearer {self.token_manager.get_token()}",
            "Content-Type": "application/json",
        }
        response_data = self.http_client.post(url, json=request.model_dump(by_alias=True), headers=headers)
        return B2CResponse(**response_data)
