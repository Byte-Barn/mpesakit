"""Dynamic QR Code: Generates a dynamic M-Pesa QR Code.

This module provides functionality to generate a Dynamic QR code using the M-Pesa API.
It requires a valid access token for authentication and uses the HttpClient for making HTTP requests.
"""

from pydantic import BaseModel, ConfigDict
from mpesakit.auth import TokenManager
from mpesakit.http_client import HttpClient

from .schemas import (
    DynamicQRGenerateRequest,
    DynamicQRGenerateResponse,
)


class DynamicQRCode(BaseModel):
    """Represents the request payload for generating a Dynamic M-Pesa QR code.

    https://developer.safaricom.co.ke/APIs/DynamicQR

    Attributes:
        http_client (HttpClient): The HTTP client used to make requests to the M-Pesa API.
        token_manager (TokenManager): The token manager for handling access tokens.
    """

    http_client: HttpClient
    token_manager: TokenManager

    model_config = ConfigDict(arbitrary_types_allowed=True)

    def generate(self, request: DynamicQRGenerateRequest) -> DynamicQRGenerateResponse:
        """Generates a Dynamic M-Pesa QR Code.

        Args:
            request (DynamicQRGenerateRequest): The request data for generating the QR code.

        Returns:
            DynamicQRGenerateResponse: The response from the M-Pesa API after generating the QR code.
        """
        url = "/mpesa/qrcode/v1/generate"
        headers = {
            "Authorization": f"Bearer {self.token_manager.get_token()}",
            "Content-Type": "application/json",
        }

        response_data = self.http_client.post(url, json=request.model_dump(by_alias=True), headers=headers)

        return DynamicQRGenerateResponse(**response_data)
