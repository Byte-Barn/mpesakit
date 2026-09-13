"""Facade for M-Pesa Dynamic QR Code generation service."""

from mpesakit.auth import TokenManager, AsyncTokenManager
from mpesakit.http_client import HttpClient, AsyncHttpClient
from mpesakit.dynamic_qr_code.schemas import (
    DynamicQRGenerateRequest,
    DynamicQRGenerateResponse,
)


class DynamicQRCodeService:
    """Facade for M-Pesa Dynamic QR Code generation."""

    def __init__(self, http_client: HttpClient, token_manager: TokenManager) -> None:
        """Initialize the Dynamic QR Code service."""
        self.http_client = http_client
        self.token_manager = token_manager
        self._api_path = "/mpesa/qrcode/v1/generate"

    def generate(
        self,
        request: DynamicQRGenerateRequest,
    ) -> DynamicQRGenerateResponse:
        """Generate a dynamic QR code for payment.

        Args:
            request: The request object containing all required payload attrs.

        Returns:
            Response DynamicQRGenerateResponse containing QR code details.
        """
        headers = {
            "Authorization": f"Bearer {self.token_manager.get_token()}",
            "Content-Type": "application/json",
        }

        response_data = self.http_client.post(
            self._api_path, json=request.model_dump(by_alias=True), headers=headers
        )

        return DynamicQRGenerateResponse(**response_data)


class AsyncDynamicQRCodeService:
    """Async facade for M-Pesa Dynamic QR Code generation."""

    def __init__(
        self, http_client: AsyncHttpClient, token_manager: AsyncTokenManager
    ) -> None:
        """Initialize the async Dynamic QR Code service."""
        self.http_client = http_client
        self.token_manager = token_manager
        self._api_path = "/mpesa/qrcode/v1/generate"

    async def generate(
        self,
        request: DynamicQRGenerateRequest,
    ) -> DynamicQRGenerateResponse:
        """Generate a dynamic QR code for payment asynchronously.

        Args:
            request: The request object containing all required payload attrs.

        Returns:
            Response DynamicQRGenerateResponse containing QR code details.
        """
        headers = {
            "Authorization": f"Bearer {await self.token_manager.get_token()}",
            "Content-Type": "application/json",
        }

        response_data = await self.http_client.post(
            self._api_path, json=request.model_dump(by_alias=True), headers=headers
        )

        return DynamicQRGenerateResponse(**response_data)
