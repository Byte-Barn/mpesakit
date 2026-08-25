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
        merchant_name: str,
        ref_no: str,
        amount: float,
        trx_code: str,
        cpi: str,
        size: str,
        **kwargs,
    ) -> DynamicQRGenerateResponse:
        """Generate a dynamic QR code for payment.

        Args:
            merchant_name: Name of the merchant.
            ref_no: Reference number for the transaction.
            amount: Transaction amount.
            trx_code: Transaction type (DynamicQRTransactionType).
            cpi: CPI code.
            size: Size of the QR code.
            **kwargs: Additional fields for DynamicQRGenerateRequest.

        Returns:
            Response DynamicQRGenerateResponse containing QR code details.
        """
        request = DynamicQRGenerateRequest(
            MerchantName=merchant_name,
            RefNo=ref_no,
            Amount=int(amount),
            TrxCode=trx_code,
            CPI=cpi,
            Size=size,
            **{
                k: v
                for k, v in kwargs.items()
                if k in DynamicQRGenerateRequest.model_fields
            },
        )
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
        merchant_name: str,
        ref_no: str,
        amount: float,
        trx_code: str,
        cpi: str,
        size: str,
        **kwargs,
    ) -> DynamicQRGenerateResponse:
        """Generate a dynamic QR code for payment asynchronously."""
        request = DynamicQRGenerateRequest(
            MerchantName=merchant_name,
            RefNo=ref_no,
            Amount=int(amount),
            TrxCode=trx_code,
            CPI=cpi,
            Size=size,
            **{
                k: v
                for k, v in kwargs.items()
                if k in DynamicQRGenerateRequest.model_fields
            },
        )

        headers = {
            "Authorization": f"Bearer {await self.token_manager.get_token()}",
            "Content-Type": "application/json",
        }

        response_data = await self.http_client.post(
            self._api_path, json=request.model_dump(by_alias=True), headers=headers
        )

        return DynamicQRGenerateResponse(**response_data)
