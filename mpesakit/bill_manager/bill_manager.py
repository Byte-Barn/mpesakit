"""BillManager: Handles M-PESA Bill Manager API interactions.

This module provides functionality for onboarding, invoicing, payment notifications,
acknowledgments, and invoice cancellation using the M-PESA Bill Manager API.
Requires a valid access token for authentication and uses the HttpClient for HTTP requests.
"""

from typing import Optional

from pydantic import BaseModel, ConfigDict

from mpesakit.auth import AsyncTokenManager, TokenManager
from mpesakit.http_client import AsyncHttpClient, HttpClient

from .schemas import (
    BillManagerBulkInvoiceRequest,
    BillManagerBulkInvoiceResponse,
    BillManagerCancelBulkInvoiceRequest,
    BillManagerCancelInvoiceResponse,
    BillManagerCancelSingleInvoiceRequest,
    BillManagerOptInRequest,
    BillManagerOptInResponse,
    BillManagerSingleInvoiceRequest,
    BillManagerSingleInvoiceResponse,
    BillManagerUpdateOptInRequest,
    BillManagerUpdateOptInResponse,
)


class BillManager(BaseModel):
    """Represents the Bill Manager API client for M-PESA operations."""

    http_client: HttpClient
    token_manager: TokenManager
    app_key: Optional[str] = None

    model_config = ConfigDict(arbitrary_types_allowed=True)

    def opt_in(self, request: BillManagerOptInRequest) -> BillManagerOptInResponse:
        """Onboard a paybill to Bill Manager."""
        url = "/v1/billmanager-invoice/optin"
        headers = {
            "Authorization": f"Bearer {self.token_manager.get_token()}",
            "Content-Type": "application/json",
        }
        response_data = self.http_client.post(
            url, json=request.model_dump(mode="json"), headers=headers
        )
        return BillManagerOptInResponse(**response_data)

    def _ensure_app_key(self):
        if self.app_key is None:
            raise ValueError(
                "app_key must be set for this operation. You must pass it when initializing BillManager."
            )

    def update_opt_in(
        self, request: BillManagerUpdateOptInRequest
    ) -> BillManagerUpdateOptInResponse:
        """Update opt-in details for Bill Manager."""
        self._ensure_app_key()
        url = "/v1/billmanager-invoice/change-optin-details"
        headers = {
            "Authorization": f"Bearer {self.token_manager.get_token()}",
            "Content-Type": "application/json",
            "appKey": f"{self.app_key}",
        }
        response_data = self.http_client.post(
            url, json=request.model_dump(mode="json"), headers=headers
        )
        return BillManagerUpdateOptInResponse(**response_data)

    def send_single_invoice(
        self, request: BillManagerSingleInvoiceRequest
    ) -> BillManagerSingleInvoiceResponse:
        """Send a single invoice via Bill Manager."""
        self._ensure_app_key()
        url = "/v1/billmanager-invoice/single-invoicing"
        headers = {
            "Authorization": f"Bearer {self.token_manager.get_token()}",
            "Content-Type": "application/json",
            "appKey": f"{self.app_key}",
        }
        response_data = self.http_client.post(
            url, json=request.model_dump(mode="json"), headers=headers
        )
        return BillManagerSingleInvoiceResponse(**response_data)

    def send_bulk_invoice(
        self, request: BillManagerBulkInvoiceRequest
    ) -> BillManagerBulkInvoiceResponse:
        """Send multiple invoices via Bill Manager."""
        self._ensure_app_key()
        url = "/v1/billmanager-invoice/bulk-invoicing"
        headers = {
            "Authorization": f"Bearer {self.token_manager.get_token()}",
            "Content-Type": "application/json",
            "appKey": f"{self.app_key}",
        }
        response_data = self.http_client.post(
            url, json=request.model_dump(mode="json"), headers=headers
        )
        return BillManagerBulkInvoiceResponse(**response_data)

    def cancel_single_invoice(
        self, request: BillManagerCancelSingleInvoiceRequest
    ) -> BillManagerCancelInvoiceResponse:
        """Cancel a single invoice via Bill Manager."""
        self._ensure_app_key()
        url = "/v1/billmanager-invoice/cancel-single-invoice"
        headers = {
            "Authorization": f"Bearer {self.token_manager.get_token()}",
            "Content-Type": "application/json",
            "appKey": f"{self.app_key}",
        }
        response_data = self.http_client.post(
            url, json=request.model_dump(mode="json"), headers=headers
        )
        return BillManagerCancelInvoiceResponse(**response_data)

    def cancel_bulk_invoice(
        self, request: BillManagerCancelBulkInvoiceRequest
    ) -> BillManagerCancelInvoiceResponse:
        """Cancel multiple invoices via Bill Manager."""
        self._ensure_app_key()
        url = "/v1/billmanager-invoice/cancel-bulk-invoices"
        headers = {
            "Authorization": f"Bearer {self.token_manager.get_token()}",
            "Content-Type": "application/json",
            "appKey": f"{self.app_key}",
        }
        response_data = self.http_client.post(
            url, json=request.model_dump(mode="json"), headers=headers
        )
        return BillManagerCancelInvoiceResponse(**response_data)


class AsyncBillManager(BaseModel):
    """Represents the async Bill Manager API client for M-PESA operations."""

    http_client: AsyncHttpClient
    token_manager: AsyncTokenManager
    app_key: Optional[str] = None

    model_config = ConfigDict(arbitrary_types_allowed=True)

    async def opt_in(
        self, request: BillManagerOptInRequest
    ) -> BillManagerOptInResponse:
        """Onboard a paybill to Bill Manager."""
        url = "/v1/billmanager-invoice/optin"
        headers = {
            "Authorization": f"Bearer {await self.token_manager.get_token()}",
            "Content-Type": "application/json",
        }
        response_data = await self.http_client.post(
            url, json=request.model_dump(mode="json"), headers=headers
        )
        return BillManagerOptInResponse(**response_data)

    def _ensure_app_key(self):
        if self.app_key is None:
            raise ValueError(
                "app_key must be set for this operation. You must pass it when initializing AsyncBillManager."
            )

    async def update_opt_in(
        self, request: BillManagerUpdateOptInRequest
    ) -> BillManagerUpdateOptInResponse:
        """Update opt-in details for Bill Manager."""
        self._ensure_app_key()
        url = "/v1/billmanager-invoice/change-optin-details"
        headers = {
            "Authorization": f"Bearer {await self.token_manager.get_token()}",
            "Content-Type": "application/json",
            "appKey": f"{self.app_key}",
        }
        response_data = await self.http_client.post(
            url, json=request.model_dump(mode="json"), headers=headers
        )
        return BillManagerUpdateOptInResponse(**response_data)

    async def send_single_invoice(
        self, request: BillManagerSingleInvoiceRequest
    ) -> BillManagerSingleInvoiceResponse:
        """Send a single invoice via Bill Manager."""
        self._ensure_app_key()
        url = "/v1/billmanager-invoice/single-invoicing"
        headers = {
            "Authorization": f"Bearer {await self.token_manager.get_token()}",
            "Content-Type": "application/json",
            "appKey": f"{self.app_key}",
        }
        response_data = await self.http_client.post(
            url, json=request.model_dump(mode="json"), headers=headers
        )
        return BillManagerSingleInvoiceResponse(**response_data)

    async def send_bulk_invoice(
        self, request: BillManagerBulkInvoiceRequest
    ) -> BillManagerBulkInvoiceResponse:
        """Send multiple invoices via Bill Manager."""
        self._ensure_app_key()
        url = "/v1/billmanager-invoice/bulk-invoicing"
        headers = {
            "Authorization": f"Bearer {await self.token_manager.get_token()}",
            "Content-Type": "application/json",
            "appKey": f"{self.app_key}",
        }
        response_data = await self.http_client.post(
            url, json=request.model_dump(mode="json"), headers=headers
        )
        return BillManagerBulkInvoiceResponse(**response_data)

    async def cancel_single_invoice(
        self, request: BillManagerCancelSingleInvoiceRequest
    ) -> BillManagerCancelInvoiceResponse:
        """Cancel a single invoice via Bill Manager."""
        self._ensure_app_key()
        url = "/v1/billmanager-invoice/cancel-single-invoice"
        headers = {
            "Authorization": f"Bearer {await self.token_manager.get_token()}",
            "Content-Type": "application/json",
            "appKey": f"{self.app_key}",
        }
        response_data = await self.http_client.post(
            url, json=request.model_dump(mode="json"), headers=headers
        )
        return BillManagerCancelInvoiceResponse(**response_data)

    async def cancel_bulk_invoice(
        self, request: BillManagerCancelBulkInvoiceRequest
    ) -> BillManagerCancelInvoiceResponse:
        """Cancel multiple invoices via Bill Manager."""
        self._ensure_app_key()
        url = "/v1/billmanager-invoice/cancel-bulk-invoices"
        headers = {
            "Authorization": f"Bearer {await self.token_manager.get_token()}",
            "Content-Type": "application/json",
            "appKey": f"{self.app_key}",
        }
        response_data = await self.http_client.post(
            url, json=request.model_dump(mode="json"), headers=headers
        )
        return BillManagerCancelInvoiceResponse(**response_data)
