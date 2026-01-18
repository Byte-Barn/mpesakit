"""BusinessPaybill: Handles M-Pesa Business PayBill API interactions.

This module provides functionality to initiate a Business PayBill transaction and handle result/timeout notifications
using the M-Pesa API. Requires a valid access token for authentication and uses the HttpClient for HTTP requests.
"""

from pydantic import BaseModel, ConfigDict

from mpesakit.auth import AsyncTokenManager, TokenManager
from mpesakit.http_client import AsyncHttpClient, HttpClient

from .schemas import (
    BusinessPayBillRequest,
    BusinessPayBillResponse,
)


class BusinessPayBill(BaseModel):
    """Represents the Business PayBill API client for M-Pesa operations.

    https://developer.safaricom.co.ke/APIs/BusinessPayBill

    Attributes:
        http_client (HttpClient): HTTP client for making requests to the M-Pesa API.
        token_manager (TokenManager): Manages access tokens for authentication.
    """

    http_client: HttpClient
    token_manager: TokenManager

    model_config = ConfigDict(arbitrary_types_allowed=True)

    def paybill(self, request: BusinessPayBillRequest) -> BusinessPayBillResponse:
        """Initiates a Business PayBill transaction.

        Args:
            request (BusinessPayBillRequest): The Business PayBill request data.

        Returns:
            BusinessPayBillResponse: Response from the M-Pesa API.
        """
        url = "/mpesa/b2b/v1/paymentrequest"
        headers = {
            "Authorization": f"Bearer {self.token_manager.get_token()}",
            "Content-Type": "application/json",
        }
        response_data = self.http_client.post(
            url, json=request.model_dump(by_alias=True), headers=headers
        )
        return BusinessPayBillResponse(**response_data)


class AsyncBusinessPayBill(BaseModel):
    """Represents the async Business PayBill API client for M-Pesa operations.

    Attributes:
        http_client (AsyncHttpClient): Async HTTP client for making requests to the M-Pesa API.
        token_manager (AsyncTokenManager): Async token manager for authentication.
    """

    http_client: AsyncHttpClient
    token_manager: AsyncTokenManager

    model_config = ConfigDict(arbitrary_types_allowed=True)

    async def paybill(self, request: BusinessPayBillRequest) -> BusinessPayBillResponse:
        """Initiates a Business PayBill transaction asynchronously.

        Args:
            request (BusinessPayBillRequest): The Business PayBill request data.

        Returns:
            BusinessPayBillResponse: Response from the M-Pesa API.
        """
        url = "/mpesa/b2b/v1/paymentrequest"
        headers = {
            "Authorization": f"Bearer {await self.token_manager.get_token()}",
            "Content-Type": "application/json",
        }
        response_data = await self.http_client.post(
            url, json=request.model_dump(by_alias=True), headers=headers
        )
        return BusinessPayBillResponse(**response_data)
