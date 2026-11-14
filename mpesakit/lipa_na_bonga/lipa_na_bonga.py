"""Lipa Na Bonga: Handles Safaricom loyalty point redemption operations.

This module allows merchants to calculate point values and initiate point redemptions
on behalf of customers. Requires an access token for authentication.
"""

from pydantic import BaseModel, ConfigDict

from mpesakit.auth import AsyncTokenManager, TokenManager
from mpesakit.http_client import AsyncHttpClient, HttpClient

from .schemas import (
    CalculatePointsRequest,
    CalculatePointsResponse,
    RedeemPointsRequest,
    RedeemPointsResponse,
)


class LipaNaBonga(BaseModel):
    """Represents the Lipa Na Bonga client for loyalty points processing.

    https://developer.safaricom.co.ke/apis/LipaNaBonga

    Attributes:
        http_client (HttpClient): HTTP client for making requests to the M-Pesa API.
        token_manager (TokenManager): Manages access tokens for authentication.
    """

    http_client: HttpClient
    token_manager: TokenManager

    model_config = ConfigDict(arbitrary_types_allowed=True)

    def calculate_points(self, request: CalculatePointsRequest) -> CalculatePointsResponse:
        """Calculates the monetary value (KES) of a given amount of Bonga points.

        Args:
            request (CalculatePointsRequest): The points calculation request payload.

        Returns:
            CalculatePointsResponse: The converted amount, points, and rate.
        """
        url = "/v1/lipa/na/bonga/calculate-points"
        headers = {
            "Authorization": f"Bearer {self.token_manager.get_token()}",
            "Content-Type": "application/json",
        }
        response_data = self.http_client.post(
            url, json=request.model_dump(by_alias=True), headers=headers
        )
        return CalculatePointsResponse(**response_data)

    def redeem_points(self, request: RedeemPointsRequest) -> RedeemPointsResponse:
        """Initiates Bonga points deduction and triggers customer PIN prompt.

        Args:
            request (RedeemPointsRequest): The redemption details payload.

        Returns:
            RedeemPointsResponse: The processing response from Safaricom.
        """
        url = "/v1/lipa/na/bonga/redeem-paybill"
        headers = {
            "Authorization": f"Bearer {self.token_manager.get_token()}",
            "Content-Type": "application/json",
        }
        response_data = self.http_client.post(
            url, json=request.model_dump(by_alias=True), headers=headers
        )
        return RedeemPointsResponse(**response_data)


class AsyncLipaNaBonga(BaseModel):
    """Represents the async Lipa Na Bonga client for loyalty points processing.

    https://developer.safaricom.co.ke/apis/LipaNaBonga

    Attributes:
        http_client (AsyncHttpClient): Async HTTP client for making requests to the M-Pesa API.
        token_manager (AsyncTokenManager): Async token manager for authentication.
    """

    http_client: AsyncHttpClient
    token_manager: AsyncTokenManager

    model_config = ConfigDict(arbitrary_types_allowed=True)

    async def calculate_points(
        self, request: CalculatePointsRequest
    ) -> CalculatePointsResponse:
        """Calculates the monetary value (KES) of a given amount of Bonga points asynchronously.

        Args:
            request (CalculatePointsRequest): The points calculation request payload.

        Returns:
            CalculatePointsResponse: The converted amount, points, and rate.
        """
        url = "/v1/lipa/na/bonga/calculate-points"
        headers = {
            "Authorization": f"Bearer {await self.token_manager.get_token()}",
            "Content-Type": "application/json",
        }
        response_data = await self.http_client.post(
            url, json=request.model_dump(by_alias=True), headers=headers
        )
        return CalculatePointsResponse(**response_data)

    async def redeem_points(
        self, request: RedeemPointsRequest
    ) -> RedeemPointsResponse:
        """Initiates Bonga points deduction asynchronously.

        Args:
            request (RedeemPointsRequest): The redemption details payload.

        Returns:
            RedeemPointsResponse: The processing response from Safaricom.
        """
        url = "/v1/lipa/na/bonga/redeem-paybill"
        headers = {
            "Authorization": f"Bearer {await self.token_manager.get_token()}",
            "Content-Type": "application/json",
        }
        response_data = await self.http_client.post(
            url, json=request.model_dump(by_alias=True), headers=headers
        )
        return RedeemPointsResponse(**response_data)
