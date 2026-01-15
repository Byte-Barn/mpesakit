"""BusinessBuyGoods: Handles M-Pesa Business Buy Goods API interactions.

This module provides functionality to initiate a Business Buy Goods transaction and handle result/timeout notifications
using the M-Pesa API. Requires a valid access token for authentication and uses the HttpClient for HTTP requests.
"""

from pydantic import BaseModel, ConfigDict
from mpesakit.auth import TokenManager
from mpesakit.http_client import HttpClient

from .schemas import (
    BusinessBuyGoodsRequest,
    BusinessBuyGoodsResponse,
)


class BusinessBuyGoods(BaseModel):
    """Represents the Business Buy Goods API client for M-Pesa operations.

    https://developer.safaricom.co.ke/APIs/BusinessBuyGoods

    Attributes:
        http_client (HttpClient): HTTP client for making requests to the M-Pesa API.
        token_manager (TokenManager): Manages access tokens for authentication.
    """

    http_client: HttpClient
    token_manager: TokenManager

    model_config = ConfigDict(arbitrary_types_allowed=True)

    def buy_goods(self, request: BusinessBuyGoodsRequest) -> BusinessBuyGoodsResponse:
        """Initiates a Business Buy Goods transaction.

        Args:
            request (BusinessBuyGoodsRequest): The Business Buy Goods request data.

        Returns:
            BusinessBuyGoodsResponse: Response from the M-Pesa API.
        """
        url = "/mpesa/b2b/v1/paymentrequest"
        headers = {
            "Authorization": f"Bearer {self.token_manager.get_token()}",
            "Content-Type": "application/json",
        }
        response_data = self.http_client.post(url, json=request.model_dump(by_alias=True), headers=headers)
        return BusinessBuyGoodsResponse(**response_data)
