"""SWAP: This API returns the last date a SIM card was swapped.

Provides functionality to initiate Swap queries using the M-Pesa API.
"""

from typing import Literal
from pydantic import BaseModel, ConfigDict

from mpesakit.auth import AsyncTokenManager, TokenManager
from mpesakit.http_client import AsyncHttpClient, HttpClient

from .schemas import SwapRequest, SwapResponse

SWAP_ENDPOINT = "/imsi/v2/checkATI"

class Swap(BaseModel):
    """Represents the Swap API client for SIM card dating."""

    http_client: HttpClient
    token_manager: TokenManager
    environment: Literal["sandbox", "production"] = "sandbox"

    model_config = ConfigDict(arbitrary_types_allowed=True)

    def swap_request(self, request: SwapRequest) -> SwapResponse:
        """Initiates a Swap request synchronously."""
        headers = {
            "Authorization": f"Bearer {self.token_manager.get_token()}",
            "Content-Type": "application/json",
        }

        response_data = self.http_client.post(
            SWAP_ENDPOINT, json=request.model_dump(by_alias=True), headers=headers
        )
        return SwapResponse(**response_data)


class AsyncSwap(BaseModel):
    """Represents the async Swap API client for SIM card dating."""

    http_client: AsyncHttpClient
    token_manager: AsyncTokenManager
    environment: Literal["sandbox", "production"] = "sandbox"

    model_config = ConfigDict(arbitrary_types_allowed=True)

    async def swap_request(self, request: SwapRequest) -> SwapResponse:
        """Initiates a Swap request asynchronously."""
        token = await self.token_manager.get_token()
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }

        response_data = await self.http_client.post(
            SWAP_ENDPOINT, json=request.model_dump(by_alias=True), headers=headers
        )
        return SwapResponse(**response_data)
