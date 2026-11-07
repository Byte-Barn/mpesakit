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
    http_client: HttpClient
    token_manager: TokenManager

    model_config = ConfigDict(arbitrary_types_allowed=True)

    def calculate_points(self, request: CalculatePointsRequest) -> CalculatePointsResponse:...

    def redeem_points(self, request: RedeemPointsRequest) -> RedeemPointsResponse:...


class AsyncLipaNaBonga(BaseModel):
    async def calculate_points(
        self, request: CalculatePointsRequest) -> CalculatePointsResponse:...

    async def redeem_points(self, request: RedeemPointsRequest) -> RedeemPointsResponse:...
