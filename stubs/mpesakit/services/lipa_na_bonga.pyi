from mpesakit.auth import AsyncTokenManager, TokenManager
from mpesakit.http_client import AsyncHttpClient, HttpClient

from mpesakit.lipa_na_bonga import (
    AsyncLipaNaBonga,
    LipaNaBonga,
    CalculatePointsRequest,
    CalculatePointsResponse,
    RedeemPointsRequest,
    RedeemPointsResponse,
)


class BongaService:
 
    def __init__(self, http_client: HttpClient, token_manager: TokenManager) -> None:...
    

    def calculate_points(
        self,
        points: int | str,
        **kwargs,
    ) -> CalculatePointsResponse:...
    def redeem_points(
        self,
        msisdn: str | int,
        amount: float | int,
        bonga_points: int,
        conversion_rate: float,
        short_code: str | int,
        account_number: str,
        **kwargs,
    ) -> RedeemPointsResponse:...

class AsyncBongaService:
    def __init__(
        self, http_client: AsyncHttpClient, token_manager: AsyncTokenManager
    ) -> None:...

    async def calculate_points(
        self,
        points: int | str,
        **kwargs,
    ) -> CalculatePointsResponse:...
    async def redeem_points(
        self,
        msisdn: str | int,
        amount: float | int,
        bonga_points: int,
        conversion_rate: float,
        short_code: str | int,
        account_number: str,
        **kwargs,
    ) -> RedeemPointsResponse:...