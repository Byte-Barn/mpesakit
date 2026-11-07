"""Facade for Lipa Na Bonga loyalty point redemption operations."""

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
    """Facade for Lipa Na Bonga operations."""

    def __init__(self, http_client: HttpClient, token_manager: TokenManager) -> None:
        """Initialize the Bonga service."""
        self.http_client = http_client
        self.token_manager = token_manager
        self.bonga = LipaNaBonga(
            http_client=self.http_client,
            token_manager=self.token_manager,
        )

    def calculate_points(
        self,
        points: int | str,
        **kwargs,
    ) -> CalculatePointsResponse:
        """Calculates the monetary value (KES) of a given amount of Bonga points.

        Args:
            points: Loyalty points accumulated by the customer.
            **kwargs: Additional fields for CalculatePointsRequest.

        Returns:
            CalculatePointsResponse: The converted amount, points, and rate.
        """
        request = CalculatePointsRequest(
            points=points,
            **{
                k: v
                for k, v in kwargs.items()
                if k in CalculatePointsRequest.model_fields
            },
        )
        return self.bonga.calculate_points(request)

    def redeem_points(
        self,
        msisdn: str | int,
        amount: float | int,
        bonga_points: int,
        conversion_rate: float,
        short_code: str | int,
        account_number: str,
        **kwargs,
    ) -> RedeemPointsResponse:
        """Initiates Bonga points deduction and triggers customer PIN prompt.

        Args:
            msisdn: Subscriber mobile number.
            amount: Monetary value of the points.
            bonga_points: Points to be deducted.
            conversion_rate: The conversion rate (e.g., 0.2).
            short_code: Merchant Paybill or Buy Goods Till number.
            account_number: Account reference name.
            **kwargs: Additional fields for RedeemPointsRequest.

        Returns:
            RedeemPointsResponse: The processing response from Safaricom.
        """
        request = RedeemPointsRequest(
            msisdn=msisdn,
            amount=amount,
            bongaPoints=bonga_points,
            conversionRate=conversion_rate,
            shortCode=short_code,
            accountNumber=account_number,
            **{
                k: v
                for k, v in kwargs.items()
                if k in RedeemPointsRequest.model_fields
            },
        )
        return self.bonga.redeem_points(request)


class AsyncBongaService:
    """Async facade for Lipa Na Bonga operations."""

    def __init__(
        self, http_client: AsyncHttpClient, token_manager: AsyncTokenManager
    ) -> None:
        """Initialize the async Bonga service."""
        self.http_client = http_client
        self.token_manager = token_manager
        self.bonga = AsyncLipaNaBonga(
            http_client=self.http_client,
            token_manager=self.token_manager,
        )

    async def calculate_points(
        self,
        points: int | str,
        **kwargs,
    ) -> CalculatePointsResponse:
        """Calculates the monetary value (KES) of a given amount of Bonga points asynchronously."""
        request = CalculatePointsRequest(
            points=points,
            **{
                k: v
                for k, v in kwargs.items()
                if k in CalculatePointsRequest.model_fields
            },
        )
        return await self.bonga.calculate_points(request)

    async def redeem_points(
        self,
        msisdn: str | int,
        amount: float | int,
        bonga_points: int,
        conversion_rate: float,
        short_code: str | int,
        account_number: str,
        **kwargs,
    ) -> RedeemPointsResponse:
        """Initiates Bonga points deduction asynchronously."""
        request = RedeemPointsRequest(
            msisdn=msisdn,
            amount=amount,
            bongaPoints=bonga_points,
            conversionRate=conversion_rate,
            shortCode=short_code,
            accountNumber=account_number,
            **{
                k: v
                for k, v in kwargs.items()
                if k in RedeemPointsRequest.model_fields
            },
        )
        return await self.bonga.redeem_points(request)
