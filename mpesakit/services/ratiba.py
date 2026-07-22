"""Facade for M-Pesa Standing Order (Ratiba) API interactions."""

from mpesakit.auth import TokenManager, AsyncTokenManager
from mpesakit.http_client import HttpClient, AsyncHttpClient
from mpesakit.mpesa_ratiba import (
    MpesaRatiba,
    AsyncMpesaRatiba,
    StandingOrderRequest,
    StandingOrderResponse,
    TransactionTypeEnum,
    ReceiverPartyIdentifierTypeEnum,
    FrequencyEnum,
)


class RatibaService:
    """Facade for M-Pesa Standing Order (Ratiba) operations."""

    def __init__(self, http_client: HttpClient, token_manager: TokenManager) -> None:
        """Initialize the Ratiba service."""
        self.http_client = http_client
        self.token_manager = token_manager
        self.ratiba = MpesaRatiba(
            http_client=self.http_client,
            token_manager=self.token_manager,
        )

    def create_standing_order(
        self,
        standing_order_name: str,
        start_date: str,
        end_date: str,
        business_short_code: str,
        transaction_type: TransactionTypeEnum,
        receiver_party_identifier_type: ReceiverPartyIdentifierTypeEnum,
        amount: str,
        party_a: str,
        callback_url: str,
        account_reference: str,
        transaction_desc: str,
        frequency: FrequencyEnum,
        **kwargs,
    ) -> StandingOrderResponse:
        """Initiate a Standing Order transaction.

        Args:
            standing_order_name: Unique name for the standing order per customer.
            start_date: Start date for the standing order (yyyymmdd).
            end_date: End date for the standing order (yyyymmdd).
            business_short_code: Business short code to receive payment.
            transaction_type: Transaction type enum.
            receiver_party_identifier_type: Receiver party identifier type enum.
            amount: Amount to be transacted (whole number as string).
            party_a: Customer's M-PESA registered phone number.
            callback_url: URL to receive notifications.
            account_reference: Account reference for PayBill transactions.
            transaction_desc: Additional info/comment.
            frequency: Frequency of transactions enum.
            **kwargs: Additional fields for StandingOrderRequest.

        Returns:
            StandingOrderResponse: Response from the M-Pesa API.
        """
        request = StandingOrderRequest(
            StandingOrderName=standing_order_name,
            StartDate=start_date,
            EndDate=end_date,
            BusinessShortCode=business_short_code,
            TransactionType=transaction_type,
            ReceiverPartyIdentifierType=receiver_party_identifier_type,
            Amount=amount,
            PartyA=party_a,
            CallBackURL=callback_url,
            AccountReference=account_reference,
            TransactionDesc=transaction_desc,
            Frequency=frequency,
            **{
                k: v
                for k, v in kwargs.items()
                if k in StandingOrderRequest.model_fields
            },
        )
        return self.ratiba.create_standing_order(request)


class AsyncRatibaService:
    """Async facade for M-Pesa Standing Order (Ratiba) operations."""

    def __init__(
        self, http_client: AsyncHttpClient, token_manager: AsyncTokenManager
    ) -> None:
        """Initialize the async Ratiba service."""
        self.http_client = http_client
        self.token_manager = token_manager
        self.ratiba = AsyncMpesaRatiba(
            http_client=self.http_client,
            token_manager=self.token_manager,
        )

    async def create_standing_order(
        self,
        standing_order_name: str,
        start_date: str,
        end_date: str,
        business_short_code: str,
        transaction_type: TransactionTypeEnum,
        receiver_party_identifier_type: ReceiverPartyIdentifierTypeEnum,
        amount: str,
        party_a: str,
        callback_url: str,
        account_reference: str,
        transaction_desc: str,
        frequency: FrequencyEnum,
        **kwargs,
    ) -> StandingOrderResponse:
        """Initiate a Standing Order transaction asynchronously."""
        request = StandingOrderRequest(
            StandingOrderName=standing_order_name,
            StartDate=start_date,
            EndDate=end_date,
            BusinessShortCode=business_short_code,
            TransactionType=transaction_type,
            ReceiverPartyIdentifierType=receiver_party_identifier_type,
            Amount=amount,
            PartyA=party_a,
            CallBackURL=callback_url,
            AccountReference=account_reference,
            TransactionDesc=transaction_desc,
            Frequency=frequency,
            **{
                k: v
                for k, v in kwargs.items()
                if k in StandingOrderRequest.model_fields
            },
        )
        return await self.ratiba.create_standing_order(request)
