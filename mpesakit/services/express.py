"""Facade for M-Pesa STK Push (Mpesa Express) service."""

from mpesakit.auth import TokenManager, AsyncTokenManager
from mpesakit.http_client import HttpClient, AsyncHttpClient


from mpesakit.mpesa_express import (
    StkPush,
    AsyncStkPush,
    StkPushSimulateRequest,
    StkPushSimulateResponse,
    StkPushQueryRequest,
    StkPushQueryResponse,
)


class StkPushService:
    """Facade for M-Pesa STK Push (Mpesa Express) operations."""

    def __init__(self, http_client: HttpClient, token_manager: TokenManager) -> None:
        """Initialize the STK Push service."""
        self.http_client = http_client
        self.token_manager = token_manager
        self.stk_push = StkPush(
            http_client=self.http_client,
            token_manager=self.token_manager,
        )

    def push(
        self,
        business_short_code: int,
        transaction_type: str,
        amount: float,
        party_a: str,
        party_b: str,
        phone_number: str,
        callback_url: str,
        account_reference: str,
        transaction_desc: str,
        passkey: str | None = None,
        timestamp: str | None = None,
        password: str | None = None,
        **kwargs,
    ) -> StkPushSimulateResponse:
        """Initiate an M-Pesa STK Push transaction.

        Args:
            business_short_code: M-Pesa business shortcode.
            transaction_type: Transaction type (e.g., 'CustomerPayBillOnline').
            amount: Transaction amount.
            party_a: MSISDN sending the funds.
            party_b: Business shortcode receiving the funds.
            phone_number: MSISDN to receive the STK prompt.
            callback_url: URL for receiving the callback.
            account_reference: Reference for the transaction.
            transaction_desc: Description of the transaction.
            passkey: M-Pesa passkey.
            timestamp: Timestamp for the transaction.
            password: Password for the transaction.
            **kwargs: Additional fields for StkPushSimulateRequest.

        Returns:
            StkPushSimulateResponse: Response from M-Pesa API.
        """
        request = StkPushSimulateRequest(
            BusinessShortCode=business_short_code,
            TransactionType=transaction_type,
            Amount=amount,
            PartyA=party_a,
            PartyB=party_b,
            PhoneNumber=phone_number,
            CallBackURL=callback_url,
            AccountReference=account_reference,
            TransactionDesc=transaction_desc,
            Passkey=passkey,
            Timestamp=timestamp,
            Password=password,
            **{
                k: v
                for k, v in kwargs.items()
                if k in StkPushSimulateRequest.model_fields
            },
        )
        return self.stk_push.push(request)

    def query(
        self,
        business_short_code: int,
        checkout_request_id: str,
        passkey: str | None = None,
        password: str | None = None,
        timestamp: str | None = None,
        **kwargs,
    ) -> StkPushQueryResponse:
        """Query the status of an M-Pesa STK Push transaction.

        Args:
            business_short_code: M-Pesa business shortcode.
            passkey: M-Pesa passkey.
            checkout_request_id: CheckoutRequestID from the push response.
            password: Password for the transaction.
            timestamp: Timestamp for the transaction.
            **kwargs: Additional fields for StkPushQueryRequest.

        Returns:
            StkPushQueryResponse: Response from M-Pesa API.
        """
        request = StkPushQueryRequest(
            BusinessShortCode=business_short_code,
            Passkey=passkey,
            CheckoutRequestID=checkout_request_id,
            Password=password,
            Timestamp=timestamp,
            **{
                k: v for k, v in kwargs.items() if k in StkPushQueryRequest.model_fields
            },
        )
        return self.stk_push.query(request)


class AsyncStkPushService:
    """Async facade for M-Pesa STK Push operations."""

    def __init__(
        self, http_client: AsyncHttpClient, token_manager: AsyncTokenManager
    ) -> None:
        """Initialize the async STK Push service."""
        self.http_client = http_client
        self.token_manager = token_manager
        self.stk_push = AsyncStkPush(
            http_client=self.http_client,
            token_manager=self.token_manager,
        )

    async def push(
        self,
        business_short_code: int,
        transaction_type: str,
        amount: float,
        party_a: str,
        party_b: str,
        phone_number: str,
        callback_url: str,
        account_reference: str,
        transaction_desc: str,
        passkey: str | None = None,
        timestamp: str | None = None,
        password: str | None = None,
        **kwargs,
    ) -> StkPushSimulateResponse:
        """Initiate an M-Pesa STK Push transaction asynchronously."""
        request = StkPushSimulateRequest(
            BusinessShortCode=business_short_code,
            TransactionType=transaction_type,
            Amount=amount,
            PartyA=party_a,
            PartyB=party_b,
            PhoneNumber=phone_number,
            CallBackURL=callback_url,
            AccountReference=account_reference,
            TransactionDesc=transaction_desc,
            Passkey=passkey,
            Timestamp=timestamp,
            Password=password,
            **{
                k: v
                for k, v in kwargs.items()
                if k in StkPushSimulateRequest.model_fields
            },
        )
        return await self.stk_push.push(request)

    async def query(
        self,
        business_short_code: int,
        checkout_request_id: str,
        passkey: str | None = None,
        password: str | None = None,
        timestamp: str | None = None,
        **kwargs,
    ) -> StkPushQueryResponse:
        """Query the status of an M-Pesa STK Push transaction asynchronously."""
        request = StkPushQueryRequest(
            BusinessShortCode=business_short_code,
            Passkey=passkey,
            CheckoutRequestID=checkout_request_id,
            Password=password,
            Timestamp=timestamp,
            **{
                k: v for k, v in kwargs.items() if k in StkPushQueryRequest.model_fields
            },
        )
        return await self.stk_push.query(request)
