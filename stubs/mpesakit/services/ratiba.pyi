from _typeshed import Incomplete
from mpesakit.auth import TokenManager as TokenManager, AsyncTokenManager as AsyncTokenManager
from mpesakit.http_client import HttpClient as HttpClient, AsyncHttpClient as AsyncHttpClient
from mpesakit.mpesa_ratiba import FrequencyEnum as FrequencyEnum, MpesaRatiba as MpesaRatiba, ReceiverPartyIdentifierTypeEnum as ReceiverPartyIdentifierTypeEnum, StandingOrderRequest as StandingOrderRequest, StandingOrderResponse as StandingOrderResponse, TransactionTypeEnum as TransactionTypeEnum

class RatibaService:
    http_client: Incomplete
    token_manager: Incomplete
    ratiba: Incomplete
    def __init__(self, http_client: HttpClient, token_manager: TokenManager) -> None: ...
    def create_standing_order(self, standing_order_name: str, start_date: str, end_date: str, business_short_code: str, transaction_type: TransactionTypeEnum, receiver_party_identifier_type: ReceiverPartyIdentifierTypeEnum, amount: str, party_a: str, callback_url: str, account_reference: str, transaction_desc: str, frequency: FrequencyEnum, **kwargs) -> StandingOrderResponse: ...

class AsyncRatibaService:
    http_client: Incomplete
    token_manager: Incomplete
    ratiba: Incomplete
    def __init__(self, http_client: AsyncHttpClient, token_manager: AsyncTokenManager) -> None: ...
    async def create_standing_order(self, standing_order_name: str, start_date: str, end_date: str, business_short_code: str, transaction_type: TransactionTypeEnum, receiver_party_identifier_type: ReceiverPartyIdentifierTypeEnum, amount: str, party_a: str, callback_url: str, account_reference: str, transaction_desc: str, frequency: FrequencyEnum, **kwargs) -> StandingOrderResponse: ...
