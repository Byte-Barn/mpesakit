from _typeshed import Incomplete
from mpesakit.auth import TokenManager as TokenManager
from mpesakit.http_client import HttpClient as HttpClient
from mpesakit.mpesa_express import StkPush as StkPush, StkPushQueryRequest as StkPushQueryRequest, StkPushQueryResponse as StkPushQueryResponse, StkPushSimulateRequest as StkPushSimulateRequest, StkPushSimulateResponse as StkPushSimulateResponse

class StkPushService:
    http_client: Incomplete
    token_manager: Incomplete
    stk_push: Incomplete
    def __init__(self, http_client: HttpClient, token_manager: TokenManager) -> None: ...
    def push(self, business_short_code: int, transaction_type: str, amount: float, party_a: str, party_b: str, phone_number: str, callback_url: str, account_reference: str, transaction_desc: str, passkey: str | None = None, timestamp: str | None = None, password: str | None = None, **kwargs) -> StkPushSimulateResponse: ...
    def query(self, business_short_code: int, checkout_request_id: str, passkey: str | None = None, password: str | None = None, timestamp: str | None = None, **kwargs) -> StkPushQueryResponse: ...
