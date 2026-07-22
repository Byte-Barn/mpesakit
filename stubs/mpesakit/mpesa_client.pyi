from _typeshed import Incomplete
from mpesakit.auth import TokenManager as TokenManager
from mpesakit.auth import AsyncTokenManager as AsyncTokenManager
from mpesakit.callback_mixin import MpesaCallbackMixin as MpesaCallbackMixin
from mpesakit.http_client import MpesaHttpClient as MpesaHttpClient
from mpesakit.http_client import MpesaAsyncHttpClient as MpesaAsyncHttpClient
from mpesakit.services import (
    B2BService as B2BService,
    AsyncB2BService as AsyncB2BService,
    B2CService as B2CService,
    AsyncB2CService as AsyncB2CService,
    BalanceService as BalanceService,
    AsyncBalanceService as AsyncBalanceService,
    BillService as BillService,
    AsyncBillService as AsyncBillService,
    C2BService as C2BService,
    AsyncC2BService as AsyncC2BService,
    DynamicQRCodeService as DynamicQRCodeService,
    AsyncDynamicQRCodeService as AsyncDynamicQRCodeService,
    RatibaService as RatibaService,
    AsyncRatibaService as AsyncRatibaService,
    ReversalService as ReversalService,
    AsyncReversalService as AsyncReversalService,
    StkPushService as StkPushService,
    AsyncStkPushService as AsyncStkPushService,
    TaxService as TaxService,
    AsyncTaxService as AsyncTaxService,
    TransactionService as TransactionService,
    AsyncTransactionService as AsyncTransactionService,
)

class MpesaClient(MpesaCallbackMixin):
    http_client: Incomplete
    token_manager: Incomplete
    express: Incomplete
    stk_push: Incomplete
    stk_query: Incomplete
    b2c: Incomplete
    b2b: Incomplete
    transactions: Incomplete
    tax: Incomplete
    balance: Incomplete
    reversal: Incomplete
    bill: Incomplete
    dynamic_qr: Incomplete
    c2b: Incomplete
    ratiba: Incomplete
    def __init__(
        self,
        consumer_key: str,
        consumer_secret: str,
        environment: str = "sandbox",
        use_session: bool = False,
    ) -> None: ...

class AsyncMpesaClient(MpesaCallbackMixin):
    http_client: Incomplete
    token_manager: Incomplete
    express: Incomplete
    stk_push: Incomplete
    stk_query: Incomplete
    b2c: Incomplete
    b2b: Incomplete
    transactions: Incomplete
    tax: Incomplete
    balance: Incomplete
    reversal: Incomplete
    bill: Incomplete
    dynamic_qr: Incomplete
    c2b: Incomplete
    ratiba: Incomplete
    def __init__(
        self, consumer_key: str, consumer_secret: str, environment: str = "sandbox"
    ) -> None: ...
