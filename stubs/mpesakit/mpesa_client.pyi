from _typeshed import Incomplete
from mpesakit.auth import TokenManager as TokenManager
from mpesakit.http_client import MpesaHttpClient as MpesaHttpClient
from mpesakit.services import B2BService as B2BService, B2CService as B2CService, BalanceService as BalanceService, BillService as BillService, C2BService as C2BService, DynamicQRCodeService as DynamicQRCodeService, RatibaService as RatibaService, ReversalService as ReversalService, StkPushService as StkPushService, TaxService as TaxService, TransactionService as TransactionService

class MpesaClient:
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
    def __init__(self, consumer_key: str, consumer_secret: str, environment: str = 'sandbox') -> None: ...
