from _typeshed import Incomplete
from mpesakit.auth import TokenManager as TokenManager
from mpesakit.b2b_express_checkout import B2BExpressCheckout as B2BExpressCheckout, B2BExpressCheckoutRequest as B2BExpressCheckoutRequest, B2BExpressCheckoutResponse as B2BExpressCheckoutResponse
from mpesakit.business_buy_goods import BusinessBuyGoods as BusinessBuyGoods, BusinessBuyGoodsRequest as BusinessBuyGoodsRequest, BusinessBuyGoodsResponse as BusinessBuyGoodsResponse
from mpesakit.business_paybill import BusinessPayBill as BusinessPayBill, BusinessPayBillRequest as BusinessPayBillRequest, BusinessPayBillResponse as BusinessPayBillResponse
from mpesakit.http_client import HttpClient as HttpClient

class B2BService:
    http_client: Incomplete
    token_manager: Incomplete
    def __init__(self, http_client: HttpClient, token_manager: TokenManager) -> None: ...
    def express_checkout(self, primary_short_code: str, receiver_short_code: str, amount: int, payment_ref: str, callback_url: str, partner_name: str, request_ref_id: str, **kwargs) -> B2BExpressCheckoutResponse: ...
    def paybill(self, initiator: str, security_credential: str, amount: int, party_a: int, party_b: int, account_reference: str, requester: str, remarks: str, queue_timeout_url: str, result_url: str, **kwargs) -> BusinessPayBillResponse: ...
    def buygoods(self, initiator: str, security_credential: str, amount: int, party_a: int, party_b: int, account_reference: str, requester: str, remarks: str, queue_timeout_url: str, result_url: str, occassion: str | None = None, **kwargs) -> BusinessBuyGoodsResponse: ...
