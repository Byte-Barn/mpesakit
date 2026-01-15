from .schemas import B2BExpressCheckoutRequest as B2BExpressCheckoutRequest, B2BExpressCheckoutResponse as B2BExpressCheckoutResponse
from _typeshed import Incomplete
from mpesakit.auth import TokenManager as TokenManager
from mpesakit.http_client import HttpClient as HttpClient
from pydantic import BaseModel

class B2BExpressCheckout(BaseModel):
    http_client: HttpClient
    token_manager: TokenManager
    model_config: Incomplete
    def ussd_push(self, request: B2BExpressCheckoutRequest) -> B2BExpressCheckoutResponse: ...
