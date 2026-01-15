from .schemas import StandingOrderRequest as StandingOrderRequest, StandingOrderResponse as StandingOrderResponse
from _typeshed import Incomplete
from mpesakit.auth import TokenManager as TokenManager
from mpesakit.http_client import HttpClient as HttpClient
from pydantic import BaseModel

class MpesaRatiba(BaseModel):
    http_client: HttpClient
    token_manager: TokenManager
    model_config: Incomplete
    def create_standing_order(self, request: StandingOrderRequest) -> StandingOrderResponse: ...
