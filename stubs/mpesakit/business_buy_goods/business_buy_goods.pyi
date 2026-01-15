from .schemas import BusinessBuyGoodsRequest as BusinessBuyGoodsRequest, BusinessBuyGoodsResponse as BusinessBuyGoodsResponse
from _typeshed import Incomplete
from mpesakit.auth import TokenManager as TokenManager
from mpesakit.http_client import HttpClient as HttpClient
from pydantic import BaseModel

class BusinessBuyGoods(BaseModel):
    http_client: HttpClient
    token_manager: TokenManager
    model_config: Incomplete
    def buy_goods(self, request: BusinessBuyGoodsRequest) -> BusinessBuyGoodsResponse: ...
