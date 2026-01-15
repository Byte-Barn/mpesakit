from .schemas import BusinessPayBillRequest as BusinessPayBillRequest, BusinessPayBillResponse as BusinessPayBillResponse
from _typeshed import Incomplete
from mpesakit.auth import TokenManager as TokenManager
from mpesakit.http_client import HttpClient as HttpClient
from pydantic import BaseModel

class BusinessPayBill(BaseModel):
    http_client: HttpClient
    token_manager: TokenManager
    model_config: Incomplete
    def paybill(self, request: BusinessPayBillRequest) -> BusinessPayBillResponse: ...
