from .schemas import B2CRequest as B2CRequest, B2CResponse as B2CResponse
from _typeshed import Incomplete
from mpesakit.auth import TokenManager as TokenManager
from mpesakit.http_client import HttpClient as HttpClient
from pydantic import BaseModel

class B2C(BaseModel):
    http_client: HttpClient
    token_manager: TokenManager
    model_config: Incomplete
    def send_payment(self, request: B2CRequest) -> B2CResponse: ...
