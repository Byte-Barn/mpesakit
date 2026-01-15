from .schemas import B2CAccountTopUpRequest as B2CAccountTopUpRequest, B2CAccountTopUpResponse as B2CAccountTopUpResponse
from _typeshed import Incomplete
from mpesakit.auth import TokenManager as TokenManager
from mpesakit.http_client import HttpClient as HttpClient
from pydantic import BaseModel

class B2CAccountTopUp(BaseModel):
    http_client: HttpClient
    token_manager: TokenManager
    model_config: Incomplete
    def topup(self, request: B2CAccountTopUpRequest) -> B2CAccountTopUpResponse: ...
