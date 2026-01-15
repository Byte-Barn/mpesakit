from .schemas import AccountBalanceRequest as AccountBalanceRequest, AccountBalanceResponse as AccountBalanceResponse
from _typeshed import Incomplete
from mpesakit.auth import TokenManager as TokenManager
from mpesakit.http_client import HttpClient as HttpClient
from pydantic import BaseModel

class AccountBalance(BaseModel):
    http_client: HttpClient
    token_manager: TokenManager
    model_config: Incomplete
    def query(self, request: AccountBalanceRequest) -> AccountBalanceResponse: ...
