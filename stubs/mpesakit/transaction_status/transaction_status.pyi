from .schemas import TransactionStatusRequest as TransactionStatusRequest, TransactionStatusResponse as TransactionStatusResponse
from _typeshed import Incomplete
from mpesakit.auth import TokenManager as TokenManager
from mpesakit.http_client import HttpClient as HttpClient
from pydantic import BaseModel

class TransactionStatus(BaseModel):
    http_client: HttpClient
    token_manager: TokenManager
    model_config: Incomplete
    def query(self, request: TransactionStatusRequest) -> TransactionStatusResponse: ...
