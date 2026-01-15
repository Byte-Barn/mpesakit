from .schemas import TaxRemittanceRequest as TaxRemittanceRequest, TaxRemittanceResponse as TaxRemittanceResponse
from _typeshed import Incomplete
from mpesakit.auth import TokenManager as TokenManager
from mpesakit.http_client import HttpClient as HttpClient
from pydantic import BaseModel

class TaxRemittance(BaseModel):
    http_client: HttpClient
    token_manager: TokenManager
    model_config: Incomplete
    def remittance(self, request: TaxRemittanceRequest) -> TaxRemittanceResponse: ...
