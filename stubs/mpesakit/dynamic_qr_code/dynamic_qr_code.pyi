from .schemas import DynamicQRGenerateRequest as DynamicQRGenerateRequest, DynamicQRGenerateResponse as DynamicQRGenerateResponse
from _typeshed import Incomplete
from mpesakit.auth import TokenManager as TokenManager
from mpesakit.http_client import HttpClient as HttpClient
from pydantic import BaseModel

class DynamicQRCode(BaseModel):
    http_client: HttpClient
    token_manager: TokenManager
    model_config: Incomplete
    def generate(self, request: DynamicQRGenerateRequest) -> DynamicQRGenerateResponse: ...
