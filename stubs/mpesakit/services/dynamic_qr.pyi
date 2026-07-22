from _typeshed import Incomplete
from mpesakit.auth import TokenManager as TokenManager, AsyncTokenManager as AsyncTokenManager
from mpesakit.dynamic_qr_code import DynamicQRCode as DynamicQRCode, DynamicQRGenerateRequest as DynamicQRGenerateRequest, DynamicQRGenerateResponse as DynamicQRGenerateResponse
from mpesakit.http_client import HttpClient as HttpClient, AsyncHttpClient as AsyncHttpClient

class DynamicQRCodeService:
    http_client: Incomplete
    token_manager: Incomplete
    qr_code: Incomplete
    def __init__(self, http_client: HttpClient, token_manager: TokenManager) -> None: ...
    def generate(self, merchant_name: str, ref_no: str, amount: float, trx_code: str, cpi: str, size: str, **kwargs) -> DynamicQRGenerateResponse: ...

class AsyncDynamicQRCodeService:
    http_client: Incomplete
    token_manager: Incomplete
    qr_code: Incomplete
    def __init__(self, http_client: AsyncHttpClient, token_manager: AsyncTokenManager) -> None: ...
    async def generate(self, merchant_name: str, ref_no: str, amount: float, trx_code: str, cpi: str, size: str, **kwargs) -> DynamicQRGenerateResponse: ...
