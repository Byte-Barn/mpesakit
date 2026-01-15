from _typeshed import Incomplete
from mpesakit.auth import TokenManager as TokenManager
from mpesakit.c2b import C2B as C2B, C2BRegisterUrlRequest as C2BRegisterUrlRequest, C2BRegisterUrlResponse as C2BRegisterUrlResponse
from mpesakit.http_client import HttpClient as HttpClient

class C2BService:
    http_client: Incomplete
    token_manager: Incomplete
    c2b: Incomplete
    def __init__(self, http_client: HttpClient, token_manager: TokenManager) -> None: ...
    def register_url(self, short_code: int, response_type: str, confirmation_url: str, validation_url: str, **kwargs) -> C2BRegisterUrlResponse: ...
