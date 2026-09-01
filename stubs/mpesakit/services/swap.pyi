from _typeshed import Incomplete
from mpesakit.auth import AsyncTokenManager, TokenManager
from mpesakit.http_client import AsyncHttpClient, HttpClient
from mpesakit.swap import SwapRequest, SwapResponse

class SwapService:
    http_client: Incomplete
    token_manager: Incomplete
    def __init__(self,http_client:HttpClient,token_manager:TokenManager,environment:str) -> None:...
    def swap_query(self,customer_number:str) -> SwapResponse: ...
    def swap_request(self,request: SwapRequest) -> SwapResponse: ...


class AsyncSwapService:
    http_client: Incomplete
    token_manager: Incomplete
    def __init__(self,http_client:AsyncHttpClient,token_manager:AsyncTokenManager,environment:str) -> None:...
    async def swap_query(self,customer_number:str) -> SwapResponse: ...
    async def swap_request(self,request: SwapRequest) -> SwapResponse: ...

