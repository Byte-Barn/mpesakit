from _typeshed import Incomplete
from mpesakit.auth import TokenManager as TokenManager
from mpesakit.http_client import HttpClient as HttpClient
from mpesakit.reversal import Reversal as Reversal, ReversalRequest as ReversalRequest, ReversalResponse as ReversalResponse

class ReversalService:
    http_client: Incomplete
    token_manager: Incomplete
    def __init__(self, http_client: HttpClient, token_manager: TokenManager) -> None: ...
    def reverse(self, initiator: str, security_credential: str, transaction_id: str, amount: int, receiver_party: int, result_url: str, queue_timeout_url: str, remarks: str, occasion: str | None = None, **kwargs) -> ReversalResponse: ...
