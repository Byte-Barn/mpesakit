from _typeshed import Incomplete
from mpesakit.auth import TokenManager as TokenManager
from mpesakit.b2p import B2P as B2P, B2PCommandIDType as B2PCommandIDType, B2PRequest as B2PRequest, B2PResponse as B2PResponse
from mpesakit.http_client import HttpClient as HttpClient

class B2PService:
    http_client: Incomplete
    token_manager: Incomplete
    b2p: Incomplete
    def __init__(self, http_client: HttpClient, token_manager: TokenManager) -> None: ...
    def send_payment(self, originator_conversation_id: str, initiator_name: str, security_credential: str, amount: int, party_a: str, party_b: str, remarks: str, queue_timeout_url: str, result_url: str, command_id: B2PCommandIDType = ..., occasion: str | None = None, **kwargs) -> B2PResponse: ...
