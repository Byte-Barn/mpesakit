from _typeshed import Incomplete
from mpesakit.auth import TokenManager as TokenManager
from mpesakit.http_client import HttpClient as HttpClient
from mpesakit.transaction_status import TransactionStatus as TransactionStatus, TransactionStatusRequest as TransactionStatusRequest, TransactionStatusResponse as TransactionStatusResponse

class TransactionService:
    http_client: Incomplete
    token_manager: Incomplete
    transaction_status: Incomplete
    def __init__(self, http_client: HttpClient, token_manager: TokenManager) -> None: ...
    def query_status(self, initiator: str, security_credential: str, transaction_id: str, party_a: int, identifier_type: int, result_url: str, queue_timeout_url: str, occasion: str = '', command_id: str | None = None, remarks: str | None = None, original_conversation_id: str | None = None, **kwargs) -> TransactionStatusResponse: ...
