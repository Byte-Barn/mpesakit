from _typeshed import Incomplete
from mpesakit.account_balance import AccountBalance as AccountBalance, AccountBalanceRequest as AccountBalanceRequest, AccountBalanceResponse as AccountBalanceResponse
from mpesakit.auth import TokenManager as TokenManager
from mpesakit.http_client import HttpClient as HttpClient

class BalanceService:
    http_client: Incomplete
    token_manager: Incomplete
    account_balance: Incomplete
    def __init__(self, http_client: HttpClient, token_manager: TokenManager) -> None: ...
    def query(self, initiator: str, security_credential: str, command_id: str, party_a: int, identifier_type: int, remarks: str, result_url: str, queue_timeout_url: str, **kwargs) -> AccountBalanceResponse: ...
