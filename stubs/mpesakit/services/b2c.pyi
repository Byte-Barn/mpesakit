from _typeshed import Incomplete
from mpesakit.auth import TokenManager as TokenManager
from mpesakit.b2c import B2C as B2C, B2CCommandIDType as B2CCommandIDType, B2CRequest as B2CRequest, B2CResponse as B2CResponse
from mpesakit.b2c_account_top_up import B2CAccountTopUp as B2CAccountTopUp, B2CAccountTopUpRequest as B2CAccountTopUpRequest, B2CAccountTopUpResponse as B2CAccountTopUpResponse
from mpesakit.http_client import HttpClient as HttpClient

class B2CService:
    http_client: Incomplete
    token_manager: Incomplete
    b2c: Incomplete
    def __init__(self, http_client: HttpClient, token_manager: TokenManager) -> None: ...
    def send_payment(self, originator_conversation_id: str, initiator_name: str, security_credential: str, command_id: B2CCommandIDType, amount: int, party_a: str, party_b: str, remarks: str, queue_timeout_url: str, result_url: str, occasion: str = '', **kwargs) -> B2CResponse: ...
    def account_topup(self, initiator: str, security_credential: str, amount: int, party_a: str, party_b: str, account_reference: str, requester: str, remarks: str, queue_timeout_url: str, result_url: str, **kwargs) -> B2CAccountTopUpResponse: ...
