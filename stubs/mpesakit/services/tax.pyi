from _typeshed import Incomplete
from mpesakit.auth import TokenManager as TokenManager
from mpesakit.http_client import HttpClient as HttpClient
from mpesakit.tax_remittance import TaxRemittance as TaxRemittance, TaxRemittanceRequest as TaxRemittanceRequest, TaxRemittanceResponse as TaxRemittanceResponse

class TaxService:
    http_client: Incomplete
    token_manager: Incomplete
    tax_remittance: Incomplete
    def __init__(self, http_client: HttpClient, token_manager: TokenManager) -> None: ...
    def remittance(self, initiator: str, security_credential: str, amount: int, party_a: int, remarks: str, account_reference: str, result_url: str, queue_timeout_url: str, **kwargs) -> TaxRemittanceResponse: ...
