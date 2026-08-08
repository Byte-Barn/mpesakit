"""End-to-End Test for M-Pesa Tax Remittance.

This test simulates initiating a tax remittance transaction using the M-Pesa Daraja API.
It requires valid credentials and a properly configured environment.

# TODO: When available, test e2e for Tax Remittance confirmation and result callbacks.
"""

import os
import pytest
from dotenv import load_dotenv
from mpesakit.auth import TokenManager
from mpesakit.http_client import MpesaHttpClient


from mpesakit.tax_remittance import (
    TaxRemittance,
    TaxRemittanceRequest,
)

load_dotenv()
pytestmark = pytest.mark.live


@pytest.fixture
def tax_remittance_service():
    """Initialize the M-Pesa Tax Remittance service with authentication."""
    http_client = MpesaHttpClient(env=os.getenv("MPESA_ENV", "sandbox"))
    token_manager = TokenManager(
        consumer_key=os.getenv("MPESA_CONSUMER_KEY"),
        consumer_secret=os.getenv("MPESA_CONSUMER_SECRET"),
        http_client=http_client,
    )
    print(token_manager.get_token())
    return TaxRemittance(http_client=http_client, token_manager=token_manager)


def test_tax_remittance_e2e(tax_remittance_service):
    """End-to-end test for M-Pesa Tax Remittance."""
    print("🔗 Starting E2E Test: Tax Remittance")

    initiator = os.getenv("MPESA_INITIATOR_NAME", "Safaricom123!!")
    security_credential = os.getenv("MPESA_SECURITY_CREDENTIAL")
    amount = int(os.getenv("MPESA_TAX_REMITTANCE_AMOUNT", "1"))
    party_a = int(os.getenv("MPESA_PARTY_A", "600999"))
    account_reference = os.getenv("MPESA_ACCOUNT_REFERENCE", "TestAccount")
    remarks = os.getenv("MPESA_TAX_REMITTANCE_REMARKS", "Test Tax Remittance")
    queue_timeout_url = os.getenv(
        "MPESA_QUEUE_TIMEOUT_URL", "https://domainpath.com/taxremittance/timeout"
    )
    result_url = os.getenv(
        "MPESA_RESULT_URL", "https://domainpath.com/taxremittance/result"
    )

    request = TaxRemittanceRequest(
        Initiator=initiator,
        SecurityCredential=security_credential,
        Amount=amount,
        PartyA=party_a,
        AccountReference=account_reference,
        Remarks=remarks,
        QueueTimeOutURL=queue_timeout_url,
        ResultURL=result_url,
    )
    request = dict(request)
    print(f"📤 Sending Tax Remittance request: {request}")
    response = tax_remittance_service.remittance(request=request)
    print(f"✅ Tax Remittance response: {response}")

    assert response.ResponseDescription is not None
    assert response.ConversationID is not None
    assert response.is_successful is True, "Tax remittance transaction failed"
