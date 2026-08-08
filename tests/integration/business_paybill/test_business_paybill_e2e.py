"""End-to-End Test for M-Pesa Business PayBill.

This test simulates initiating a Business PayBill transaction using the M-Pesa Daraja API.
It requires valid credentials and a properly configured environment.

# TODO: When available, test e2e for Business PayBill confirmation and result callbacks.
"""

import os
import pytest
from dotenv import load_dotenv
from mpesakit.auth import TokenManager
from mpesakit.http_client import MpesaHttpClient

from mpesakit.business_paybill import (
    BusinessPayBill,
    BusinessPayBillRequest,
)

load_dotenv()
pytestmark = pytest.mark.live


@pytest.fixture
def business_paybill_service():
    """Initialize the M-Pesa Business PayBill service with authentication."""
    http_client = MpesaHttpClient(env=os.getenv("MPESA_ENV", "sandbox"))
    token_manager = TokenManager(
        consumer_key=os.getenv("MPESA_CONSUMER_KEY"),
        consumer_secret=os.getenv("MPESA_CONSUMER_SECRET"),
        http_client=http_client,
    )
    print(token_manager.get_token())
    return BusinessPayBill(http_client=http_client, token_manager=token_manager)


def test_business_paybill_e2e(business_paybill_service):
    """End-to-end test for M-Pesa Business PayBill."""
    print("🔗 Starting E2E Test: Business PayBill")

    initiator = os.getenv("MPESA_INITIATOR_NAME", "Safaricom123!!")
    security_credential = os.getenv("MPESA_SECURITY_CREDENTIAL")
    amount = int(os.getenv("MPESA_BUSINESS_PAYBILL_AMOUNT", "1"))
    party_a = int(os.getenv("MPESA_PARTY_A", "600999"))
    party_b = int(os.getenv("MPESA_PARTY_B", "600998"))
    account_reference = os.getenv("MPESA_ACCOUNT_REFERENCE", "TestAccount")
    remarks = os.getenv("MPESA_BUSINESS_PAYBILL_REMARKS", "Test Business PayBill")
    queue_timeout_url = os.getenv(
        "MPESA_QUEUE_TIMEOUT_URL", "https://domainpath.com/paybill/timeout"
    )
    result_url = os.getenv("MPESA_RESULT_URL", "https://domainpath.com/paybill/result")

    request = BusinessPayBillRequest(
        Initiator=initiator,
        SecurityCredential=security_credential,
        Amount=amount,
        PartyA=party_a,
        PartyB=party_b,
        AccountReference=account_reference,
        Remarks=remarks,
        QueueTimeOutURL=queue_timeout_url,
        ResultURL=result_url,
    )
    request = dict(request)
    print(f"📤 Sending Business PayBill request: {request}")
    response = business_paybill_service.paybill(request=request)
    print(f"✅ Business PayBill response: {response}")

    assert response.ResponseDescription is not None
    assert response.ConversationID is not None
    assert response.is_successful is True, "Business PayBill transaction failed"
