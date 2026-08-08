"""End-to-End Test for M-Pesa B2C Account Topup.

This test simulates initiating a B2C Account Topup transaction using the M-Pesa Daraja API.
It requires valid credentials and a properly configured environment.
"""

import os
import pytest
from dotenv import load_dotenv
from mpesakit.auth import TokenManager
from mpesakit.http_client import MpesaHttpClient

from mpesakit.b2c_account_top_up import (
    B2CAccountTopUp,
    B2CAccountTopUpRequest,
)

load_dotenv()
pytestmark = pytest.mark.live


@pytest.fixture
def b2c_account_topup_service():
    """Initialize the M-Pesa B2C Account Topup service with authentication."""
    http_client = MpesaHttpClient(env=os.getenv("MPESA_ENV", "sandbox"))
    token_manager = TokenManager(
        consumer_key=os.getenv("MPESA_CONSUMER_KEY"),
        consumer_secret=os.getenv("MPESA_CONSUMER_SECRET"),
        http_client=http_client,
    )
    print(token_manager.get_token())
    return B2CAccountTopUp(http_client=http_client, token_manager=token_manager)


def test_b2c_account_topup_e2e(b2c_account_topup_service):
    """End-to-end test for M-Pesa B2C Account Topup."""
    print("🔗 Starting E2E Test: B2C Account Topup")

    initiator = os.getenv("MPESA_INITIATOR_NAME", "testapi")
    security_credential = os.getenv("MPESA_SECURITY_CREDENTIAL")
    amount = int(os.getenv("MPESA_B2C_TOPUP_AMOUNT", "1"))
    party_a = int(os.getenv("MPESA_PARTY_A", "600992"))
    party_b = int(os.getenv("MPESA_PARTY_B", "600000"))
    account_reference = os.getenv("MPESA_ACCOUNT_REFERENCE", "353353")
    requester = os.getenv("MPESA_REQUESTER")  # Optional, can be None
    remarks = os.getenv("MPESA_B2C_TOPUP_REMARKS", "Test B2C Topup")
    queue_timeout_url = os.getenv(
        "MPESA_QUEUE_TIMEOUT_URL", "https://domainpath.com/b2c/topup/timeout"
    )
    result_url = os.getenv(
        "MPESA_RESULT_URL", "https://domainpath.com/b2c/topup/result"
    )

    request = B2CAccountTopUpRequest(
        Initiator=initiator,
        SecurityCredential=security_credential,
        Amount=amount,
        PartyA=party_a,
        PartyB=party_b,
        AccountReference=account_reference,
        Requester=requester,
        Remarks=remarks,
        QueueTimeOutURL=queue_timeout_url,
        ResultURL=result_url,
    )
    print(f"📤 Sending B2C Account Topup request: {request.model_dump(mode='json')}")
    response = b2c_account_topup_service.topup(request=request)
    print(f"✅ B2C Account Topup response: {response}")

    assert response.ResponseDescription is not None
    assert response.ConversationID is not None
    assert response.is_successful is True, "B2C Account Topup transaction failed"
