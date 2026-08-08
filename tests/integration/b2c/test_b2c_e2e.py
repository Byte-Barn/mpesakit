"""End-to-End Test for M-Pesa B2C Payment Request.

This test simulates sending a B2C payment request using the M-Pesa Daraja API.
It requires valid credentials and a Bulk Disbursement Account Shortcode.

# TODO: When available, test e2e for B2C payment confirmation and result callbacks.
"""

import os
import uuid
import pytest
from dotenv import load_dotenv
from mpesakit.auth import TokenManager
from mpesakit.http_client import MpesaHttpClient
from mpesakit.b2c import B2C, B2CRequest, B2CCommandIDType


load_dotenv()
pytestmark = pytest.mark.live


@pytest.fixture
def b2c_service():
    """Initialize the M-Pesa B2C service with authentication."""
    http_client = MpesaHttpClient(env=os.getenv("MPESA_ENV", "sandbox"))
    token_manager = TokenManager(
        consumer_key=os.getenv("MPESA_CONSUMER_KEY"),
        consumer_secret=os.getenv("MPESA_CONSUMER_SECRET"),
        http_client=http_client,
    )
    print(token_manager.get_token())

    return B2C(http_client=http_client, token_manager=token_manager)


def test_b2c_send_payment_e2e(b2c_service):
    """End-to-end test for M-Pesa B2C Payment Request."""
    print("🔗 Starting E2E Test: B2C Payment Request")

    originator_conversation_id = str(uuid.uuid4())
    initiator_name = os.getenv("MPESA_INITIATOR_NAME", "Safaricom123!!")
    security_credential = os.getenv("MPESA_SECURITY_CREDENTIAL")
    shortcode = int(os.getenv("MPESA_SHORTCODE", "600999"))
    recipient_phone = int(os.getenv("MPESA_RECIPIENT_PHONE", "254712345678"))
    amount = int(os.getenv("MPESA_B2C_AMOUNT", "10"))
    queue_timeout_url = os.getenv(
        "MPESA_QUEUE_TIMEOUT_URL", "https://domainpath.com/b2c/timeout"
    )
    result_url = os.getenv("MPESA_RESULT_URL", "https://domainpath.com/b2c/result")

    request = B2CRequest(
        OriginatorConversationID=originator_conversation_id,
        InitiatorName=initiator_name,
        SecurityCredential=security_credential,
        CommandID=B2CCommandIDType.BusinessPayment.value,
        Amount=amount,
        PartyA=shortcode,
        PartyB=recipient_phone,
        Remarks="Test B2C Payment",
        QueueTimeOutURL=queue_timeout_url,
        ResultURL=result_url,
        Occasion="TestPayment",
    )
    print(f"📤 Sending B2C payment request: {dict(request)}")
    response = b2c_service.send_payment(request=request)
    print(f"✅ B2C Payment response: {response}")

    assert response.ResponseDescription is not None
    assert response.OriginatorConversationID == originator_conversation_id
    assert response.is_successful is True, "B2C payment request failed"
