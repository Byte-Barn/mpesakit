"""End-to-End Test for M-Pesa Transaction Reversal.

This test simulates initiating a transaction reversal using the M-Pesa Daraja API.
It requires valid credentials and a previously completed transaction.

# TODO: When available, test e2e for Reversal confirmation and result callbacks.
"""

import os
import pytest
from dotenv import load_dotenv
from mpesakit.auth import TokenManager
from mpesakit.http_client import MpesaHttpClient


from mpesakit.reversal import (
    Reversal,
    ReversalRequest,
)

load_dotenv()
pytestmark = pytest.mark.live


@pytest.fixture
def reversal_service():
    """Initialize the M-Pesa Reversal service with authentication."""
    http_client = MpesaHttpClient(env=os.getenv("MPESA_ENV", "sandbox"))
    token_manager = TokenManager(
        consumer_key=os.getenv("MPESA_CONSUMER_KEY"),
        consumer_secret=os.getenv("MPESA_CONSUMER_SECRET"),
        http_client=http_client,
    )
    print(token_manager.get_token())
    return Reversal(http_client=http_client, token_manager=token_manager)


def test_transaction_reversal_e2e(reversal_service):
    """End-to-end test for M-Pesa Transaction Reversal."""
    print("🔗 Starting E2E Test: Transaction Reversal")

    initiator = os.getenv("MPESA_INITIATOR_NAME", "Safaricom123!!")
    security_credential = os.getenv("MPESA_SECURITY_CREDENTIAL")
    shortcode = int(os.getenv("MPESA_SHORTCODE", "600999"))
    transaction_id = os.getenv(
        "MPESA_TRANSACTION_ID", "LKXXXX1234"
    )  # Replace with a valid transaction ID
    amount = int(os.getenv("MPESA_REVERSAL_AMOUNT", "1"))
    receiver_party = int(os.getenv("MPESA_RECEIVER_PARTY", shortcode))
    queue_timeout_url = os.getenv(
        "MPESA_QUEUE_TIMEOUT_URL", "https://domainpath.com/reversal/timeout"
    )
    result_url = os.getenv("MPESA_RESULT_URL", "https://domainpath.com/reversal/result")
    remarks = os.getenv("MPESA_REVERSAL_REMARKS", "Test Reversal")
    occasion = os.getenv("MPESA_REVERSAL_OCCASION", "TestReversal")

    request = ReversalRequest(
        Initiator=initiator,
        SecurityCredential=security_credential,
        TransactionID=transaction_id,
        Amount=amount,
        ReceiverParty=receiver_party,
        ResultURL=result_url,
        QueueTimeOutURL=queue_timeout_url,
        Remarks=remarks,
        Occasion=occasion,
    )
    request = request.model_dump(by_alias=True)
    print(f"📤 Sending Transaction Reversal request: {request}")
    response = reversal_service.reverse(request=request)
    print(f"✅ Transaction Reversal response: {response}")

    assert response.ResponseDescription is not None
    assert response.ConversationID is not None
    assert response.is_successful is True, "Transaction reversal failed"
