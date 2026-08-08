"""End-to-End Test for M-Pesa Transaction Status Query.

This test simulates querying the status of a transaction using the M-Pesa Daraja API.
It requires valid credentials and a previously initiated transaction.

# TODO: When available, test e2e for Transaction Status confirmation and result callbacks.
"""

import os
import pytest
from dotenv import load_dotenv
from mpesakit.auth import TokenManager
from mpesakit.http_client import MpesaHttpClient
from mpesakit.transaction_status import (
    TransactionStatus,
    TransactionStatusRequest,
    TransactionStatusIdentifierType,
)


load_dotenv()
pytestmark = pytest.mark.live


@pytest.fixture
def transaction_status_service():
    """Initialize the M-Pesa Transaction Status service with authentication."""
    http_client = MpesaHttpClient(env=os.getenv("MPESA_ENV", "sandbox"))
    token_manager = TokenManager(
        consumer_key=os.getenv("MPESA_CONSUMER_KEY"),
        consumer_secret=os.getenv("MPESA_CONSUMER_SECRET"),
        http_client=http_client,
    )
    print(token_manager.get_token())
    return TransactionStatus(http_client=http_client, token_manager=token_manager)


def test_transaction_status_query_e2e(transaction_status_service):
    """End-to-end test for M-Pesa Transaction Status Query."""
    print("🔗 Starting E2E Test: Transaction Status Query")

    initiator = os.getenv("MPESA_INITIATOR_NAME", "Safaricom123!!")
    security_credential = os.getenv("MPESA_SECURITY_CREDENTIAL")
    shortcode = int(os.getenv("MPESA_SHORTCODE", "600999"))
    transaction_id = os.getenv(
        "MPESA_TRANSACTION_ID", "LKXXXX1234"
    )  # Replace with a valid transaction ID
    queue_timeout_url = os.getenv(
        "MPESA_QUEUE_TIMEOUT_URL", "https://domainpath.com/transaction/timeout"
    )
    result_url = os.getenv(
        "MPESA_RESULT_URL", "https://domainpath.com/transaction/result"
    )

    request = TransactionStatusRequest(
        Initiator=initiator,
        SecurityCredential=security_credential,
        CommandID="TransactionStatusQuery",
        TransactionID=transaction_id,
        PartyA=shortcode,
        IdentifierType=TransactionStatusIdentifierType.SHORT_CODE,
        ResultURL=result_url,
        QueueTimeOutURL=queue_timeout_url,
        Remarks="Test Transaction Status",
        Occasion="TestStatusQuery",
    )
    print(f"📤 Sending Transaction Status request: {dict(request)}")
    response = transaction_status_service.query(request=request)
    print(f"✅ Transaction Status response: {response}")

    assert response.is_successful is True, "Transaction status query failed"
    assert response.ResponseDescription is not None
    assert response.ConversationID is not None

    # Test using OriginalConversationID instead of TransactionID
    OriginalConversationID = os.getenv(
        "MPESA_OriginalConversationID", "fb68-4739-bb93-c6cb5d9fb16313586"
    )
    request.OriginalConversationID = OriginalConversationID
    request.TransactionID = None  # Remove TransactionID to use OriginalConversationID

    print(
        f"📤 Sending Transaction Status request with OriginalConversationID: {OriginalConversationID}"
    )
    response_with_original_conversation_id = transaction_status_service.query(
        request=request
    )
    print(
        f"✅ Transaction Status response (OriginalConversationID): {response_with_original_conversation_id}"
    )

    assert response_with_original_conversation_id.ResponseDescription is not None
    assert response_with_original_conversation_id.ConversationID is not None
    assert response_with_original_conversation_id.is_successful is True, (
        "Transaction status query with OriginalConversationID failed"
    )
