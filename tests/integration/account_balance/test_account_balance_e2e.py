"""End-to-End Test for M-Pesa Account Balance Query.

This test simulates querying the account balance using the M-Pesa Daraja API.
It requires valid credentials and a configured account.

# TODO: When available, test e2e for Account Balance confirmation and result callbacks.
"""

import os
import pytest
from dotenv import load_dotenv
from mpesakit.auth import TokenManager
from mpesakit.http_client import MpesaHttpClient


from mpesakit.account_balance import (
    AccountBalance,
    AccountBalanceRequest,
    AccountBalanceIdentifierType,
)

load_dotenv()
pytestmark = pytest.mark.live


@pytest.fixture
def account_balance_service():
    """Initialize the M-Pesa Account Balance service with authentication."""
    http_client = MpesaHttpClient(env=os.getenv("MPESA_ENV", "sandbox"))
    token_manager = TokenManager(
        consumer_key=os.getenv("MPESA_CONSUMER_KEY"),
        consumer_secret=os.getenv("MPESA_CONSUMER_SECRET"),
        http_client=http_client,
    )
    print(token_manager.get_token())
    return AccountBalance(http_client=http_client, token_manager=token_manager)


def test_account_balance_query_e2e(account_balance_service):
    """End-to-end test for M-Pesa Account Balance Query."""
    print("🔗 Starting E2E Test: Account Balance Query")

    initiator = os.getenv("MPESA_INITIATOR_NAME", "Safaricom123!!")
    security_credential = os.getenv("MPESA_SECURITY_CREDENTIAL")
    shortcode = int(os.getenv("MPESA_SHORTCODE", "600999"))
    identifier_type = AccountBalanceIdentifierType.SHORT_CODE.value
    remarks = "Test Account Balance"
    queue_timeout_url = os.getenv(
        "MPESA_QUEUE_TIMEOUT_URL", "https://domainpath.com/accountbalance/timeout"
    )
    result_url = os.getenv(
        "MPESA_RESULT_URL", "https://domainpath.com/accountbalance/result"
    )

    request = AccountBalanceRequest(
        Initiator=initiator,
        SecurityCredential=security_credential,
        CommandID="AccountBalance",
        PartyA=shortcode,
        IdentifierType=identifier_type,
        Remarks=remarks,
        QueueTimeOutURL=queue_timeout_url,
        ResultURL=result_url,
    )
    print(f"📤 Sending Account Balance request: {dict(request)}")
    response = account_balance_service.query(request=request)
    print(f"✅ Account Balance response: {response}")

    assert response.is_successful is True, "Account balance query failed"
    assert response.ResponseDescription is not None
    assert response.ConversationID is not None
