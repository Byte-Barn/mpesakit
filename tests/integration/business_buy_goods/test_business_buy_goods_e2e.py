"""End-to-End Test for M-Pesa Business Buy Goods.

This test simulates initiating a Business Buy Goods transaction using the M-Pesa Daraja API.
It requires valid credentials and a properly configured environment.

# TODO: When available, test e2e for Business Buy Goods confirmation and result callbacks.
"""

import os
import pytest
from dotenv import load_dotenv
from mpesakit.auth import TokenManager
from mpesakit.http_client import MpesaHttpClient

from mpesakit.business_buy_goods import (
    BusinessBuyGoods,
    BusinessBuyGoodsRequest,
)

load_dotenv()
pytestmark = pytest.mark.live


@pytest.fixture
def business_buy_goods_service():
    """Initialize the M-Pesa Business Buy Goods service with authentication."""
    http_client = MpesaHttpClient(env=os.getenv("MPESA_ENV", "sandbox"))
    token_manager = TokenManager(
        consumer_key=os.getenv("MPESA_CONSUMER_KEY"),
        consumer_secret=os.getenv("MPESA_CONSUMER_SECRET"),
        http_client=http_client,
    )
    print(token_manager.get_token())
    return BusinessBuyGoods(http_client=http_client, token_manager=token_manager)


def test_business_buy_goods_e2e(business_buy_goods_service):
    """End-to-end test for M-Pesa Business Buy Goods."""
    print("🔗 Starting E2E Test: Business Buy Goods")

    initiator = os.getenv("MPESA_INITIATOR_NAME", "Safaricom123!!")
    security_credential = os.getenv("MPESA_SECURITY_CREDENTIAL")
    amount = int(os.getenv("MPESA_BUSINESS_BUY_GOODS_AMOUNT", "1"))
    party_a = int(os.getenv("MPESA_PARTY_A", "600999"))
    party_b = int(os.getenv("MPESA_PARTY_B", "600998"))
    account_reference = os.getenv("MPESA_ACCOUNT_REFERENCE", "TestAccount")
    remarks = os.getenv("MPESA_BUSINESS_BUY_GOODS_REMARKS", "Test Business Buy Goods")
    queue_timeout_url = os.getenv(
        "MPESA_QUEUE_TIMEOUT_URL", "https://domainpath.com/buygoods/timeout"
    )
    result_url = os.getenv("MPESA_RESULT_URL", "https://domainpath.com/buygoods/result")

    request = BusinessBuyGoodsRequest(
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
    print(f"📤 Sending Business Buy Goods request: {dict(request)}")
    response = business_buy_goods_service.buy_goods(request=request)
    print(f"✅ Business Buy Goods response: {response}")

    assert response.ResponseDescription is not None
    assert response.ConversationID is not None
    assert response.is_successful is True, "Business Buy Goods transaction failed"
