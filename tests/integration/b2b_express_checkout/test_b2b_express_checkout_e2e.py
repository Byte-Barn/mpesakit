"""End-to-End Test for M-Pesa B2B Express Checkout.

This test simulates initiating a B2B Express Checkout USSD Push transaction using the M-Pesa Daraja API.
It requires valid credentials and a properly configured environment.

# TODO: When available, test e2e for B2B Express Checkout confirmation and result callbacks.
"""

import os
import pytest
from dotenv import load_dotenv
from mpesakit.auth import TokenManager
from mpesakit.http_client import MpesaHttpClient
import uuid

from mpesakit.b2b_express_checkout import (
    B2BExpressCheckout,
    B2BExpressCheckoutRequest,
)

load_dotenv()
pytestmark = pytest.mark.live


@pytest.fixture
def b2b_express_checkout_service():
    """Initialize the M-Pesa B2B Express Checkout service with authentication."""
    http_client = MpesaHttpClient(env=os.getenv("MPESA_ENV", "sandbox"))
    token_manager = TokenManager(
        consumer_key=os.getenv("MPESA_CONSUMER_KEY"),
        consumer_secret=os.getenv("MPESA_CONSUMER_SECRET"),
        http_client=http_client,
    )
    print(token_manager.get_token())
    return B2BExpressCheckout(http_client=http_client, token_manager=token_manager)


def test_b2b_express_checkout_e2e(b2b_express_checkout_service):
    """End-to-end test for M-Pesa B2B Express Checkout."""
    print("🔗 Starting E2E Test: B2B Express Checkout")

    primary_short_code = int(os.getenv("MPESA_PARTY_A", "600999"))
    receiver_short_code = int(os.getenv("MPESA_PARTY_B", "600998"))
    amount = int(os.getenv("MPESA_B2B_EXPRESS_AMOUNT", "1"))
    payment_ref = os.getenv("MPESA_B2B_EXPRESS_REMARKS", "Test B2B Express Checkout")
    callback_url = os.getenv(
        "MPESA_RESULT_URL", "https://domainpath.com/b2bexpress/result"
    )
    partner_name = os.getenv("MPESA_PARTNER_NAME", "VendorName")
    request_ref_id = str(uuid.uuid4())

    request = B2BExpressCheckoutRequest(
        primaryShortCode=primary_short_code,
        receiverShortCode=receiver_short_code,
        amount=amount,
        paymentRef=payment_ref,
        callbackUrl=callback_url,
        partnerName=partner_name,
        RequestRefID=request_ref_id,
    )
    print(f"📤 Sending B2B Express Checkout request: {request.model_dump(mode='json')}")
    response = b2b_express_checkout_service.ussd_push(request=request)
    print(f"✅ B2B Express Checkout response: {response}")

    assert response.ResponseDescription is not None
    assert response.ConversationID is not None
    assert response.is_successful is True, "B2B Express Checkout transaction failed"
