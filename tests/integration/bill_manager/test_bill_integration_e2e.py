"""End-to-End Test for M-Pesa Bill Manager Opt-In.

This test simulates onboarding a paybill to Bill Manager using the M-Pesa Daraja API.
It requires valid credentials and a properly configured environment.
"""

import os
import pytest
from dotenv import load_dotenv
from mpesakit.auth import TokenManager
from mpesakit.http_client import MpesaHttpClient

from mpesakit.bill_manager import (
    BillManager,
    BillManagerOptInRequest,
)

load_dotenv()
pytestmark = pytest.mark.live


@pytest.fixture
def bill_manager_service():
    """Initialize the M-Pesa Bill Manager service with authentication."""
    http_client = MpesaHttpClient(env=os.getenv("MPESA_ENVIRONMENT", "sandbox"))
    token_manager = TokenManager(
        consumer_key=os.getenv("MPESA_CONSUMER_KEY"),
        consumer_secret=os.getenv("MPESA_CONSUMER_SECRET"),
        http_client=http_client,
    )
    app_key = os.getenv("MPESA_BILL_MANAGER_APP_KEY")
    return BillManager(
        http_client=http_client, token_manager=token_manager, app_key=app_key
    )


def test_bill_manager_opt_in_e2e(bill_manager_service):
    """End-to-end test for M-Pesa Bill Manager Opt-In."""
    print("🔗 Starting E2E Test: Bill Manager Opt-In")

    shortcode = os.getenv("MPESA_BILL_MANAGER_SHORTCODE", "600999")
    contact_email = os.getenv("MPESA_BILL_MANAGER_CONTACT_EMAIL", "john@example.com")
    contact_phone = os.getenv("MPESA_BILL_MANAGER_CONTACT_PHONE", "254700000000")
    callbackurl = os.getenv(
        "MPESA_BILL_MANAGER_CALLBACK_URL", "https://example.com/callback"
    )

    request = BillManagerOptInRequest(
        shortcode=shortcode,
        email=contact_email,
        officialContact=contact_phone,
        sendReminders=True,
        callbackurl=callbackurl,
    )
    print(f"📤 Sending Bill Manager Opt-In request: {request}")
    response = bill_manager_service.opt_in(request=request)
    print(f"✅ Bill Manager Opt-In response: {response}")

    assert response.status is not None
    assert response.message is not None
    assert response.status.lower() == "success", "Bill Manager Opt-In failed"
