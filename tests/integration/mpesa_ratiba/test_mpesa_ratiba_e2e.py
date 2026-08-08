"""End-to-End Test for M-Pesa Ratiba (Standing Order).

This test simulates initiating a Standing Order transaction using the M-Pesa Daraja API.
It requires valid credentials and a properly configured environment.
"""

import os
import pytest
from dotenv import load_dotenv
from mpesakit.auth import TokenManager
from mpesakit.http_client import MpesaHttpClient

from mpesakit.mpesa_ratiba import (
    MpesaRatiba,
    StandingOrderRequest,
    FrequencyEnum,
    TransactionTypeEnum,
    ReceiverPartyIdentifierTypeEnum,
)

load_dotenv()
pytestmark = pytest.mark.live


@pytest.fixture
def mpesa_ratiba_service():
    """Initialize the M-Pesa Ratiba service with authentication."""
    http_client = MpesaHttpClient(env=os.getenv("MPESA_ENV", "sandbox"))
    token_manager = TokenManager(
        consumer_key=os.getenv("MPESA_CONSUMER_KEY"),
        consumer_secret=os.getenv("MPESA_CONSUMER_SECRET"),
        http_client=http_client,
    )
    print(token_manager.get_token())
    return MpesaRatiba(http_client=http_client, token_manager=token_manager)


def test_mpesa_ratiba_e2e(mpesa_ratiba_service):
    """End-to-end test for M-Pesa Ratiba (Standing Order)."""
    print("🔗 Starting E2E Test: M-Pesa Ratiba (Standing Order)")

    standing_order_name = os.getenv("MPESA_RATIBA_NAME", "Test Standing Order")
    start_date = os.getenv("MPESA_RATIBA_START_DATE", "20240905")
    end_date = os.getenv("MPESA_RATIBA_END_DATE", "20250905")
    business_short_code = os.getenv("MPESA_RATIBA_BUSINESS_SHORT_CODE", "174379")
    transaction_type = TransactionTypeEnum.STANDING_ORDER_CUSTOMER_PAY_BILL
    receiver_party_identifier_type = ReceiverPartyIdentifierTypeEnum.BUSINESS_SHORT_CODE
    amount = os.getenv("MPESA_RATIBA_AMOUNT", "4500")
    party_a = os.getenv("MPESA_RATIBA_PARTY_A", "254708374149")
    callback_url = os.getenv("MPESA_RATIBA_CALLBACK_URL", "https://mydomain.com/pat")
    account_reference = os.getenv("MPESA_RATIBA_ACCOUNT_REFERENCE", "Test")
    transaction_desc = os.getenv("MPESA_RATIBA_TRANSACTION_DESC", "BikeRepayment")
    frequency = FrequencyEnum.MONTHLY

    request = StandingOrderRequest(
        StandingOrderName=standing_order_name,
        StartDate=start_date,
        EndDate=end_date,
        BusinessShortCode=business_short_code,
        TransactionType=transaction_type,
        ReceiverPartyIdentifierType=receiver_party_identifier_type,
        Amount=amount,
        PartyA=party_a,
        CallBackURL=callback_url,
        AccountReference=account_reference,
        TransactionDesc=transaction_desc,
        Frequency=frequency,
    )
    print(f"📤 Sending Standing Order request: {request.model_dump(mode='json')}")
    response = mpesa_ratiba_service.create_standing_order(request=request)
    print(f"✅ Standing Order response: {response}")

    assert response.ResponseHeader.responseDescription is not None
    assert response.ResponseHeader.responseCode == "200"
    assert response.is_successful is True, "Standing Order transaction failed"
