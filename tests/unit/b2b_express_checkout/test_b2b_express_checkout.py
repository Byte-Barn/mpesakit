"""Unit tests for the M-Pesa SDK B2B Express Checkout functionality.

This module tests the B2B Express Checkout API client, ensuring it can handle USSD push requests,
process responses correctly, and manage callback/error cases.
"""

import pytest
from unittest.mock import MagicMock
from mpesakit.auth import TokenManager
from mpesakit.http_client import HttpClient

from mpesakit.b2b_express_checkout import (
    B2BExpressCheckout,
    B2BExpressCheckoutRequest,
    B2BExpressCheckoutResponse,
    B2BExpressCheckoutCallback,
    B2BExpressCallbackResponse,
)


@pytest.fixture
def mock_token_manager():
    """Mock TokenManager to return a fixed token."""
    mock = MagicMock(spec=TokenManager)
    mock.get_token.return_value = "test_token"
    return mock


@pytest.fixture
def mock_http_client():
    """Mock HttpClient to simulate HTTP requests."""
    return MagicMock(spec=HttpClient)


@pytest.fixture
def b2b_express_checkout(mock_http_client, mock_token_manager):
    """Fixture to create a B2BExpressCheckout instance with mocked dependencies."""
    return B2BExpressCheckout(
        http_client=mock_http_client, token_manager=mock_token_manager
    )


def valid_b2b_express_checkout_request():
    """Create a valid B2BExpressCheckoutRequest for testing."""
    return B2BExpressCheckoutRequest(
        primaryShortCode=123456,
        receiverShortCode=654321,
        amount=100,
        paymentRef="Invoice123",
        callbackUrl="http://example.com/result",
        partnerName="VendorName",
        RequestRefID="550e8400-e29b-41d4-a716-446655440000",
    )


def test_ussd_push_acknowledged(b2b_express_checkout, mock_http_client):
    """Test that USSD push request is acknowledged, not finalized."""
    request = valid_b2b_express_checkout_request()
    response_data = {
        "code": "0",
        "status": "USSD Initiated Successfully",
    }
    mock_http_client.post.return_value = response_data

    response = b2b_express_checkout.ussd_push(request)

    assert isinstance(response, B2BExpressCheckoutResponse)
    assert response.is_successful() is True
    assert response.code == response_data["code"]
    assert response.status == response_data["status"]


def test_ussd_push_http_error(b2b_express_checkout, mock_http_client):
    """Test handling of HTTP errors during USSD push request."""
    request = valid_b2b_express_checkout_request()
    mock_http_client.post.side_effect = Exception("HTTP error")
    with pytest.raises(Exception) as excinfo:
        b2b_express_checkout.ussd_push(request)
    assert "HTTP error" in str(excinfo.value)


def test_b2b_express_checkout_success_callback():
    """Test parsing of a successful B2B Express Checkout callback."""
    payload = {
        "resultCode": "0",
        "resultDesc": "The service request is processed successfully.",
        "amount": 71.0,
        "requestId": "404e1aec-19e0-4ce3-973d-bd92e94c8021",
        "resultType": "0",
        "conversationID": "AG_20230426_2010434680d9f5a73766",
        "transactionId": "RDQ01NFT1Q",
        "status": "SUCCESS",
    }
    callback = B2BExpressCheckoutCallback(**payload)
    assert callback.is_successful() is True
    assert callback.status == "SUCCESS"
    assert callback.transactionId == "RDQ01NFT1Q"
    assert callback.amount == 71.0


def test_b2b_express_checkout_fail_callback():
    """Test parsing of a failed B2B Express Checkout callback."""
    payload = {
        "resultCode": "4001",
        "resultDesc": "User cancelled transaction",
        "requestId": "c2a9ba32-9e11-4b90-892c-7bc54944609a",
        "amount": 71.0,
        "paymentReference": "MAndbubry3hi",
    }
    callback = B2BExpressCheckoutCallback(**payload)
    assert callback.resultCode == "4001"
    assert "cancelled" in callback.resultDesc
    assert callback.amount == 71.0
    assert callback.paymentReference == "MAndbubry3hi"


def test_b2b_express_callback_response():
    """Test the response schema for B2B Express Checkout callback."""
    resp = B2BExpressCallbackResponse()
    assert resp.ResultCode == 0
    assert "Callback received successfully" in resp.ResultDesc

def test_b2b_express_callback_resultcode_as_string():
    """Ensure resultCode as a string doesn't cause comparison/type errors in is_successful()."""
    payload = {
        "resultCode": "0",
        "resultDesc": "Processed successfully",
        "amount": 25.0,
        "requestId": "string-resultcode-test",
    }
    callback = B2BExpressCheckoutCallback(**payload)

    # Should treat "0" (string) as success and not raise a TypeError when comparing types.
    assert callback.resultCode == "0"
    assert callback.is_successful() is True
