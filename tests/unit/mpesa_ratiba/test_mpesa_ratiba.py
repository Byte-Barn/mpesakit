"""Unit tests for the M-Pesa Ratiba (Standing Order) API client.

This module tests the Standing Order API client, ensuring it can initiate standing order requests,
process responses correctly, and manage callback/error cases.
"""

from unittest.mock import AsyncMock, MagicMock

import pytest

from mpesakit.auth import AsyncTokenManager, TokenManager
from mpesakit.http_client import AsyncHttpClient, HttpClient
from mpesakit.mpesa_ratiba import (
    AsyncMpesaRatiba,
    FrequencyEnum,
    MpesaRatiba,
    ReceiverPartyIdentifierTypeEnum,
    StandingOrderCallback,
    StandingOrderCallbackResponse,
    StandingOrderRequest,
    StandingOrderResponse,
    TransactionTypeEnum,
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
def mpesa_ratiba(mock_http_client, mock_token_manager):
    """Fixture to create a MpesaRatiba instance with mocked dependencies."""
    return MpesaRatiba(http_client=mock_http_client, token_manager=mock_token_manager)


def valid_standing_order_request():
    """Create a valid StandingOrderRequest for testing."""
    return StandingOrderRequest(
        StandingOrderName="Test Standing Order",
        StartDate="20240905",
        EndDate="20250905",
        BusinessShortCode="174379",
        TransactionType=TransactionTypeEnum.STANDING_ORDER_CUSTOMER_PAY_BILL,
        ReceiverPartyIdentifierType=ReceiverPartyIdentifierTypeEnum.MERCHANT_TILL,
        Amount="4500",
        PartyA="254708374149",
        CallBackURL="https://mydomain.com/pat",
        AccountReference="Test",
        TransactionDesc="Electric Bike",
        Frequency=FrequencyEnum.DAILY,
    )


def test_create_standing_order_success(mpesa_ratiba, mock_http_client):
    """Test that standing order request is acknowledged and successful."""
    request = valid_standing_order_request()
    response_data = {
        "ResponseHeader": {
            "responseRefID": "4dd9b5d9-d738-42ba-9326-2cc99e966000",
            "responseCode": "200",
            "responseDescription": "Request accepted for processing",
            "ResultDesc": "The service request is processed successfully.",
        },
        "ResponseBody": {
            "responseDescription": "Request accepted for processing",
            "responseCode": "200",
        },
    }
    mock_http_client.post.return_value = response_data

    response = mpesa_ratiba.create_standing_order(request)

    assert isinstance(response, StandingOrderResponse)
    assert response.is_successful() is True
    assert (
        response.ResponseHeader.responseCode
        == response_data["ResponseHeader"]["responseCode"]
    )
    assert (
        response.ResponseHeader.responseDescription
        == response_data["ResponseHeader"]["responseDescription"]
    )


def test_create_standing_order_http_error(mpesa_ratiba, mock_http_client):
    """Test handling of HTTP errors during standing order request."""
    request = valid_standing_order_request()
    mock_http_client.post.side_effect = Exception("HTTP error")
    with pytest.raises(Exception) as excinfo:
        mpesa_ratiba.create_standing_order(request)
    assert "HTTP error" in str(excinfo.value)


def test_standing_order_success_callback():
    """Test parsing of a successful Standing Order callback."""
    payload = {
        "ResponseHeader": {
            "responseRefID": "0acc0239-20fa-4a52-8b9d-9bd64c0465c3",
            "requestRefID": "0acc0239-20fa-4a52-8b9d-9bd64c0465c3",
            "responseCode": "0",
            "responseDescription": "The service request is processed successfully",
        },
        "ResponseBody": {
            "ResponseData": [
                {"Name": "TransactionID", "Value": "SC8F2IQMH5"},
                {"Name": "responseCode", "Value": "0"},
                {"Name": "Status", "Value": "OKAY"},
                {"Name": "Msisdn", "Value": "254******867"},
            ]
        },
    }
    callback = StandingOrderCallback(**payload)
    assert callback.is_successful() is True
    assert callback.ResponseHeader.responseCode == "0"
    assert any(
        item.Name == "TransactionID" and item.Value == "SC8F2IQMH5"
        for item in callback.ResponseBody.ResponseData
    )
    assert any(
        item.Name == "Status" and item.Value == "OKAY"
        for item in callback.ResponseBody.ResponseData
    )


def test_standing_order_fail_callback():
    """Test parsing of a failed Standing Order callback."""
    payload = {
        "ResponseHeader": {
            "responseRefID": "0acc0239-20fa-4a52-8b9d-9bd64c0465c3",
            "requestRefID": "0acc0239-20fa-4a52-8b9d-9bd64c0465c3",
            "responseCode": "1",
            "responseDescription": "User cancelled transaction",
        },
        "ResponseBody": {
            "ResponseData": [
                {"Name": "TransactionID", "Value": "TX123456"},
                {"Name": "responseCode", "Value": "1"},
                {"Name": "Status", "Value": "CANCELLED"},
            ]
        },
    }
    callback = StandingOrderCallback(**payload)
    assert callback.is_successful() is False
    assert callback.ResponseHeader.responseCode == "1"
    assert any(
        item.Name == "Status" and "CANCELLED" in item.Value
        for item in callback.ResponseBody.ResponseData
    )
    assert any(
        item.Name == "TransactionID" and item.Value == "TX123456"
        for item in callback.ResponseBody.ResponseData
    )


def test_standing_order_callback_response():
    """Test the response schema for Standing Order callback."""
    resp = StandingOrderCallbackResponse()
    assert resp.ResultCode == "0"
    assert "processed successfully" in resp.ResultDesc


def test_standing_order_request_invalid_date_format():
    """Test StandingOrderRequest raises ValueError for invalid date format."""
    with pytest.raises(ValueError) as excinfo:
        StandingOrderRequest(
            StandingOrderName="Test",
            StartDate="202423305",  # Invalid date
            EndDate="20250905",
            BusinessShortCode="174379",
            TransactionType=TransactionTypeEnum.STANDING_ORDER_CUSTOMER_PAY_BILL,
            ReceiverPartyIdentifierType=ReceiverPartyIdentifierTypeEnum.MERCHANT_TILL,
            Amount="4500",
            PartyA="254708374149",
            CallBackURL="https://mydomain.com/pat",
            AccountReference="Test",
            TransactionDesc="Electric Bike",
            Frequency=FrequencyEnum.DAILY,
        )
    assert "Date must be in 'yyyymmdd' format" in str(excinfo.value)


def test_standing_order_request_invalid_date_value():
    """Test StandingOrderRequest raises ValueError for invalid date value."""
    with pytest.raises(ValueError) as excinfo:
        StandingOrderRequest(
            StandingOrderName="Test",
            StartDate="20241305",  # Invalid month
            EndDate="20250905",
            BusinessShortCode="174379",
            TransactionType=TransactionTypeEnum.STANDING_ORDER_CUSTOMER_PAY_BILL,
            ReceiverPartyIdentifierType=ReceiverPartyIdentifierTypeEnum.MERCHANT_TILL,
            Amount="4500",
            PartyA="254708374149",
            CallBackURL="https://mydomain.com/pat",
            AccountReference="Test",
            TransactionDesc="Electric Bike",
            Frequency=FrequencyEnum.WEEKLY,
        )
    assert "Date must be in 'yyyymmdd' format" in str(excinfo.value)


def test_standing_order_request_other_date_value():
    """Test StandingOrderRequest raises ValueError for invalid date value."""
    request = StandingOrderRequest(
        StandingOrderName="Test",
        StartDate="2024-12-05",  # With separators
        EndDate="20250905",
        BusinessShortCode="174379",
        TransactionType=TransactionTypeEnum.STANDING_ORDER_CUSTOMER_PAY_BILL,
        ReceiverPartyIdentifierType=ReceiverPartyIdentifierTypeEnum.MERCHANT_TILL,
        Amount="4500",
        PartyA="254708374149",
        CallBackURL="https://mydomain.com/pat",
        AccountReference="Test",
        TransactionDesc="Electric Bike",
        Frequency=FrequencyEnum.MONTHLY,
    )
    assert request.StartDate == "20241205"  # Should normalize to yyyymmdd format


def test_invalid_phone_number():
    """Test that invalid phone numbers raise ValueError."""
    with pytest.raises(ValueError) as excinfo:
        StandingOrderRequest(
            StandingOrderName="Test",
            StartDate="20240905",
            EndDate="20250905",
            BusinessShortCode="174379",
            TransactionType=TransactionTypeEnum.STANDING_ORDER_CUSTOMER_PAY_BILL,
            ReceiverPartyIdentifierType=ReceiverPartyIdentifierTypeEnum.MERCHANT_TILL,
            Amount="4500",
            PartyA="123456789",  # Invalid phone number
            CallBackURL="https://mydomain.com/pat",
            AccountReference="Test",
            TransactionDesc="Electric Bike",
            Frequency=FrequencyEnum.MONTHLY,
        )
    assert "Invalid PartyA phone number" in str(excinfo.value)


def test_callback_resultcode_as_string_handled_gracefully():
    """Ensure StandingOrderCallback.is_successful() handles responseCode as a string without TypeError."""
    payload = {
        "ResponseHeader": {
            "responseRefID": "0acc0239-20fa-4a52-8b9d-9bd64c0465c3",
            "requestRefID": "0acc0239-20fa-4a52-8b9d-9bd64c0465c3",
            "responseCode": "0",
            "responseDescription": "The service request is processed successfully",
        },
        "ResponseBody": {
            "ResponseData": [
                {"Name": "TransactionID", "Value": "SC8F2IQMH5"},
                {"Name": "responseCode", "Value": "0"},
                {"Name": "Status", "Value": "OKAY"},
                {"Name": "Msisdn", "Value": "254******867"},
            ]
        },
    }
    callback = StandingOrderCallback(**payload)
    try:
        result = callback.is_successful()
    except TypeError as exc:
        pytest.fail(
            f"is_successful raised TypeError when responseCode is a string: {exc}"
        )
    assert result is True


@pytest.fixture
def mock_async_token_manager():
    """Mock AsyncTokenManager to return a fixed token."""
    mock = AsyncMock(spec=AsyncTokenManager)
    mock.get_token.return_value = "test_token"
    return mock


@pytest.fixture
def mock_async_http_client():
    """Mock AsyncHttpClient to simulate async HTTP requests."""
    return AsyncMock(spec=AsyncHttpClient)


@pytest.fixture
def async_mpesa_ratiba(mock_async_http_client, mock_async_token_manager):
    """Fixture to create an AsyncMpesaRatiba instance with mocked dependencies."""
    return AsyncMpesaRatiba(
        http_client=mock_async_http_client, token_manager=mock_async_token_manager
    )


@pytest.mark.asyncio
async def test_create_standing_order_success_async(
    async_mpesa_ratiba, mock_async_http_client
):
    """Test that async standing order request is acknowledged and successful."""
    request = valid_standing_order_request()
    response_data = {
        "ResponseHeader": {
            "responseRefID": "4dd9b5d9-d738-42ba-9326-2cc99e966000",
            "responseCode": "200",
            "responseDescription": "Request accepted for processing",
            "ResultDesc": "The service request is processed successfully.",
        },
        "ResponseBody": {
            "responseDescription": "Request accepted for processing",
            "responseCode": "200",
        },
    }
    mock_async_http_client.post.return_value = response_data

    response = await async_mpesa_ratiba.create_standing_order(request)

    assert isinstance(response, StandingOrderResponse)
    assert response.is_successful() is True
    assert (
        response.ResponseHeader.responseCode
        == response_data["ResponseHeader"]["responseCode"]
    )
    assert (
        response.ResponseHeader.responseDescription
        == response_data["ResponseHeader"]["responseDescription"]
    )


@pytest.mark.asyncio
async def test_create_standing_order_http_error_async(
    async_mpesa_ratiba, mock_async_http_client
):
    """Test handling of HTTP errors during async standing order request."""
    request = valid_standing_order_request()
    mock_async_http_client.post.side_effect = Exception("HTTP error")
    with pytest.raises(Exception) as excinfo:
        await async_mpesa_ratiba.create_standing_order(request)
    assert "HTTP error" in str(excinfo.value)
