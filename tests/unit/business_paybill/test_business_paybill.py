"""Unit tests for the M-Pesa SDK Business PayBill functionality.

This module tests the Business PayBill API client, ensuring it can handle payment requests,
process responses correctly, and manage error cases.
"""

from unittest.mock import AsyncMock, MagicMock

import pytest

from mpesakit.auth import AsyncTokenManager, TokenManager
from mpesakit.business_paybill import (
    AsyncBusinessPayBill,
    BusinessPayBill,
    BusinessPayBillRequest,
    BusinessPayBillResponse,
    BusinessPayBillResultCallback,
    BusinessPayBillResultCallbackResponse,
    BusinessPayBillTimeoutCallback,
    BusinessPayBillTimeoutCallbackResponse,
)
from mpesakit.http_client import AsyncHttpClient, HttpClient


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
def business_paybill(mock_http_client, mock_token_manager):
    """Fixture to create a BusinessPayBill instance with mocked dependencies."""
    return BusinessPayBill(
        http_client=mock_http_client, token_manager=mock_token_manager
    )


def valid_business_paybill_request():
    """Create a valid BusinessPayBillRequest for testing."""
    return BusinessPayBillRequest(
        Initiator="API_Username",
        SecurityCredential="encrypted_credential",
        Amount=239,
        PartyA=123456,
        PartyB=654321,
        AccountReference="353353",
        Remarks="OK",
        QueueTimeOutURL="https://mydomain.com/b2b/paybill/queue/",
        ResultURL="https://mydomain.com/b2b/paybill/result/",
    )


def test_paybill_request_acknowledged(business_paybill, mock_http_client):
    """Test that paybill request is acknowledged, not finalized."""
    request = valid_business_paybill_request()
    response_data = {
        "OriginatorConversationID": "5118-111210482-1",
        "ConversationID": "AG_20230420_2010759fd5662ef6d054",
        "ResponseCode": "0",
        "ResponseDescription": "Accept the service request successfully.",
    }
    mock_http_client.post.return_value = response_data

    response = business_paybill.paybill(request)

    assert isinstance(response, BusinessPayBillResponse)
    assert response.is_successful() is True
    assert response.ConversationID == response_data["ConversationID"]
    assert (
        response.OriginatorConversationID == response_data["OriginatorConversationID"]
    )
    assert response.ResponseCode == response_data["ResponseCode"]
    assert response.ResponseDescription == response_data["ResponseDescription"]


def test_paybill_http_error(business_paybill, mock_http_client):
    """Test handling of HTTP errors during paybill request."""
    request = valid_business_paybill_request()
    mock_http_client.post.side_effect = Exception("HTTP error")
    with pytest.raises(Exception) as excinfo:
        business_paybill.paybill(request)
    assert "HTTP error" in str(excinfo.value)


def test_business_paybill_result_callback_success():
    """Test parsing of a successful business paybill result callback."""
    payload = {
        "Result": {
            "ResultType": 0,
            "ResultCode": 0,
            "ResultDesc": "The service request is processed successfully",
            "OriginatorConversationID": "626f6ddf-ab37-4650-b882-b1de92ec9aa4",
            "ConversationID": "AG_20181005_00004d7ee675c0c7ee0b",
            "TransactionID": "QKA81LK5CY",
            "ResultParameters": {
                "ResultParameter": [
                    {"Key": "Amount", "Value": "190.00"},
                    {"Key": "Currency", "Value": "KES"},
                ]
            },
            "ReferenceData": {
                "ReferenceItem": [
                    {"Key": "BillReferenceNumber", "Value": "19008"},
                ]
            },
        }
    }
    callback = BusinessPayBillResultCallback(**payload)
    assert callback.is_successful() is True
    assert callback.Result.TransactionID == "QKA81LK5CY"
    assert callback.Result.ResultParameters.ResultParameter[0].Key == "Amount"


def test_business_paybill_result_callback_response():
    """Test the response schema for result callback."""
    resp = BusinessPayBillResultCallbackResponse()
    assert resp.ResultCode == 0
    assert "Callback received successfully" in resp.ResultDesc


def test_business_paybill_timeout_callback():
    """Test parsing of a business paybill timeout callback."""
    payload = {
        "Result": {
            "ResultType": 1,
            "ResultCode": 1,
            "ResultDesc": "The service request timed out.",
            "OriginatorConversationID": "8521-4298025-1",
            "ConversationID": "AG_20181005_00004d7ee675c0c7ee0b",
        }
    }
    callback = BusinessPayBillTimeoutCallback(**payload)
    assert callback.Result.ResultType == 1
    assert callback.Result.ResultCode == 1
    assert "timed out" in callback.Result.ResultDesc


def test_business_paybill_timeout_callback_response():
    """Test the response schema for timeout callback."""
    resp = BusinessPayBillTimeoutCallbackResponse()
    assert resp.ResultCode == 0
    assert "Timeout notification received" in resp.ResultDesc


def test_business_paybill_result_callback_with_string_result_code():
    """Ensure is_successful handles ResultCode provided as a string without raising TypeError."""
    payload = {
        "Result": {
            "ResultType": 0,
            "ResultCode": "0",  # string instead of int
            "ResultDesc": "The service request is processed successfully",
            "OriginatorConversationID": "626f6ddf-ab37-4650-b882-b1de92ec9aa4",
            "ConversationID": "AG_20181005_00004d7ee675c0c7ee0b",
            "TransactionID": "QKA81LK5CY",
            "ResultParameters": {
                "ResultParameter": [
                    {"Key": "Amount", "Value": "190.00"},
                    {"Key": "Currency", "Value": "KES"},
                ]
            },
        }
    }

    # This should not raise a TypeError when comparing string to int inside is_successful
    callback = BusinessPayBillResultCallback(**payload)
    assert callback.is_successful() is True
    assert callback.Result.TransactionID == "QKA81LK5CY"


@pytest.fixture
def mock_async_token_manager():
    """Mock AsyncTokenManager to return a fixed token."""
    mock = AsyncMock(spec=AsyncTokenManager)
    mock.get_token.return_value = "test_async_token"
    return mock


@pytest.fixture
def mock_async_http_client():
    """Mock AsyncHttpClient to simulate async HTTP requests."""
    return AsyncMock(spec=AsyncHttpClient)


@pytest.fixture
def async_business_paybill(mock_async_http_client, mock_async_token_manager):
    """Fixture to create an AsyncBusinessPayBill instance with mocked dependencies."""
    return AsyncBusinessPayBill(
        http_client=mock_async_http_client, token_manager=mock_async_token_manager
    )


@pytest.mark.asyncio
async def test_async_paybill_request_acknowledged(
    async_business_paybill, mock_async_http_client
):
    """Test that async paybill request is acknowledged successfully."""
    request = valid_business_paybill_request()
    response_data = {
        "OriginatorConversationID": "5118-111210482-1",
        "ConversationID": "AG_20230420_2010759fd5662ef6d054",
        "ResponseCode": "0",
        "ResponseDescription": "Accept the service request successfully.",
    }
    mock_async_http_client.post.return_value = response_data

    response = await async_business_paybill.paybill(request)

    assert isinstance(response, BusinessPayBillResponse)
    assert response.is_successful() is True
    assert response.ConversationID == response_data["ConversationID"]
    assert (
        response.OriginatorConversationID == response_data["OriginatorConversationID"]
    )


@pytest.mark.asyncio
async def test_async_paybill_http_error(async_business_paybill, mock_async_http_client):
    """Test handling of HTTP errors during async paybill request."""
    request = valid_business_paybill_request()
    mock_async_http_client.post.side_effect = Exception("Async HTTP error")
    with pytest.raises(Exception) as excinfo:
        await async_business_paybill.paybill(request)
    assert "Async HTTP error" in str(excinfo.value)


@pytest.mark.asyncio
async def test_async_paybill_token_retrieval(
    async_business_paybill, mock_async_token_manager, mock_async_http_client
):
    """Test that async paybill correctly retrieves and uses the token."""
    request = valid_business_paybill_request()
    response_data = {
        "OriginatorConversationID": "5118-111210482-1",
        "ConversationID": "AG_20230420_2010759fd5662ef6d054",
        "ResponseCode": "0",
        "ResponseDescription": "Accept the service request successfully.",
    }
    mock_async_http_client.post.return_value = response_data

    await async_business_paybill.paybill(request)

    mock_async_token_manager.get_token.assert_awaited_once()
    mock_async_http_client.post.assert_awaited_once()
    call_args = mock_async_http_client.post.call_args
    assert "Authorization" in call_args.kwargs["headers"]
    assert "Bearer test_async_token" in call_args.kwargs["headers"]["Authorization"]
