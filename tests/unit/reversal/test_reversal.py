"""Unit tests for the M-Pesa SDK Reversal functionality.

This module tests the Reversal API client, ensuring it can handle reversal requests,
process responses correctly, and manage error cases.
"""

from unittest.mock import AsyncMock, MagicMock

import pytest

from mpesakit.auth import AsyncTokenManager, TokenManager
from mpesakit.http_client import AsyncHttpClient, HttpClient
from mpesakit.reversal import (
    ReversalRequest,
    ReversalResponse,
    ReversalResultCallback,
    ReversalResultCallbackResponse,
    ReversalTimeoutCallback,
    ReversalTimeoutCallbackResponse,
)
from mpesakit.reversal.reversal import AsyncReversal, Reversal


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
def reversal(mock_http_client, mock_token_manager):
    """Fixture to create a Reversal instance with mocked dependencies."""
    return Reversal(http_client=mock_http_client, token_manager=mock_token_manager)


def valid_reversal_request():
    """Create a valid ReversalRequest for testing."""
    return ReversalRequest(
        Initiator="TestInit610",
        SecurityCredential="encrypted_credential",
        TransactionID="LKXXXX1234",
        Amount=100,
        ReceiverParty=600610,
        ResultURL="https://ip:port/result",
        QueueTimeOutURL="https://ip:port/timeout",
        Remarks="Test",
        Occasion="work",
    )


def test_reverse_request_acknowledged(reversal, mock_http_client):
    """Test that reversal request is acknowledged, not finalized."""
    request = valid_reversal_request()
    response_data = {
        "OriginatorConversationID": "71840-27539181-07",
        "ConversationID": "AG_20210709_12346c8e6f8858d7b70a",
        "ResponseCode": "0",
        "ResponseDescription": "Accept the service request successfully.",
    }
    mock_http_client.post.return_value = response_data

    response = reversal.reverse(request)

    assert isinstance(response, ReversalResponse)

    assert response.is_successful() is True

    assert response.ConversationID == response_data["ConversationID"]
    assert (
        response.OriginatorConversationID == response_data["OriginatorConversationID"]
    )
    assert response.ResponseCode == response_data["ResponseCode"]
    assert response.ResponseDescription == response_data["ResponseDescription"]


def test_reverse_http_error(reversal, mock_http_client):
    """Test handling of HTTP errors during reversal request."""
    request = valid_reversal_request()
    mock_http_client.post.side_effect = Exception("HTTP error")
    with pytest.raises(Exception) as excinfo:
        reversal.reverse(request)
    assert "HTTP error" in str(excinfo.value)


def test_reversal_result_callback_success():
    """Test parsing of a successful reversal result callback."""
    payload = {
        "Result": {
            "ResultType": 0,
            "ResultCode": "21",
            "ResultDesc": "The service request is processed successfully",
            "OriginatorConversationID": "8521-4298025-1",
            "ConversationID": "AG_20181005_00004d7ee675c0c7ee0b",
            "TransactionID": "MJ561H6X5O",
            "ResultParameters": {
                "ResultParameter": [
                    {"Key": "Amount", "Value": "100"},
                    {"Key": "OriginalTransactionID", "Value": "MJ551H6X5D"},
                ]
            },
            "ReferenceData": {
                "ReferenceItem": {
                    "Key": "QueueTimeoutURL",
                    "Value": "https://internalsandbox.safaricom.co.ke/mpesa/reversalresults/v1/submit",
                }
            },
        }
    }
    callback = ReversalResultCallback(**payload)
    assert callback.Result.ResultType == 0
    assert callback.Result.ResultCode == "21"
    assert callback.Result.TransactionID == "MJ561H6X5O"
    assert callback.Result.ResultParameters.ResultParameter[0].Key == "Amount"


def test_reversal_result_callback_response():
    """Test the response schema for result callback."""
    resp = ReversalResultCallbackResponse()
    assert resp.ResultCode == 0
    assert "processed successfully" in resp.ResultDesc


def test_reversal_timeout_callback():
    """Test parsing of a reversal timeout callback."""
    payload = {
        "Result": {
            "ResultType": 1,
            "ResultCode": "1",
            "ResultDesc": "The service request timed out.",
            "OriginatorConversationID": "8521-4298025-1",
            "ConversationID": "AG_20181005_00004d7ee675c0c7ee0b",
        }
    }
    callback = ReversalTimeoutCallback(**payload)
    assert callback.Result.ResultType == 1
    assert callback.Result.ResultCode == "1"
    assert "timed out" in callback.Result.ResultDesc


def test_reversal_timeout_callback_response():
    """Test the response schema for timeout callback."""
    resp = ReversalTimeoutCallbackResponse()
    assert resp.ResultCode == 0
    assert "Timeout notification received" in resp.ResultDesc


def test_reversal_request_identifier_type_is_valid():
    """Test that invalid ReceiverIdentifierType raises ValueError."""
    kwargs = dict(
        Initiator="TestInit610",
        SecurityCredential="encrypted_credential",
        TransactionID="LKXXXX1234",
        Amount=100,
        ReceiverParty=600610,
        ResultURL="https://ip:port/result",
        QueueTimeOutURL="https://ip:port/timeout",
        Remarks="Test",
    )
    request = ReversalRequest(**kwargs)
    assert request.RecieverIdentifierType == "11"


def test_reversal_request_remarks_too_long_raises():
    """Test that Remarks exceeding length raises ValueError."""
    kwargs = dict(
        Initiator="TestInit610",
        SecurityCredential="encrypted_credential",
        TransactionID="LKXXXX1234",
        Amount=100,
        ReceiverParty=600610,
        ResultURL="https://ip:port/result",
        QueueTimeOutURL="https://ip:port/timeout",
        Remarks="A" * 101,
    )
    with pytest.raises(ValueError) as excinfo:
        ReversalRequest(**kwargs)
    assert "Remarks must not exceed 100 characters." in str(excinfo.value)


def test_reversal_request_occasion_too_long_raises():
    """Test that Occasion exceeding length raises ValueError."""
    kwargs = dict(
        Initiator="TestInit610",
        SecurityCredential="encrypted_credential",
        TransactionID="LKXXXX1234",
        Amount=100,
        ReceiverParty=600610,
        ResultURL="https://ip:port/result",
        QueueTimeOutURL="https://ip:port/timeout",
        Remarks="Test",
        Occasion="A" * 101,
    )
    with pytest.raises(ValueError) as excinfo:
        ReversalRequest(**kwargs)
    assert "Occasion must not exceed 100 characters." in str(excinfo.value)


def test_reverse_responsecode_string_no_type_error(reversal, mock_http_client):
    """Ensure is_successful handles ResponseCode as a string without TypeError."""
    request = valid_reversal_request()
    response_data = {
        "OriginatorConversationID": "71840-27539181-07",
        "ConversationID": "AG_20210709_12346c8e6f8858d7b70a",
        "ResponseCode": "0",  # string type
        "ResponseDescription": "Accept the service request successfully.",
    }
    mock_http_client.post.return_value = response_data

    response = reversal.reverse(request)

    assert isinstance(response, ReversalResponse)
    # Calling is_successful should not raise a TypeError when comparing str to int
    assert response.is_successful() is True


def test_reversal_result_callback_success_is_successful():
    """Test is_successful method for a successful reversal result callback."""
    payload = {
        "Result": {
            "ResultType": 0,
            "ResultCode": "0",
            "ResultDesc": "The service request is processed successfully",
            "OriginatorConversationID": "8521-4298025-1",
            "ConversationID": "AG_20181005_00004d7ee675c0c7ee0b",
            "TransactionID": "MJ561H6X5O",
            "ResultParameters": {
                "ResultParameter": [
                    {"Key": "Amount", "Value": "100"},
                ]
            },
            "ReferenceData": {
                "ReferenceItem": {
                    "Key": "QueueTimeoutURL",
                    "Value": "https://internalsandbox.safaricom.co.ke/mpesa/reversalresults/v1/submit",
                }
            },
        }
    }
    callback = ReversalResultCallback(**payload)
    assert callback.is_successful() is True


def test_reversal_result_callback_failure_is_successful():
    """Test is_successful method for a failure reversal result callback."""
    payload = {
        "Result": {
            "ResultType": 1,
            "ResultCode": "1",
            "ResultDesc": "The service request failed.",
            "OriginatorConversationID": "8521-4298025-1",
            "ConversationID": "AG_20181005_00004d7ee675c0c7ee0b",
            "TransactionID": "MJ561H6X5O",
            "ResultParameters": {
                "ResultParameter": [
                    {"Key": "Amount", "Value": "100"},
                ]
            },
            "ReferenceData": {
                "ReferenceItem": {
                    "Key": "QueueTimeoutURL",
                    "Value": "https://internalsandbox.safaricom.co.ke/mpesa/reversalresults/v1/submit",
                }
            },
        }
    }
    callback = ReversalResultCallback(**payload)
    assert callback.is_successful() is False


def test_reversal_result_callback_success_code_is_successful():
    """Test is_successful method with a success code as a string."""
    payload = {
        "Result": {
            "ResultType": 0,
            "ResultCode": "00000000",
            "ResultDesc": "The service request is processed successfully",
            "OriginatorConversationID": "8521-4298025-1",
            "ConversationID": "AG_20181005_00004d7ee675c0c7ee0b",
            "TransactionID": "MJ561H6X5O",
        }
    }
    callback = ReversalResultCallback(**payload)
    assert callback.is_successful() is True


def test_reversal_result_callback_failure_code_is_successful():
    """Test is_successful method with a failure code."""
    payload = {
        "Result": {
            "ResultType": 1,
            "ResultCode": "12345",
            "ResultDesc": "The service request failed.",
            "OriginatorConversationID": "8521-4298025-1",
            "ConversationID": "AG_20181005_00004d7ee675c0c7ee0b",
            "TransactionID": "MJ561H6X5O",
        }
    }
    callback = ReversalResultCallback(**payload)
    assert callback.is_successful() is False


@pytest.fixture
def mock_async_token_manager():
    """Mock AsyncTokenManager to return a fixed token."""
    mock = AsyncMock(spec=AsyncTokenManager)
    mock.get_token.return_value = "test_token_async"
    return mock


@pytest.fixture
def mock_async_http_client():
    """Mock AsyncHttpClient to simulate async HTTP requests."""
    return AsyncMock(spec=AsyncHttpClient)


@pytest.fixture
def async_reversal(mock_async_http_client, mock_async_token_manager):
    """Fixture to create an AsyncReversal instance with mocked dependencies."""
    return AsyncReversal(
        http_client=mock_async_http_client, token_manager=mock_async_token_manager
    )


@pytest.mark.asyncio
async def test_async_reverse_request_acknowledged(
    async_reversal, mock_async_http_client
):
    """Test that async reversal request is acknowledged."""
    request = valid_reversal_request()
    response_data = {
        "OriginatorConversationID": "71840-27539181-07",
        "ConversationID": "AG_20210709_12346c8e6f8858d7b70a",
        "ResponseCode": "0",
        "ResponseDescription": "Accept the service request successfully.",
    }
    mock_async_http_client.post.return_value = response_data

    response = await async_reversal.reverse(request)

    assert isinstance(response, ReversalResponse)
    assert response.is_successful() is True
    assert response.ConversationID == response_data["ConversationID"]


@pytest.mark.asyncio
async def test_async_reverse_http_error(async_reversal, mock_async_http_client):
    """Test handling of HTTP errors during async reversal request."""
    request = valid_reversal_request()
    mock_async_http_client.post.side_effect = Exception("Async HTTP error")
    with pytest.raises(Exception) as excinfo:
        await async_reversal.reverse(request)
    assert "Async HTTP error" in str(excinfo.value)


@pytest.mark.asyncio
async def test_async_reverse_token_manager_called(
    async_reversal, mock_async_token_manager, mock_async_http_client
):
    """Test that async token manager's get_token is called."""
    request = valid_reversal_request()
    response_data = {
        "OriginatorConversationID": "71840-27539181-07",
        "ConversationID": "AG_20210709_12346c8e6f8858d7b70a",
        "ResponseCode": "0",
        "ResponseDescription": "Accept the service request successfully.",
    }
    mock_async_http_client.post.return_value = response_data

    await async_reversal.reverse(request)

    mock_async_token_manager.get_token.assert_awaited_once()


@pytest.mark.asyncio
async def test_async_reverse_http_client_post_called(
    async_reversal, mock_async_http_client
):
    """Test that async HTTP client's post method is called with correct parameters."""
    request = valid_reversal_request()
    response_data = {
        "OriginatorConversationID": "71840-27539181-07",
        "ConversationID": "AG_20210709_12346c8e6f8858d7b70a",
        "ResponseCode": "0",
        "ResponseDescription": "Accept the service request successfully.",
    }
    mock_async_http_client.post.return_value = response_data

    await async_reversal.reverse(request)

    assert mock_async_http_client.post.called
    call_args = mock_async_http_client.post.call_args
    assert call_args[0][0] == "/mpesa/reversal/v1/request"
    assert "Authorization" in call_args[1]["headers"]
    assert call_args[1]["headers"]["Authorization"] == "Bearer test_token_async"
    assert call_args[1]["headers"]["Content-Type"] == "application/json"


@pytest.mark.asyncio
async def test_async_reverse_responsecode_string_no_type_error(
    async_reversal, mock_async_http_client
):
    """Ensure async is_successful handles ResponseCode as a string without TypeError."""
    request = valid_reversal_request()
    response_data = {
        "OriginatorConversationID": "71840-27539181-07",
        "ConversationID": "AG_20210709_12346c8e6f8858d7b70a",
        "ResponseCode": "0",
        "ResponseDescription": "Accept the service request successfully.",
    }
    mock_async_http_client.post.return_value = response_data

    response = await async_reversal.reverse(request)

    assert isinstance(response, ReversalResponse)
    assert response.is_successful() is True
