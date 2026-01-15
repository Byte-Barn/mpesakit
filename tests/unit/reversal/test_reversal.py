"""Unit tests for the M-Pesa SDK Reversal functionality.

This module tests the Reversal API client, ensuring it can handle reversal requests,
process responses correctly, and manage error cases.
"""

import pytest
from unittest.mock import MagicMock
from mpesakit.auth import TokenManager
from mpesakit.http_client import HttpClient
from mpesakit.reversal.reversal import Reversal

from mpesakit.reversal import (
    ReversalRequest,
    ReversalResponse,
    ReversalResultCallback,
    ReversalResultCallbackResponse,
    ReversalTimeoutCallback,
    ReversalTimeoutCallbackResponse,
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
