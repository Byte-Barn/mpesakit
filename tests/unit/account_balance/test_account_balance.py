"""Unit tests for the M-Pesa Account Balance API interactions.

This module tests the AccountBalance class and its methods for querying account balance.
"""

from unittest.mock import MagicMock

import pytest

from mpesakit.account_balance import (
    AccountBalance,
    AccountBalanceIdentifierType,
    AccountBalanceRequest,
    AccountBalanceResponse,
    AccountBalanceResultCallback,
    AccountBalanceTimeoutCallback,
    AsyncAccountBalance,
)
from mpesakit.auth import AsyncTokenManager, TokenManager
from mpesakit.http_client import AsyncHttpClient, HttpClient


@pytest.fixture
def mock_token_manager():
    """Mock TokenManager for testing."""
    mock = MagicMock(spec=TokenManager)
    mock.get_token.return_value = "test_token"
    return mock


@pytest.fixture
def mock_http_client():
    """Mock HttpClient for testing."""
    return MagicMock(spec=HttpClient)


@pytest.fixture
def account_balance(mock_http_client, mock_token_manager):
    """Fixture to create an AccountBalance instance with mocked dependencies."""
    return AccountBalance(
        http_client=mock_http_client, token_manager=mock_token_manager
    )


def valid_account_balance_request():
    """Create a valid AccountBalanceRequest for testing."""
    return AccountBalanceRequest(
        Initiator="testapi",
        SecurityCredential="encrypted_credential",
        CommandID="AccountBalance",
        PartyA=600999,
        IdentifierType=4,
        Remarks="Balance check",
        QueueTimeOutURL="https://example.com/timeout",
        ResultURL="https://example.com/result",
    )


def test_query_returns_acknowledgement(account_balance, mock_http_client):
    """Test that query returns only the acknowledgement response, not the account balance."""
    request = valid_account_balance_request()
    response_data = {
        "ConversationID": "AG_20170717_00006c6f7f5b8b6b1a62",
        "OriginatorConversationID": "12345-67890-1",
        "ResponseCode": "0",
        "ResponseDescription": "Accept the service request successfully.",
    }
    mock_http_client.post.return_value = response_data

    response = account_balance.query(request)

    assert response.is_successful() is True

    assert isinstance(response, AccountBalanceResponse)
    assert response.ConversationID == response_data["ConversationID"]
    assert (
        response.OriginatorConversationID == response_data["OriginatorConversationID"]
    )
    assert response.ResponseCode == response_data["ResponseCode"]
    assert response.ResponseDescription == response_data["ResponseDescription"]

    mock_http_client.post.assert_called_once()
    args, kwargs = mock_http_client.post.call_args
    assert args[0] == "/mpesa/accountbalance/v1/query"
    assert kwargs["headers"]["Authorization"] == "Bearer test_token"
    assert kwargs["headers"]["Content-Type"] == "application/json"


def test_callback_result_parsing():
    """Test parsing of AccountBalanceResultCallback with actual account balance data."""
    callback_data = {
        "Result": {
            "ResultType": 0,
            "ResultCode": 0,
            "ResultDesc": "The service request is processed successfully",
            "OriginatorConversationID": "16917-22577599-3",
            "ConversationID": "AG_20200206_00005e091a8ec6b9eac5",
            "TransactionID": "OA90000000",
            "ResultParameter": {
                "ResultParameters": [
                    {
                        "Key": "AccountBalance",
                        "Value": (
                            "Working Account|KES|700000.00|700000.00|0.00|0.00&"
                            "Float Account|KES|0.00|0.00|0.00|0.00&"
                            "Utility Account|KES|228037.00|228037.00|0.00|0.00&"
                            "Charges Paid Account|KES|-1540.00|-1540.00|0.00|0.00&"
                            "Organization Settlement Account|KES|0.00|0.00|0.00|0.00"
                        ),
                    },
                    {
                        "Key": "BOCompletedTime",
                        "Value": "20200109125710",
                    },
                ]
            },
            "ReferenceData": {
                "ReferenceItem": {
                    "Key": "QueueTimeoutURL",
                    "Value": "https://internalsandbox.safaricom.co.ke/mpesa/abresults/v1/submit",
                }
            },
        }
    }

    callback = AccountBalanceResultCallback(**callback_data)
    assert callback.Result.ResultType == 0
    assert callback.Result.ResultCode == 0
    assert callback.Result.ResultDesc == "The service request is processed successfully"
    assert callback.Result.TransactionID == "OA90000000"
    assert callback.Result.ResultParameter is not None
    params = callback.Result.ResultParameter.ResultParameters
    account_balance_param = next((p for p in params if p.Key == "AccountBalance"), None)
    assert account_balance_param is not None
    assert "Working Account|KES|700000.00" in account_balance_param.Value


def test_account_balance_request_identifier_type_validation():
    """Test that AccountBalanceRequest raises ValueError for invalid IdentifierType."""
    valid_data = dict(
        Initiator="testapi",
        SecurityCredential="encrypted_credential",
        CommandID="AccountBalance",
        PartyA=600999,
        IdentifierType=AccountBalanceIdentifierType.MSISDN.value,
        Remarks="Valid remarks",
        QueueTimeOutURL="https://example.com/timeout",
        ResultURL="https://example.com/result",
    )
    # Should not raise
    AccountBalanceRequest(**valid_data)

    invalid_data = valid_data.copy()
    invalid_data["IdentifierType"] = 999  # Invalid type

    with pytest.raises(ValueError, match="IdentifierType must be one of"):
        AccountBalanceRequest(**invalid_data)


def test_account_balance_request_remarks_length_validation():
    """Test that AccountBalanceRequest raises ValueError for remarks exceeding 100 chars."""
    long_remarks = "x" * 101
    data = dict(
        Initiator="testapi",
        SecurityCredential="encrypted_credential",
        CommandID="AccountBalance",
        PartyA=600999,
        IdentifierType=4,
        Remarks=long_remarks,
        QueueTimeOutURL="https://example.com/timeout",
        ResultURL="https://example.com/result",
    )
    with pytest.raises(ValueError, match="Remarks must not exceed 100 characters."):
        AccountBalanceRequest(**data)


def test_timeout_callback_parsing():
    """Test parsing of AccountBalanceTimeoutCallback."""
    timeout_data = {
        "Result": {
            "ResultType": 1,
            "ResultCode": 1,
            "ResultDesc": "The service request timed out.",
            "OriginatorConversationID": "16917-22577599-3",
            "ConversationID": "AG_20200206_00005e091a8ec6b9eac5",
        }
    }

    callback = AccountBalanceTimeoutCallback(**timeout_data)
    assert callback.Result.ResultType == 1
    assert callback.Result.ResultCode == 1
    assert callback.Result.ResultDesc == "The service request timed out."
    assert callback.Result.OriginatorConversationID == "16917-22577599-3"
    assert callback.Result.ConversationID == "AG_20200206_00005e091a8ec6b9eac5"


def test_query_handles_string_response_code(account_balance, mock_http_client):
    """Ensure that ResponseCode as a string does not raise TypeError when checking is_successful."""
    request = valid_account_balance_request()
    # ResponseCode as string "0" should be treated as success
    response_data_success = {
        "ConversationID": "AG_20170717_00006c6f7f5b8b6b1a62",
        "OriginatorConversationID": "12345-67890-1",
        "ResponseCode": "0",
        "ResponseDescription": "Accept the service request successfully.",
    }
    mock_http_client.post.return_value = response_data_success

    response = account_balance.query(request)
    # should not raise and should consider "0" a success
    assert response.is_successful() is True
    assert response.ResponseCode == "0"

    # Now simulate a non-success ResponseCode as string "1"
    response_data_fail = response_data_success.copy()
    response_data_fail["ResponseCode"] = "1"
    response_data_fail["ResponseDescription"] = "Failure."
    mock_http_client.post.return_value = response_data_fail

    response_fail = account_balance.query(request)
    # should not raise and should consider "1" a failure
    assert response_fail.is_successful() is False
    assert response_fail.ResponseCode == "1"


@pytest.fixture
def mock_async_token_manager():
    """Mock AsyncTokenManager for testing."""
    mock = MagicMock(spec=AsyncTokenManager)
    mock.get_token.return_value = "test_async_token"
    return mock


@pytest.fixture
def mock_async_http_client():
    """Mock AsyncHttpClient for testing."""
    return MagicMock(spec=AsyncHttpClient)


@pytest.fixture
def async_account_balance(mock_async_http_client, mock_async_token_manager):
    """Fixture to create an AsyncAccountBalance instance with mocked dependencies."""
    return AsyncAccountBalance(
        http_client=mock_async_http_client, token_manager=mock_async_token_manager
    )


@pytest.mark.asyncio
async def test_async_query_returns_acknowledgement(
    async_account_balance, mock_async_http_client
):
    """Test that async query returns only the acknowledgement response, not the account balance."""
    request = valid_account_balance_request()
    response_data = {
        "ConversationID": "AG_20170717_00006c6f7f5b8b6b1a62",
        "OriginatorConversationID": "12345-67890-1",
        "ResponseCode": "0",
        "ResponseDescription": "Accept the service request successfully.",
    }
    mock_async_http_client.post.return_value = response_data

    response = await async_account_balance.query(request)

    assert response.is_successful() is True
    assert isinstance(response, AccountBalanceResponse)
    assert response.ConversationID == response_data["ConversationID"]
    assert (
        response.OriginatorConversationID == response_data["OriginatorConversationID"]
    )
    assert response.ResponseCode == response_data["ResponseCode"]
    assert response.ResponseDescription == response_data["ResponseDescription"]

    mock_async_http_client.post.assert_called_once()
    args, kwargs = mock_async_http_client.post.call_args
    assert args[0] == "/mpesa/accountbalance/v1/query"
    assert kwargs["headers"]["Authorization"] == "Bearer test_async_token"
    assert kwargs["headers"]["Content-Type"] == "application/json"


@pytest.mark.asyncio
async def test_async_query_handles_string_response_code(
    async_account_balance, mock_async_http_client
):
    """Ensure that ResponseCode as a string does not raise TypeError when checking is_successful in async."""
    request = valid_account_balance_request()
    response_data_success = {
        "ConversationID": "AG_20170717_00006c6f7f5b8b6b1a62",
        "OriginatorConversationID": "12345-67890-1",
        "ResponseCode": "0",
        "ResponseDescription": "Accept the service request successfully.",
    }
    mock_async_http_client.post.return_value = response_data_success

    response = await async_account_balance.query(request)
    assert response.is_successful() is True
    assert response.ResponseCode == "0"

    response_data_fail = response_data_success.copy()
    response_data_fail["ResponseCode"] = "1"
    response_data_fail["ResponseDescription"] = "Failure."
    mock_async_http_client.post.return_value = response_data_fail

    response_fail = await async_account_balance.query(request)
    assert response_fail.is_successful() is False
    assert response_fail.ResponseCode == "1"
