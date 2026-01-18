"""Unit tests for the M-Pesa SDK Tax Remittance functionality.

This module tests the Tax Remittance API client, ensuring it can handle remittance requests,
process responses correctly, and manage error cases.
"""

from unittest.mock import AsyncMock, MagicMock

import pytest

from mpesakit.auth import AsyncTokenManager, TokenManager
from mpesakit.http_client import AsyncHttpClient, HttpClient
from mpesakit.tax_remittance import (
    TaxRemittanceRequest,
    TaxRemittanceResponse,
    TaxRemittanceResultCallback,
    TaxRemittanceResultCallbackResponse,
    TaxRemittanceTimeoutCallback,
    TaxRemittanceTimeoutCallbackResponse,
)
from mpesakit.tax_remittance.tax_remittance import AsyncTaxRemittance, TaxRemittance


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
def tax_remittance(mock_http_client, mock_token_manager):
    """Fixture to create a TaxRemittance instance with mocked dependencies."""
    return TaxRemittance(http_client=mock_http_client, token_manager=mock_token_manager)


def valid_tax_remittance_request():
    """Create a valid TaxRemittanceRequest for testing."""
    return TaxRemittanceRequest(
        Initiator="TaxPayer",
        SecurityCredential="encrypted_credential",
        Amount=239,
        PartyA=888880,
        AccountReference="353353",
        Remarks="OK",
        QueueTimeOutURL="https://mydomain.com/b2b/remittax/queue/",
        ResultURL="https://mydomain.com/b2b/remittax/result/",
    )


def test_remittance_request_acknowledged(tax_remittance, mock_http_client):
    """Test that remittance request is acknowledged, not finalized."""
    request = valid_tax_remittance_request()
    response_data = {
        "OriginatorConversationID": "5118-111210482-1",
        "ConversationID": "AG_20230420_2010759fd5662ef6d054",
        "ResponseCode": "0",
        "ResponseDescription": "Accept the service request successfully.",
    }
    mock_http_client.post.return_value = response_data

    response = tax_remittance.remittance(request)

    assert isinstance(response, TaxRemittanceResponse)
    assert response.is_successful() is True
    assert response.ConversationID == response_data["ConversationID"]
    assert (
        response.OriginatorConversationID == response_data["OriginatorConversationID"]
    )
    assert response.ResponseCode == response_data["ResponseCode"]
    assert response.ResponseDescription == response_data["ResponseDescription"]


def test_remittance_http_error(tax_remittance, mock_http_client):
    """Test handling of HTTP errors during remittance request."""
    request = valid_tax_remittance_request()
    mock_http_client.post.side_effect = Exception("HTTP error")
    with pytest.raises(Exception) as excinfo:
        tax_remittance.remittance(request)
    assert "HTTP error" in str(excinfo.value)


def test_tax_remittance_result_callback_success():
    """Test parsing of a successful tax remittance result callback."""
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
    callback = TaxRemittanceResultCallback(**payload)
    assert callback.is_successful() is True
    assert callback.Result.TransactionID == "QKA81LK5CY"
    assert callback.Result.ResultParameters.ResultParameter[0].Key == "Amount"


def test_tax_remittance_result_callback_response():
    """Test the response schema for result callback."""
    resp = TaxRemittanceResultCallbackResponse()
    assert resp.ResultCode == 0
    assert "Callback received successfully" in resp.ResultDesc


def test_tax_remittance_timeout_callback():
    """Test parsing of a tax remittance timeout callback."""
    payload = {
        "Result": {
            "ResultType": 1,
            "ResultCode": 1,
            "ResultDesc": "The service request timed out.",
            "OriginatorConversationID": "8521-4298025-1",
            "ConversationID": "AG_20181005_00004d7ee675c0c7ee0b",
        }
    }
    callback = TaxRemittanceTimeoutCallback(**payload)
    assert callback.Result.ResultType == 1
    assert callback.Result.ResultCode == 1
    assert "timed out" in callback.Result.ResultDesc


def test_tax_remittance_timeout_callback_response():
    """Test the response schema for timeout callback."""
    resp = TaxRemittanceTimeoutCallbackResponse()
    assert resp.ResultCode == 0
    assert "Timeout notification received" in resp.ResultDesc


def test_tax_remittance_result_callback_with_string_resultcode():
    """Ensure is_successful handles ResultCode provided as a string without type errors."""
    payload = {
        "Result": {
            "ResultType": 0,
            "ResultCode": "0",  # ResultCode as a string
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
    callback = TaxRemittanceResultCallback(**payload)
    # Should not raise a TypeError comparing str and int; should treat "0" as success
    assert callback.is_successful() is True


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
def async_tax_remittance(mock_async_http_client, mock_async_token_manager):
    """Fixture to create an AsyncTaxRemittance instance with mocked dependencies."""
    return AsyncTaxRemittance(
        http_client=mock_async_http_client, token_manager=mock_async_token_manager
    )


@pytest.mark.asyncio
async def test_async_remittance_request_acknowledged(
    async_tax_remittance, mock_async_http_client
):
    """Test that async remittance request is acknowledged."""
    request = valid_tax_remittance_request()
    response_data = {
        "OriginatorConversationID": "5118-111210482-1",
        "ConversationID": "AG_20230420_2010759fd5662ef6d054",
        "ResponseCode": "0",
        "ResponseDescription": "Accept the service request successfully.",
    }
    mock_async_http_client.post.return_value = response_data

    response = await async_tax_remittance.remittance(request)

    assert isinstance(response, TaxRemittanceResponse)
    assert response.is_successful() is True
    assert response.ConversationID == response_data["ConversationID"]


@pytest.mark.asyncio
async def test_async_remittance_http_error(
    async_tax_remittance, mock_async_http_client
):
    """Test handling of HTTP errors during async remittance request."""
    request = valid_tax_remittance_request()
    mock_async_http_client.post.side_effect = Exception("Async HTTP error")
    with pytest.raises(Exception) as excinfo:
        await async_tax_remittance.remittance(request)
    assert "Async HTTP error" in str(excinfo.value)


@pytest.mark.asyncio
async def test_async_remittance_token_retrieval(
    async_tax_remittance, mock_async_http_client, mock_async_token_manager
):
    """Test that async remittance correctly retrieves token asynchronously."""
    request = valid_tax_remittance_request()
    response_data = {
        "OriginatorConversationID": "5118-111210482-1",
        "ConversationID": "AG_20230420_2010759fd5662ef6d054",
        "ResponseCode": "0",
        "ResponseDescription": "Accept the service request successfully.",
    }
    mock_async_http_client.post.return_value = response_data

    await async_tax_remittance.remittance(request)

    mock_async_token_manager.get_token.assert_called_once()
