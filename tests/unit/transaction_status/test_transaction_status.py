"""Unit tests for the M-Pesa Transaction Status API interactions.

This module tests the TransactionStatus class and its methods for querying transaction status.
"""

from unittest.mock import AsyncMock, MagicMock

import pytest

from mpesakit.auth import AsyncTokenManager, TokenManager
from mpesakit.http_client import AsyncHttpClient, HttpClient
from mpesakit.transaction_status import (
    AsyncTransactionStatus,
    TransactionStatus,
    TransactionStatusIdentifierType,
    TransactionStatusRequest,
    TransactionStatusResponse,
    TransactionStatusResultCallback,
    TransactionStatusResultMetadata,
    TransactionStatusResultParameter,
)


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
def transaction_status(mock_http_client, mock_token_manager):
    """Fixture to create a TransactionStatus instance with mocked dependencies."""
    return TransactionStatus(
        http_client=mock_http_client, token_manager=mock_token_manager
    )


def valid_transaction_status_request():
    """Create a valid TransactionStatusRequest for testing."""
    return TransactionStatusRequest(
        Initiator="testapi",
        SecurityCredential="encrypted_credential",
        CommandID="TransactionStatusQuery",
        TransactionID="LKXXXX1234",
        PartyA=600999,
        IdentifierType=TransactionStatusIdentifierType.SHORT_CODE.value,
        ResultURL="https://example.com/result",
        QueueTimeOutURL="https://example.com/timeout",
        Remarks="Status check for transaction",
        Occasion="JuneSalary",
        OriginalConversationID=None,
    )


def test_query_success(transaction_status, mock_http_client):
    """Test querying transaction status with a valid request."""
    request = valid_transaction_status_request()
    response_data = {
        "ConversationID": "AG_20170717_00006c6f7f5b8b6b1a62",
        "OriginatorConversationID": "12345-67890-1",
        "ResponseCode": "0",
        "ResponseDescription": "Accept the service request successfully.",
    }
    mock_http_client.post.return_value = response_data

    response = transaction_status.query(request)

    assert isinstance(response, TransactionStatusResponse)
    assert response.ConversationID == response_data["ConversationID"]
    assert (
        response.OriginatorConversationID == response_data["OriginatorConversationID"]
    )
    assert response.ResponseCode == response_data["ResponseCode"]
    assert response.ResponseDescription == response_data["ResponseDescription"]
    mock_http_client.post.assert_called_once()
    args, kwargs = mock_http_client.post.call_args
    assert args[0] == "/mpesa/transactionstatus/v1/query"
    assert kwargs["headers"]["Authorization"] == "Bearer test_token"


def test_query_http_error(transaction_status, mock_http_client):
    """Test handling HTTP errors during transaction status query."""
    request = valid_transaction_status_request()
    mock_http_client.post.side_effect = Exception("HTTP error")
    with pytest.raises(Exception) as excinfo:
        transaction_status.query(request)
    assert "HTTP error" in str(excinfo.value)


def test_transaction_status_response_is_successful_zero_code():
    """Test is_successful method with a zero response code."""
    resp = TransactionStatusResponse(
        ConversationID="AG_20170717_00006c6f7f5b8b6b1a62",
        OriginatorConversationID="12345-67890-1",
        ResponseCode="0",
        ResponseDescription="Accept the service request successfully.",
    )
    assert resp.is_successful() is True


def test_transaction_status_response_is_successful_all_zeros():
    """Test is_successful method with all zeros response code."""
    resp = TransactionStatusResponse(
        ConversationID="AG_20170717_00006c6f7f5b8b6b1a62",
        OriginatorConversationID="12345-67890-1",
        ResponseCode="00000000",
        ResponseDescription="Accept the service request successfully.",
    )
    assert resp.is_successful() is True


def test_transaction_status_response_is_successful_non_zero_code():
    """Test is_successful method with a non-zero response code."""
    resp = TransactionStatusResponse(
        ConversationID="AG_20170717_00006c6f7f5b8b6b1a62",
        OriginatorConversationID="12345-67890-1",
        ResponseCode="1",
        ResponseDescription="Failed.",
    )
    assert resp.is_successful() is False


def test_transaction_status_response_is_successful_mixed_code():
    """Test is_successful method with a mixed response code."""
    resp = TransactionStatusResponse(
        ConversationID="AG_20170717_00006c6f7f5b8b6b1a62",
        OriginatorConversationID="12345-67890-1",
        ResponseCode="00001",
        ResponseDescription="Failed.",
    )
    assert resp.is_successful() is False


def test_transaction_status_response_is_successful_empty_code():
    """Test is_successful method with an empty response code."""
    resp = TransactionStatusResponse(
        ConversationID="AG_20170717_00006c6f7f5b8b6b1a62",
        OriginatorConversationID="12345-67890-1",
        ResponseCode="",
        ResponseDescription="Failed.",
    )
    assert resp.is_successful() is False


@pytest.mark.parametrize("invalid_identifier_type", [0, 3, 5, "invalid"])
def test_transaction_status_request_invalid_identifier_type_raises(
    invalid_identifier_type,
):
    """Test that invalid IdentifierType raises a ValueError."""
    kwargs = dict(
        Initiator="testapi",
        SecurityCredential="encrypted_credential",
        CommandID="TransactionStatusQuery",
        TransactionID="LKXXXX1234",
        PartyA=600999,
        IdentifierType=invalid_identifier_type,
        ResultURL="https://example.com/result",
        QueueTimeOutURL="https://example.com/timeout",
        Remarks="Status check for transaction",
    )
    with pytest.raises(ValueError) as excinfo:
        TransactionStatusRequest(**kwargs)
    assert "IdentifierType must be one of" in str(excinfo.value)


def test_transaction_status_request_missing_transaction_id_and_originator_conversation_id_raises():
    """Test that missing both TransactionID and OriginalConversationID raises ValueError."""
    kwargs = dict(
        Initiator="testapi",
        SecurityCredential="encrypted_credential",
        CommandID="TransactionStatusQuery",
        TransactionID=None,
        PartyA=600999,
        IdentifierType=TransactionStatusIdentifierType.SHORT_CODE.value,
        ResultURL="https://example.com/result",
        QueueTimeOutURL="https://example.com/timeout",
        Remarks="Status check for transaction",
        OriginalConversationID=None,
    )
    with pytest.raises(ValueError) as excinfo:
        TransactionStatusRequest(**kwargs)
    assert (
        "At least one of OriginalConversationID or TransactionID must be provided."
        in str(excinfo.value)
    )


def test_transaction_status_request_remarks_too_long_raises():
    """Test that Remarks longer than 100 characters raises ValueError."""
    kwargs = dict(
        Initiator="testapi",
        SecurityCredential="encrypted_credential",
        CommandID="TransactionStatusQuery",
        TransactionID="LKXXXX1234",
        PartyA=600999,
        IdentifierType=TransactionStatusIdentifierType.SHORT_CODE.value,
        ResultURL="https://example.com/result",
        QueueTimeOutURL="https://example.com/timeout",
        Remarks="A" * 101,
    )
    with pytest.raises(ValueError) as excinfo:
        TransactionStatusRequest(**kwargs)
    assert "Remarks must not exceed 100 characters." in str(excinfo.value)


def test_transaction_status_request_occasion_too_long_raises():
    """Test that Occasion longer than 100 characters raises ValueError."""
    kwargs = dict(
        Initiator="testapi",
        SecurityCredential="encrypted_credential",
        CommandID="TransactionStatusQuery",
        TransactionID="LKXXXX1234",
        PartyA=600999,
        IdentifierType=TransactionStatusIdentifierType.SHORT_CODE.value,
        ResultURL="https://example.com/result",
        QueueTimeOutURL="https://example.com/timeout",
        Remarks="Status check for transaction",
        Occasion="A" * 101,
    )
    with pytest.raises(ValueError) as excinfo:
        TransactionStatusRequest(**kwargs)
    assert "Occasion must not exceed 100 characters." in str(excinfo.value)


def test_transaction_status_request_msisdn_normalization(monkeypatch):
    """Test that PartyA is normalized to a valid Kenyan MSISDN."""
    # Patch normalize_phone_number to return a valid normalized number
    monkeypatch.setattr(
        "mpesakit.utils.phone.normalize_phone_number", lambda x: "254712345678"
    )
    req = TransactionStatusRequest(
        Initiator="testapi",
        SecurityCredential="encrypted_credential",
        CommandID="TransactionStatusQuery",
        TransactionID="LKXXXX1234",
        PartyA=254712345678,
        IdentifierType=TransactionStatusIdentifierType.MSISDN.value,
        ResultURL="https://example.com/result",
        QueueTimeOutURL="https://example.com/timeout",
        Remarks="Status check for transaction",
    )
    assert req.PartyA == 254712345678


def test_transaction_status_request_invalid_msisdn(monkeypatch):
    """Test that invalid PartyA raises ValueError."""
    monkeypatch.setattr("mpesakit.utils.phone.normalize_phone_number", lambda x: None)
    kwargs = dict(
        Initiator="testapi",
        SecurityCredential="encrypted_credential",
        CommandID="TransactionStatusQuery",
        TransactionID="LKXXXX1234",
        PartyA=12345,
        IdentifierType=TransactionStatusIdentifierType.MSISDN.value,
        ResultURL="https://example.com/result",
        QueueTimeOutURL="https://example.com/timeout",
        Remarks="Status check for transaction",
    )
    with pytest.raises(ValueError) as excinfo:
        TransactionStatusRequest(**kwargs)
    assert "PartyA must be a valid Kenyan MSISDN" in str(excinfo.value)


def make_result_parameters(params):
    """Helper function to create ResultParameters from a dictionary."""
    return [TransactionStatusResultParameter(Key=k, Value=v) for k, v in params.items()]


def test_result_metadata_properties_all_present():
    """Test that all properties are correctly extracted from ResultParameters."""
    params = {
        "TransactionAmount": 1500,
        "TransactionReceipt": "LKXXXX1234",
        "Status": "Completed",
        "Reason": "None",
    }
    meta = TransactionStatusResultMetadata(
        ResultType=0,
        ResultCode=0,
        ResultDesc="Success",
        OriginatorConversationID="conv-id",
        ConversationID="conv-id-2",
        TransactionID="LKXXXX1234",
        ResultParameters=make_result_parameters(params),
    )
    assert meta.transaction_amount == 1500
    assert meta.transaction_receipt == "LKXXXX1234"
    assert meta.transaction_status == "Completed"
    assert meta.transaction_reason == "None"


def test_result_metadata_properties_some_missing():
    """Test that properties are correctly extracted when some parameters are missing."""
    params = {
        "TransactionAmount": 2000,
        "TransactionReceipt": "LKXXXX5678",
    }
    meta = TransactionStatusResultMetadata(
        ResultType=1,
        ResultCode=500,
        ResultDesc="Failure",
        OriginatorConversationID="conv-id",
        ConversationID="conv-id-2",
        TransactionID=None,
        ResultParameters=make_result_parameters(params),
    )
    assert meta.transaction_amount == 2000
    assert meta.transaction_receipt == "LKXXXX5678"
    assert meta.transaction_status is None
    assert meta.transaction_reason is None


def test_result_metadata_properties_none_parameters():
    """Test that properties are None when ResultParameters is empty."""
    meta = TransactionStatusResultMetadata(
        ResultType=0,
        ResultCode=0,
        ResultDesc="Success",
        OriginatorConversationID="conv-id",
        ConversationID="conv-id-2",
        TransactionID="LKXXXX1234",
        ResultParameters=None,
    )
    assert meta.transaction_amount is None
    assert meta.transaction_receipt is None
    assert meta.transaction_status is None
    assert meta.transaction_reason is None


def test_result_callback_schema():
    """Test that TransactionStatusResultCallback schema is correctly defined."""
    params = {
        "TransactionAmount": 1000,
        "TransactionReceipt": "LKXXXX1234",
        "Status": "Completed",
    }
    meta = TransactionStatusResultMetadata(
        ResultType=0,
        ResultCode=0,
        ResultDesc="Success",
        OriginatorConversationID="conv-id",
        ConversationID="conv-id-2",
        TransactionID="LKXXXX1234",
        ResultParameters=make_result_parameters(params),
    )
    callback = TransactionStatusResultCallback(Result=meta)
    assert isinstance(callback.Result, TransactionStatusResultMetadata)
    assert callback.Result.transaction_amount == 1000
    assert callback.Result.transaction_receipt == "LKXXXX1234"
    assert callback.Result.transaction_status == "Completed"


def test_query_response_code_type_variations(transaction_status, mock_http_client):
    """Ensure TransactionStatusResponse.is_successful handles ResponseCode as str or int without TypeError."""
    request = valid_transaction_status_request()
    cases = [
        (0, True),
        ("0", True),
        ("00000000", True),
        (1, False),
        ("1", False),
        ("00001", False),
    ]
    for code, expected_success in cases:
        response_data = {
            "ConversationID": "AG_20170717_00006c6f7f5b8b6b1a62",
            "OriginatorConversationID": "12345-67890-1",
            "ResponseCode": code,
            "ResponseDescription": "Response with varied code type.",
        }
        mock_http_client.post.return_value = response_data

        # Should not raise due to type differences and should return expected boolean
        resp = transaction_status.query(request)
        assert isinstance(resp, TransactionStatusResponse)
        assert resp.is_successful() is expected_success


def test_transaction_status_result_callback_is_successful_zero_code():
    """Test is_successful method with ResultCode as '0'."""
    result = TransactionStatusResultMetadata(
        ResultType=0,
        ResultCode="0",
        ResultDesc="Success",
        OriginatorConversationID="12345-67890-1",
        ConversationID="AG_20170717_00006c6f7f5b8b6b1a62",
        TransactionID="LKXXXX1234",
        ResultParameters=[],
    )
    callback = TransactionStatusResultCallback(Result=result)
    assert callback.is_successful() is True


def test_transaction_status_result_callback_is_successful_all_zeros():
    """Test is_successful method with ResultCode as '00000000'."""
    result = TransactionStatusResultMetadata(
        ResultType=0,
        ResultCode="00000000",
        ResultDesc="Success",
        OriginatorConversationID="12345-67890-1",
        ConversationID="AG_20170717_00006c6f7f5b8b6b1a62",
        TransactionID="LKXXXX1234",
        ResultParameters=[],
    )
    callback = TransactionStatusResultCallback(Result=result)
    assert callback.is_successful() is True


def test_transaction_status_result_callback_is_successful_non_zero_code():
    """Test is_successful method with ResultCode as '1'."""
    result = TransactionStatusResultMetadata(
        ResultType=1,
        ResultCode="1",
        ResultDesc="Failure",
        OriginatorConversationID="12345-67890-1",
        ConversationID="AG_20170717_00006c6f7f5b8b6b1a62",
        TransactionID="LKXXXX1234",
        ResultParameters=[],
    )
    callback = TransactionStatusResultCallback(Result=result)
    assert callback.is_successful() is False


def test_transaction_status_result_callback_is_successful_mixed_code():
    """Test is_successful method with ResultCode as '00001'."""
    result = TransactionStatusResultMetadata(
        ResultType=1,
        ResultCode="00001",
        ResultDesc="Failure",
        OriginatorConversationID="12345-67890-1",
        ConversationID="AG_20170717_00006c6f7f5b8b6b1a62",
        TransactionID="LKXXXX1234",
        ResultParameters=[],
    )
    callback = TransactionStatusResultCallback(Result=result)
    assert callback.is_successful() is False


def test_transaction_status_result_callback_is_successful_empty_code():
    """Test is_successful method with an empty ResultCode."""
    result = TransactionStatusResultMetadata(
        ResultType=0,
        ResultCode="",
        ResultDesc="Failure",
        OriginatorConversationID="12345-67890-1",
        ConversationID="AG_20170717_00006c6f7f5b8b6b1a62",
        TransactionID="LKXXXX1234",
        ResultParameters=[],
    )
    callback = TransactionStatusResultCallback(Result=result)
    assert callback.is_successful() is False


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


@pytest.mark.asyncio
async def test_async_query_success(mock_async_http_client, mock_async_token_manager):
    """Test querying transaction status asynchronously with a valid request."""
    request = valid_transaction_status_request()
    response_data = {
        "ConversationID": "AG_20170717_00006c6f7f5b8b6b1a62",
        "OriginatorConversationID": "12345-67890-1",
        "ResponseCode": "0",
        "ResponseDescription": "Accept the service request successfully.",
    }
    mock_async_http_client.post.return_value = response_data

    async_transaction_status = AsyncTransactionStatus(
        http_client=mock_async_http_client,
        token_manager=mock_async_token_manager,
    )

    response = await async_transaction_status.query(request)

    assert isinstance(response, TransactionStatusResponse)
    assert response.ConversationID == response_data["ConversationID"]
    assert (
        response.OriginatorConversationID == response_data["OriginatorConversationID"]
    )
    assert response.ResponseCode == response_data["ResponseCode"]
    assert response.ResponseDescription == response_data["ResponseDescription"]
    mock_async_http_client.post.assert_called_once()
    args, kwargs = mock_async_http_client.post.call_args
    assert args[0] == "/mpesa/transactionstatus/v1/query"
    assert kwargs["headers"]["Authorization"] == "Bearer test_token"


@pytest.mark.asyncio
async def test_async_query_http_error(mock_async_http_client, mock_async_token_manager):
    """Test handling HTTP errors during async transaction status query."""
    request = valid_transaction_status_request()
    mock_async_http_client.post.side_effect = Exception("HTTP error")
    async_transaction_status = AsyncTransactionStatus(
        http_client=mock_async_http_client,
        token_manager=mock_async_token_manager,
    )

    with pytest.raises(Exception) as excinfo:
        await async_transaction_status.query(request)

    assert "HTTP error" in str(excinfo.value)
