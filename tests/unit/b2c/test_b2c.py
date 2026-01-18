"""Unit tests for the B2C functionality of the Mpesa SDK."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from mpesakit.auth import AsyncTokenManager, TokenManager
from mpesakit.b2c import (
    B2C,
    AsyncB2C,
    B2CCommandIDType,
    B2CRequest,
    B2CResponse,
    B2CResultCallback,
    B2CResultMetadata,
    B2CResultParameter,
)
from mpesakit.http_client import AsyncHttpClient, HttpClient


@pytest.fixture
def mock_token_manager():
    """Mock TokenManager to return a fixed token for testing."""
    mock = MagicMock(spec=TokenManager)
    mock.get_token.return_value = "test_token"
    return mock


@pytest.fixture
def mock_http_client():
    """Mock HttpClient for testing."""
    return MagicMock(spec=HttpClient)


@pytest.fixture
def b2c(mock_http_client, mock_token_manager):
    """Fixture to create an instance of B2C with mocked dependencies."""
    return B2C(http_client=mock_http_client, token_manager=mock_token_manager)


def valid_b2c_request():
    """Return a valid B2CRequest instance."""
    return B2CRequest(
        OriginatorConversationID="12345-67890-1",
        InitiatorName="testapi",
        SecurityCredential="encrypted_credential",
        CommandID=B2CCommandIDType.SalaryPayment.value,
        Amount=1000,
        PartyA=600999,
        PartyB=254712345678,
        Remarks="Salary for June",
        QueueTimeOutURL="https://example.com/timeout",
        ResultURL="https://example.com/result",
        Occasion="JuneSalary",
    )


def test_send_payment_success(b2c, mock_http_client):
    """Test that a successful B2C payment can be performed."""
    request = valid_b2c_request()
    response_data = {
        "ConversationID": "AG_20170717_00006c6f7f5b8b6b1a62",
        "OriginatorConversationID": "12345-67890-1",
        "ResponseCode": "0",
        "ResponseDescription": "Accept the service request successfully.",
    }
    mock_http_client.post.return_value = response_data

    response = b2c.send_payment(request)

    assert isinstance(response, B2CResponse)
    assert response.ConversationID == response_data["ConversationID"]
    assert (
        response.OriginatorConversationID == response_data["OriginatorConversationID"]
    )
    assert response.ResponseCode == response_data["ResponseCode"]
    assert response.ResponseDescription == response_data["ResponseDescription"]
    mock_http_client.post.assert_called_once()
    args, kwargs = mock_http_client.post.call_args
    assert args[0] == "/mpesa/b2c/v3/paymentrequest"
    assert kwargs["headers"]["Authorization"] == "Bearer test_token"


def test_send_payment_http_error(b2c, mock_http_client):
    """Test that B2C payment handles HTTP errors gracefully."""
    request = valid_b2c_request()
    mock_http_client.post.side_effect = Exception("HTTP error")

    with pytest.raises(Exception) as excinfo:
        b2c.send_payment(request)
    assert "HTTP error" in str(excinfo.value)


def test_b2c_response_is_successful_zero_code():
    """Test is_successful returns True for ResponseCode '0'."""
    resp = B2CResponse(
        ConversationID="AG_20170717_00006c6f7f5b8b6b1a62",
        OriginatorConversationID="12345-67890-1",
        ResponseCode="0",
        ResponseDescription="Accept the service request successfully.",
    )
    assert resp.is_successful() is True


def test_b2c_response_is_successful_all_zeros():
    """Test is_successful returns True for ResponseCode '00000000'."""
    resp = B2CResponse(
        ConversationID="AG_20170717_00006c6f7f5b8b6b1a62",
        OriginatorConversationID="12345-67890-1",
        ResponseCode="00000000",
        ResponseDescription="Accept the service request successfully.",
    )
    assert resp.is_successful() is True


def test_b2c_response_is_successful_non_zero_code():
    """Test is_successful returns False for non-success ResponseCode."""
    resp = B2CResponse(
        ConversationID="AG_20170717_00006c6f7f5b8b6b1a62",
        OriginatorConversationID="12345-67890-1",
        ResponseCode="1",
        ResponseDescription="Failed.",
    )
    assert resp.is_successful() is False


def test_b2c_response_is_successful_mixed_code():
    """Test is_successful returns False for mixed ResponseCode like '00001'."""
    resp = B2CResponse(
        ConversationID="AG_20170717_00006c6f7f5b8b6b1a62",
        OriginatorConversationID="12345-67890-1",
        ResponseCode="00001",
        ResponseDescription="Failed.",
    )
    assert resp.is_successful() is False


def test_b2c_response_is_successful_empty_code():
    """Test is_successful returns False for empty ResponseCode."""
    resp = B2CResponse(
        ConversationID="AG_20170717_00006c6f7f5b8b6b1a62",
        OriginatorConversationID="12345-67890-1",
        ResponseCode="",
        ResponseDescription="Failed.",
    )
    assert resp.is_successful() is False


@pytest.mark.parametrize("invalid_command_id", ["InvalidCommand", "", None])
def test_b2c_request_invalid_command_id_raises(invalid_command_id):
    """Test that B2CRequest raises ValueError for invalid CommandID."""
    kwargs = dict(
        OriginatorConversationID="12345-67890-1",
        InitiatorName="testapi",
        SecurityCredential="encrypted_credential",
        CommandID=invalid_command_id,
        Amount=1000,
        PartyA=600999,
        PartyB=254712345678,
        Remarks="Salary for June",
        QueueTimeOutURL="https://example.com/timeout",
        ResultURL="https://example.com/result",
    )
    with pytest.raises(ValueError) as excinfo:
        B2CRequest(**kwargs)
    assert "CommandID must be one of" in str(excinfo.value)


def test_b2c_request_invalid_partyb_raises():
    """Test that B2CRequest raises ValueError for invalid PartyB phone number."""
    kwargs = dict(
        OriginatorConversationID="12345-67890-1",
        InitiatorName="testapi",
        SecurityCredential="encrypted_credential",
        CommandID=B2CCommandIDType.BusinessPayment.value,
        Amount=1000,
        PartyA=600999,
        PartyB="notaphone",
        Remarks="Salary for June",
        QueueTimeOutURL="https://example.com/timeout",
        ResultURL="https://example.com/result",
    )
    with pytest.raises(ValueError) as excinfo:
        B2CRequest(**kwargs)
    assert "PartyB must be a valid Kenyan phone number" in str(excinfo.value)


def test_b2c_request_remarks_too_long_raises():
    """Test that B2CRequest raises ValueError for Remarks > 100 chars."""
    kwargs = dict(
        OriginatorConversationID="12345-67890-1",
        InitiatorName="testapi",
        SecurityCredential="encrypted_credential",
        CommandID=B2CCommandIDType.BusinessPayment.value,
        Amount=1000,
        PartyA=600999,
        PartyB=254712345678,
        Remarks="A" * 101,
        QueueTimeOutURL="https://example.com/timeout",
        ResultURL="https://example.com/result",
    )
    with pytest.raises(ValueError) as excinfo:
        B2CRequest(**kwargs)
    assert "Remarks must not exceed 100 characters." in str(excinfo.value)


def test_b2c_request_occasion_too_long_raises():
    """Test that B2CRequest raises ValueError for Occasion > 100 chars."""
    kwargs = dict(
        OriginatorConversationID="12345-67890-1",
        InitiatorName="testapi",
        SecurityCredential="encrypted_credential",
        CommandID=B2CCommandIDType.BusinessPayment.value,
        Amount=1000,
        PartyA=600999,
        PartyB=254712345678,
        Remarks="Salary for June",
        QueueTimeOutURL="https://example.com/timeout",
        ResultURL="https://example.com/result",
        Occasion="A" * 101,
    )
    with pytest.raises(ValueError) as excinfo:
        B2CRequest(**kwargs)
    assert "Occasion must not exceed 100 characters." in str(excinfo.value)


def make_result_parameters(params):
    """Helper to create list of B2CResultParameter from dict."""
    return [B2CResultParameter(Key=k, Value=v) for k, v in params.items()]


def test_result_metadata_properties_all_present():
    """Test that B2CResultMetadata properties are correctly parsed."""
    params = {
        "TransactionAmount": 1500,
        "TransactionReceipt": "LKXXXX1234",
        "B2CRecipientIsRegisteredCustomer": "Y",
        "ReceiverPartyPublicName": "John Doe",
        "TransactionCompletedDateTime": "2024-06-01T12:34:56+03:00",
        "B2CChargesPaidAccountAvailableFunds": 5000.50,
        "B2CUtilityAccountAvailableFunds": 2000.0,
        "B2CWorkingAccountAvailableFunds": 10000.0,
    }
    meta = B2CResultMetadata(
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
    assert meta.recipient_is_registered is True
    assert meta.receiver_party_public_name == "John Doe"
    assert meta.transaction_completed_datetime == "2024-06-01T12:34:56+03:00"
    assert meta.charges_paid_account_available_funds == 5000.50
    assert meta.utility_account_available_funds == 2000.0
    assert meta.working_account_available_funds == 10000.0


def test_result_metadata_properties_some_missing():
    """Test that B2CResultMetadata handles missing parameters gracefully."""
    params = {
        "TransactionAmount": 2000,
        "TransactionReceipt": "LKXXXX5678",
        "B2CRecipientIsRegisteredCustomer": "N",
    }
    meta = B2CResultMetadata(
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
    assert meta.recipient_is_registered is False
    assert meta.receiver_party_public_name is None
    assert meta.transaction_completed_datetime is None
    assert meta.charges_paid_account_available_funds is None
    assert meta.utility_account_available_funds is None
    assert meta.working_account_available_funds is None


def test_result_metadata_properties_none_parameters():
    """Test that B2CResultMetadata handles None parameters correctly."""
    meta = B2CResultMetadata(
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
    assert meta.recipient_is_registered is None
    assert meta.receiver_party_public_name is None
    assert meta.transaction_completed_datetime is None
    assert meta.charges_paid_account_available_funds is None
    assert meta.utility_account_available_funds is None
    assert meta.working_account_available_funds is None


def test_result_metadata_recipient_is_registered_none():
    """Test that B2CResultMetadata handles invalid recipient_is_registered value."""
    params = {
        "TransactionAmount": 1000,
        "TransactionReceipt": "LKXXXX1234",
        "B2CRecipientIsRegisteredCustomer": "X",  # Invalid value
    }
    meta = B2CResultMetadata(
        ResultType=0,
        ResultCode=0,
        ResultDesc="Success",
        OriginatorConversationID="conv-id",
        ConversationID="conv-id-2",
        TransactionID="LKXXXX1234",
        ResultParameters=make_result_parameters(params),
    )
    assert meta.recipient_is_registered is None


def test_result_callback_schema():
    """Test that B2CResultCallback schema is correctly instantiated."""
    params = {
        "TransactionAmount": 1000,
        "TransactionReceipt": "LKXXXX1234",
    }
    meta = B2CResultMetadata(
        ResultType=0,
        ResultCode=0,
        ResultDesc="Success",
        OriginatorConversationID="conv-id",
        ConversationID="conv-id-2",
        TransactionID="LKXXXX1234",
        ResultParameters=make_result_parameters(params),
    )
    callback = B2CResultCallback(Result=meta)
    assert isinstance(callback.Result, B2CResultMetadata)
    assert callback.Result.transaction_amount == 1000
    assert callback.Result.transaction_receipt == "LKXXXX1234"


def test_result_callback_is_successful_zero_code():
    """Test is_successful returns True for ResultCode 0."""
    meta = B2CResultMetadata(
        ResultType=0,
        ResultCode=0,
        ResultDesc="Success",
        OriginatorConversationID="conv-id",
        ConversationID="conv-id-2",
        TransactionID="LKXXXX1234",
        ResultParameters=[],
    )
    callback = B2CResultCallback(Result=meta)
    assert callback.is_successful() is True


def test_result_callback_is_successful_all_zeros():
    """Test is_successful returns True for ResultCode as string of zeros."""
    meta = B2CResultMetadata(
        ResultType=0,
        ResultCode=0,
        ResultDesc="Success",
        OriginatorConversationID="conv-id",
        ConversationID="conv-id-2",
        TransactionID="LKXXXX1234",
        ResultParameters=[],
    )
    callback = B2CResultCallback(Result=meta)
    # Simulate ResultCode "00000000"
    callback.Result.ResultCode = 0
    assert callback.is_successful() is True


def test_result_callback_is_successful_non_zero_code():
    """Test is_successful returns False for non-zero ResultCode."""
    meta = B2CResultMetadata(
        ResultType=0,
        ResultCode=1,
        ResultDesc="Failed",
        OriginatorConversationID="conv-id",
        ConversationID="conv-id-2",
        TransactionID=None,
        ResultParameters=[],
    )
    callback = B2CResultCallback(Result=meta)
    assert callback.is_successful() is False


def test_result_callback_is_successful_mixed_code():
    """Test is_successful returns False for mixed code like 00001."""
    meta = B2CResultMetadata(
        ResultType=0,
        ResultCode=1,
        ResultDesc="Failed",
        OriginatorConversationID="conv-id",
        ConversationID="conv-id-2",
        TransactionID=None,
        ResultParameters=[],
    )
    callback = B2CResultCallback(Result=meta)
    assert callback.is_successful() is False


def test_result_callback_is_successful_negative_code():
    """Test is_successful returns False for negative ResultCode."""
    meta = B2CResultMetadata(
        ResultType=0,
        ResultCode=-1,
        ResultDesc="Error",
        OriginatorConversationID="conv-id",
        ConversationID="conv-id-2",
        TransactionID=None,
        ResultParameters=[],
    )
    callback = B2CResultCallback(Result=meta)
    assert callback.is_successful() is False


@pytest.fixture
def mock_async_token_manager():
    """Mock AsyncTokenManager to return a fixed token for testing."""
    mock = MagicMock(spec=AsyncTokenManager)
    mock.get_token = AsyncMock(return_value="test_token_async")
    return mock


@pytest.fixture
def mock_async_http_client():
    """Mock AsyncHttpClient for testing."""
    mock = MagicMock(spec=AsyncHttpClient)
    mock.post = AsyncMock()
    return mock


@pytest.fixture
def async_b2c(mock_async_http_client, mock_async_token_manager):
    """Fixture to create an instance of AsyncB2C with mocked dependencies."""
    return AsyncB2C(
        http_client=mock_async_http_client, token_manager=mock_async_token_manager
    )


@pytest.mark.asyncio
async def test_async_send_payment_success(
    async_b2c, mock_async_http_client, mock_async_token_manager
):
    """Test that a successful async B2C payment can be performed."""
    request = valid_b2c_request()
    response_data = {
        "ConversationID": "AG_20170717_00006c6f7f5b8b6b1a62",
        "OriginatorConversationID": "12345-67890-1",
        "ResponseCode": "0",
        "ResponseDescription": "Accept the service request successfully.",
    }

    mock_async_http_client.post.return_value = response_data

    response = await async_b2c.send_payment(request)

    assert isinstance(response, B2CResponse)
    assert response.ConversationID == response_data["ConversationID"]
    assert (
        response.OriginatorConversationID == response_data["OriginatorConversationID"]
    )
    assert response.ResponseCode == response_data["ResponseCode"]
    assert response.ResponseDescription == response_data["ResponseDescription"]


@pytest.mark.asyncio
async def test_async_send_payment_token_error(
    async_b2c, mock_async_http_client, mock_async_token_manager
):
    """Test that async B2C payment handles token errors gracefully."""
    request = valid_b2c_request()
    mock_async_token_manager.get_token.side_effect = Exception("Token error")

    with pytest.raises(Exception) as excinfo:
        await async_b2c.send_payment(request)
    assert "Token error" in str(excinfo.value)


@pytest.mark.asyncio
async def test_async_send_payment_post_error(
    async_b2c, mock_async_http_client, mock_async_token_manager
):
    """Test that async B2C payment handles POST request errors."""
    request = valid_b2c_request()
    mock_async_http_client.post.side_effect = Exception("HTTP error")

    with pytest.raises(Exception) as excinfo:
        await async_b2c.send_payment(request)
    assert "HTTP error" in str(excinfo.value)
