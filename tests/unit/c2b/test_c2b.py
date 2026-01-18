"""Unit tests for the C2B functionality of the Mpesa SDK."""

import warnings
from unittest.mock import AsyncMock, MagicMock

import pytest

from mpesakit.auth import AsyncTokenManager, TokenManager
from mpesakit.c2b import (
    C2B,
    AsyncC2B,
    C2BConfirmationResponse,
    C2BRegisterUrlRequest,
    C2BRegisterUrlResponse,
    C2BValidationRequest,
    C2BValidationResponse,
    C2BValidationResultCodeType,
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
def c2b(mock_http_client, mock_token_manager):
    """Fixture to create an instance of C2B with mocked dependencies."""
    return C2B(http_client=mock_http_client, token_manager=mock_token_manager)


def test_register_url_success(c2b, mock_http_client):
    """Test that a successful C2B URL registration can be performed."""
    request = C2BRegisterUrlRequest(
        ShortCode=600997,
        ResponseType="Completed",
        ConfirmationURL="https://domainpath.com/c2b/confirmation",
        ValidationURL="https://domainpath.com/c2b/validation",
    )
    response_data = {
        "ResponseDescription": "Success",
        "OriginatorConversationID": "abc123",
        "ConversationID": "conv456",
        "CustomerMessage": "URLs registered",
        "ResponseCode": "0",
    }
    mock_http_client.post.return_value = response_data

    response = c2b.register_url(request)

    assert isinstance(response, C2BRegisterUrlResponse)
    assert response.ResponseDescription == "Success"
    assert response.OriginatorConversationID == "abc123"
    mock_http_client.post.assert_called_once()
    args, kwargs = mock_http_client.post.call_args
    assert args[0] == "/mpesa/c2b/v1/registerurl"
    assert kwargs["headers"]["Authorization"] == "Bearer test_token"


def test_register_url_handles_typo_field(c2b, mock_http_client):
    """Test that the C2B URL registration handles the typo in the response field."""
    request = C2BRegisterUrlRequest(
        ShortCode=600997,
        ResponseType="Completed",
        ConfirmationURL="https://domainpath.com/c2b/confirmation",
        ValidationURL="https://domainpath.com/c2b/validation",
    )
    response_data = {
        "ResponseDescription": "Success",
        "OriginatorCoversationID": "typo123",
        "ConversationID": "conv456",
        "CustomerMessage": "URLs registered",
        "ResponseCode": "0",
    }
    mock_http_client.post.return_value = response_data

    response = c2b.register_url(request)

    assert response.OriginatorConversationID == "typo123"


def test_register_url_handles_http_error(c2b, mock_http_client):
    """Test that the C2B URL registration handles HTTP errors gracefully."""
    request = C2BRegisterUrlRequest(
        ShortCode=600997,
        ResponseType="Completed",
        ConfirmationURL="https://domainpath.com/c2b/confirmation",
        ValidationURL="https://domainpath.com/c2b/validation",
    )
    mock_http_client.post.side_effect = Exception("HTTP error")

    with pytest.raises(Exception) as excinfo:
        c2b.register_url(request)
    assert "HTTP error" in str(excinfo.value)


def test_validate_payment_success(c2b):
    """Test that a C2B payment validation can be handled successfully."""
    # Provide all required fields for C2BValidationRequest
    request = C2BValidationRequest(
        TransactionType="Pay Bill",
        TransID="T12345",
        TransTime="20240610120000",
        TransAmount="100.00",
        BusinessShortCode="600997",
        MSISDN="254700000000",
        ThirdPartyTransID="trans123",
    )
    result_code = C2BValidationResultCodeType.ACCEPTED
    result_desc = "Accepted"

    response = C2BValidationResponse(
        ResultCode=result_code,
        ResultDesc=result_desc,
        ThirdPartyTransID=request.ThirdPartyTransID,
    )

    assert isinstance(response, C2BValidationResponse)
    assert response.ResultCode == result_code.value
    assert response.ResultDesc == result_desc
    assert response.ThirdPartyTransID == "trans123"


def test_validate_payment_rejected(c2b):
    """Test that a C2B payment validation can be handled as rejected."""
    request = C2BValidationRequest(
        TransactionType="Pay Bill",
        TransID="T54321",
        TransTime="20240610130000",
        TransAmount="200.00",
        BusinessShortCode="600997",
        MSISDN="254700000001",
        ThirdPartyTransID="trans456",
    )
    result_code = C2BValidationResultCodeType.INVALID_SHORTCODE
    result_desc = "Rejected"

    response = C2BValidationResponse(
        ResultCode=result_code,
        ResultDesc=result_desc,
        ThirdPartyTransID=request.ThirdPartyTransID,
    )

    assert response.ResultCode == result_code.value
    assert response.ResultDesc == result_desc
    assert response.ThirdPartyTransID == "trans456"


def test_confirm_payment_success(c2b):
    """Test that a C2B payment confirmation can be acknowledged successfully."""
    response = C2BConfirmationResponse()
    assert response.ResultCode == 0
    assert "Success" in response.ResultDesc


def test_warn_invalid_urls_triggers_warning(monkeypatch):
    """Test that _warn_invalid_urls triggers a warning for forbidden keywords in URLs."""
    # Patch warnings.warn to track calls
    warn_calls = []
    monkeypatch.setattr(
        warnings, "warn", lambda msg, category: warn_calls.append((msg, category))
    )

    # Prepare values with forbidden keywords
    values = {
        "ConfirmationURL": "https://mydomain.com/mpesa/confirmation",
        "ValidationURL": "https://mydomain.com/safaricom/validation",
    }
    # Call the method
    C2BRegisterUrlRequest._warn_invalid_urls(values)

    assert any(
        "ConfirmationURL contains forbidden keyword 'mpesa'" in call[0]
        for call in warn_calls
    )
    assert any(
        "ValidationURL contains forbidden keyword 'safaricom'" in call[0]
        for call in warn_calls
    )
    assert all(call[1] is UserWarning for call in warn_calls)


def test_warn_invalid_urls_no_warning(monkeypatch):
    """Test that _warn_invalid_urls does not trigger a warning for safe URLs."""
    warn_calls = []
    monkeypatch.setattr(
        warnings, "warn", lambda msg, category: warn_calls.append((msg, category))
    )

    values = {
        "ConfirmationURL": "https://mydomain.com/c2b/confirmation",
        "ValidationURL": "https://mydomain.com/c2b/validation",
    }
    C2BRegisterUrlRequest._warn_invalid_urls(values)

    assert len(warn_calls) == 0


def test_warn_invalid_urls_multiple_keywords(monkeypatch):
    """Test that _warn_invalid_urls triggers multiple warnings for multiple forbidden keywords."""
    warn_calls = []
    monkeypatch.setattr(
        warnings, "warn", lambda msg, category: warn_calls.append((msg, category))
    )

    values = {
        "ConfirmationURL": "https://mydomain.com/exe/mpesa/confirmation",
        "ValidationURL": "https://mydomain.com/cmd/sql/validation",
    }
    C2BRegisterUrlRequest._warn_invalid_urls(values)

    assert any(
        "ConfirmationURL contains forbidden keyword 'mpesa'" in call[0]
        for call in warn_calls
    )
    assert any(
        "ConfirmationURL contains forbidden keyword 'exe'" in call[0]
        for call in warn_calls
    )
    assert any(
        "ValidationURL contains forbidden keyword 'cmd'" in call[0]
        for call in warn_calls
    )
    assert any(
        "ValidationURL contains forbidden keyword 'sql'" in call[0]
        for call in warn_calls
    )


def test_register_url_invalid_response_type_raises_value_error():
    """Test that C2BRegisterUrlRequest raises ValueError for invalid ResponseType."""
    invalid_response_type = "InvalidType"
    valid_kwargs = dict(
        ShortCode=600997,
        ConfirmationURL="https://domainpath.com/c2b/confirmation",
        ValidationURL="https://domainpath.com/c2b/validation",
    )

    kwargs = dict(ResponseType=invalid_response_type, **valid_kwargs)
    with pytest.raises(ValueError) as excinfo:
        C2BRegisterUrlRequest(**kwargs)
    assert "ResponseType must be one of" in str(excinfo.value)
    assert invalid_response_type in str(excinfo.value)


def test_c2b_register_url_response_is_successful_zero_code():
    """Test is_successful returns True for ResponseCode '0'."""
    resp = C2BRegisterUrlResponse(
        ResponseDescription="Success",
        OriginatorConversationID="abc123",
        ConversationID="conv456",
        CustomerMessage="URLs registered",
        ResponseCode="0",
    )
    assert resp.is_successful() is True


def test_c2b_register_url_response_is_successful_all_zeros():
    """Test is_successful returns True for ResponseCode '00000000'."""
    resp = C2BRegisterUrlResponse(
        ResponseDescription="Success",
        OriginatorConversationID="abc123",
        ConversationID="conv456",
        CustomerMessage="URLs registered",
        ResponseCode="00000000",
    )
    assert resp.is_successful() is True


def test_c2b_register_url_response_is_successful_non_zero_code():
    """Test is_successful returns False for non-success ResponseCode."""
    resp = C2BRegisterUrlResponse(
        ResponseDescription="Failed",
        OriginatorConversationID="abc123",
        ConversationID="conv456",
        CustomerMessage="Error",
        ResponseCode="1",
    )
    assert resp.is_successful() is False


def test_c2b_register_url_response_is_successful_mixed_code():
    """Test is_successful returns False for mixed ResponseCode like '00001'."""
    resp = C2BRegisterUrlResponse(
        ResponseDescription="Failed",
        OriginatorConversationID="abc123",
        ConversationID="conv456",
        CustomerMessage="Error",
        ResponseCode="00001",
    )
    assert resp.is_successful() is False


def test_c2b_register_url_response_is_successful_empty_code():
    """Test is_successful returns False for empty ResponseCode."""
    resp = C2BRegisterUrlResponse(
        ResponseDescription="Failed",
        OriginatorConversationID="abc123",
        ConversationID="conv456",
        CustomerMessage="Error",
        ResponseCode="",
    )
    assert resp.is_successful() is False


def test_validate_result_code_valid():
    """Test _validate_result_code accepts valid ResultCode values."""
    for code in [e.value for e in C2BValidationResultCodeType]:
        values = {"ResultCode": code}
        result = C2BValidationResponse._validate_result_code(values)
        assert result == values


def test_validate_result_code_invalid():
    """Test _validate_result_code raises ValueError for invalid ResultCode."""
    values = {"ResultCode": "INVALID_CODE"}
    valid_codes = [e.value for e in C2BValidationResultCodeType]
    with pytest.raises(ValueError) as excinfo:
        C2BValidationResponse._validate_result_code(values)
    assert f"ResultCode must be one of {valid_codes}" in str(excinfo.value)
    assert "INVALID_CODE" in str(excinfo.value)


def test_warn_long_resultdesc_triggers_warning(monkeypatch):
    """Test _warn_long_resultdesc triggers a warning if ResultDesc exceeds 90 chars."""
    warn_calls = []
    monkeypatch.setattr(
        warnings, "warn", lambda msg, category: warn_calls.append((msg, category))
    )
    long_desc = "A" * 91
    values = {"ResultDesc": long_desc}
    result = C2BValidationResponse._warn_long_resultdesc(values)
    assert any("ResultDesc exceeds 90 characters." in call[0] for call in warn_calls)
    assert all(call[1] is UserWarning for call in warn_calls)
    assert result == values


def test_warn_long_resultdesc_no_warning(monkeypatch):
    """Test _warn_long_resultdesc does not trigger warning for <= 90 chars."""
    warn_calls = []
    monkeypatch.setattr(
        warnings, "warn", lambda msg, category: warn_calls.append((msg, category))
    )
    desc = "A" * 90
    values = {"ResultDesc": desc}
    result = C2BValidationResponse._warn_long_resultdesc(values)
    assert len(warn_calls) == 0
    assert result == values


def test_warn_long_resultdesc_none(monkeypatch):
    """Test _warn_long_resultdesc does not warn if ResultDesc is None."""
    warn_calls = []
    monkeypatch.setattr(
        warnings, "warn", lambda msg, category: warn_calls.append((msg, category))
    )
    values = {"ResultDesc": None}
    result = C2BValidationResponse._warn_long_resultdesc(values)
    assert len(warn_calls) == 0
    assert result == values


def test_is_successful_with_mixed_string_response_code_no_type_error():
    """Ensure is_successful handles mixed/numeric-like string ResponseCode without TypeError and returns False for non-success codes."""
    resp = C2BRegisterUrlResponse(
        ResponseDescription="Failed",
        OriginatorConversationID="abc123",
        ConversationID="conv456",
        CustomerMessage="Error",
        ResponseCode="00001",
    )
    try:
        result = resp.is_successful()
    except TypeError as e:
        pytest.fail(
            f"is_successful raised TypeError when ResponseCode is a mixed string: {e}"
        )
    assert isinstance(result, bool)
    assert result is False


@pytest.fixture
def mock_async_token_manager():
    """Mock AsyncTokenManager to return a fixed token for testing."""
    mock = AsyncMock(spec=AsyncTokenManager)
    mock.get_token.return_value = "test_async_token"
    return mock


@pytest.fixture
def mock_async_http_client():
    """Mock AsyncHttpClient for testing."""
    return AsyncMock(spec=AsyncHttpClient)


@pytest.fixture
def async_c2b(mock_async_http_client, mock_async_token_manager):
    """Fixture to create an instance of AsyncC2B with mocked dependencies."""
    return AsyncC2B(
        http_client=mock_async_http_client, token_manager=mock_async_token_manager
    )


@pytest.mark.asyncio
async def test_async_register_url_success(async_c2b, mock_async_http_client):
    """Test that a successful async C2B URL registration can be performed."""
    request = C2BRegisterUrlRequest(
        ShortCode=600997,
        ResponseType="Completed",
        ConfirmationURL="https://domainpath.com/c2b/confirmation",
        ValidationURL="https://domainpath.com/c2b/validation",
    )
    response_data = {
        "ResponseDescription": "Success",
        "OriginatorCoversationID": "abc123",
        "ConversationID": "conv456",
        "CustomerMessage": "URLs registered",
        "ResponseCode": "0",
    }
    mock_async_http_client.post.return_value = response_data

    response = await async_c2b.register_url(request)

    assert isinstance(response, C2BRegisterUrlResponse)
    assert response.ResponseDescription == "Success"
    assert response.OriginatorConversationID == "abc123"
    mock_async_http_client.post.assert_called_once()
    args, kwargs = mock_async_http_client.post.call_args
    assert args[0] == "/mpesa/c2b/v1/registerurl"
    assert kwargs["headers"]["Authorization"] == "Bearer test_async_token"


@pytest.mark.asyncio
async def test_async_register_url_handles_typo_field(async_c2b, mock_async_http_client):
    """Test that the async C2B URL registration handles the typo in the response field."""
    request = C2BRegisterUrlRequest(
        ShortCode=600997,
        ResponseType="Completed",
        ConfirmationURL="https://domainpath.com/c2b/confirmation",
        ValidationURL="https://domainpath.com/c2b/validation",
    )
    response_data = {
        "ResponseDescription": "Success",
        "OriginatorCoversationID": "typo123",
        "ConversationID": "conv456",
        "CustomerMessage": "URLs registered",
        "ResponseCode": "0",
    }
    mock_async_http_client.post.return_value = response_data

    response = await async_c2b.register_url(request)

    assert response.OriginatorConversationID == "typo123"


@pytest.mark.asyncio
async def test_async_register_url_handles_http_error(async_c2b, mock_async_http_client):
    """Test that the async C2B URL registration handles HTTP errors gracefully."""
    request = C2BRegisterUrlRequest(
        ShortCode=600997,
        ResponseType="Completed",
        ConfirmationURL="https://domainpath.com/c2b/confirmation",
        ValidationURL="https://domainpath.com/c2b/validation",
    )
    mock_async_http_client.post.side_effect = Exception("HTTP error")

    with pytest.raises(Exception) as excinfo:
        await async_c2b.register_url(request)
    assert "HTTP error" in str(excinfo.value)
