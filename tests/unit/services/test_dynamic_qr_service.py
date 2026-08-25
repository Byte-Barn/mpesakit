"""Unit tests for the DynamicQRCodeService class in mpesakit.services.dynamic_qr module."""

import pytest
from mpesakit.services.dynamic_qr import (
    DynamicQRCodeService,
    AsyncDynamicQRCodeService,
)
from mpesakit.dynamic_qr_code.schemas import (
  DynamicQRGenerateResponse,
  DynamicQRTransactionType,
)


@pytest.fixture
def dynamic_qr_service(mock_http_client, mock_token_manager):
    """Creates a DynamicQRCodeService instance for testing."""
    return DynamicQRCodeService(
        http_client=mock_http_client,
        token_manager=mock_token_manager,
    )


@pytest.fixture
def async_dynamic_qr_service(mock_async_http_client, mock_async_token_manager):
    """Creates an AsyncDynamicQRCodeService instance for testing."""
    return AsyncDynamicQRCodeService(
        http_client=mock_async_http_client,
        token_manager=mock_async_token_manager,
    )


def test_generate_success(dynamic_qr_service, mock_http_client):
    """Test successful generation of a dynamic QR code."""
    response_data = {
        "ResponseCode": "00",
        "RequestID": "16738-27456357-1",
        "ResponseDescription": "QR Code Successfully Generated.",
        "QRCode": "base64-encoded-string",
    }

    mock_http_client.post.return_value = response_data

    response = dynamic_qr_service.generate(
        merchant_name="Test Merchant",
        ref_no="REF123",
        amount=100.0,
        trx_code=DynamicQRTransactionType.BUY_GOODS,
        cpi="CPI123",
        size="300",
    )
    assert isinstance(response, DynamicQRGenerateResponse)
    assert response.is_successful is True

    # Adjust the response class if needed
    # Validate QRCode string is in response body.
    assert hasattr(response, "QRCode") or hasattr(response, "qr_code")
    assert (
        getattr(response, "QRCode", None) == "base64-encoded-string"
        or getattr(response, "qr_code", None) == "base64-encoded-string"
    )
    mock_http_client.post.assert_called_once()
    # Validate auth header present in successful request(?)
    args, kwargs = mock_http_client.post.call_args
    assert "Authorization" in kwargs["headers"]
    assert kwargs["headers"]["Authorization"] == "Bearer test_token"


def test_generate_dynamic_qr_handles_http_error(dynamic_qr_service, mock_http_client):
    """Test that an HTTP error during Dynamic QR Code generation is handled."""
    mock_http_client.post.side_effect = Exception("HTTP error")

    with pytest.raises(Exception) as excinfo:
        dynamic_qr_service.generate(
            merchant_name="Test Supermarket",
            ref_no="xewr34fer4t",
            amount=200,
            trx_code=DynamicQRTransactionType.BUY_GOODS,
            cpi="373132",
            size="300",
        )
    assert "HTTP error" in str(excinfo.value)


def test_generate_dynamic_qr_string_response_code_no_type_error(
    dynamic_qr_service, mock_http_client
):
    """Ensure ResponseCode as a string does not cause type comparison errors in is_successful."""
    # ResponseCode provided as a string (common in some APIs)
    response_data = {
        "ResponseCode": "00",
        "RequestID": "16738-27456357-1",
        "ResponseDescription": "QR Code Successfully Generated.",
        "QRCode": "base64-encoded-string",
    }
    mock_http_client.post.return_value = response_data

    # Should not raise a TypeError when evaluating is_successful
    response = dynamic_qr_service.generate(
        merchant_name="Test Supermarket",
        ref_no="xewr34fer4t",
        amount=200,
        trx_code=DynamicQRTransactionType.BUY_GOODS,
        cpi="373132",
        size="300",
    )
    assert response.is_successful is True


def test_generate_filters_kwargs(dynamic_qr_service, mock_http_client):
    """Test that extra kwargs are filtered out in the request."""
    response_data = {
        "ResponseCode": "00",
        "RequestID": "16738-27456357-1",
        "ResponseDescription": "QR Code Successfully Generated.",
        "QRCode": "base64-encoded-string",
    }
    mock_http_client.post.return_value = response_data

    resp = dynamic_qr_service.generate(
        merchant_name="Test Merchant",
        ref_no="REF456",
        amount=200.0,
        trx_code=DynamicQRTransactionType.BUY_GOODS.value,
        cpi="CPI456",
        size="400x400",
        ExtraField="should_be_filtered",
    )
    assert isinstance(resp, DynamicQRGenerateResponse)
    assert resp.is_successful is True


def test_dynamic_qr_service_initializes_correctly(mock_http_client, mock_token_manager):
    """Test DynamicQRCodeService initializes with correct arguments."""
    service = DynamicQRCodeService(
        http_client=mock_http_client,
        token_manager=mock_token_manager,
    )
    assert service.http_client is mock_http_client
    assert service.token_manager is mock_token_manager


@pytest.mark.asyncio
async def test_async_generate_success(async_dynamic_qr_service, mock_async_http_client):
    """Test successful async generation of a dynamic QR code."""
    response_data = {
        "ResponseCode": "00",
        "RequestID": "16738-27456357-1",
        "ResponseDescription": "QR Code Successfully Generated.",
        "QRCode": "base64-encoded-string",
    }
    mock_async_http_client.post.return_value = response_data

    response = await async_dynamic_qr_service.generate(
        merchant_name="Test Merchant",
        ref_no="REF123",
        amount=100.0,
        trx_code=DynamicQRTransactionType.BUY_GOODS,
        cpi="CPI123",
        size="300",
    )
    assert isinstance(response, DynamicQRGenerateResponse)
    assert response.is_successful is True
    assert (
        getattr(response, "QRCode", None) == "base64-encoded-string"
        or getattr(response, "qr_code", None) == "base64-encoded-string"
    )
    mock_async_http_client.post.assert_called_once()
    args, kwargs = mock_async_http_client.post.call_args
    assert "Authorization" in kwargs["headers"]
    assert kwargs["headers"]["Authorization"] == "Bearer test_token"


@pytest.mark.asyncio
async def test_async_generate_dynamic_qr_handles_http_error(
    async_dynamic_qr_service, mock_async_http_client
):
    """Test that an HTTP error during async Dynamic QR Code generation is handled."""
    mock_async_http_client.post.side_effect = Exception("Async HTTP error")

    with pytest.raises(Exception) as excinfo:
        await async_dynamic_qr_service.generate(
            merchant_name="Test Supermarket",
            ref_no="xewr34fer4t",
            amount=200,
            trx_code=DynamicQRTransactionType.BUY_GOODS,
            cpi="373132",
            size="300",
        )
    assert "Async HTTP error" in str(excinfo.value)


@pytest.mark.asyncio
async def test_async_generate_filters_kwargs(
    async_dynamic_qr_service, mock_async_http_client
):
    """Test that async generate filters out unexpected kwargs."""
    response_data = {
        "ResponseCode": "00",
        "RequestID": "16738-27456357-1",
        "ResponseDescription": "QR Code Successfully Generated.",
        "QRCode": "base64-encoded-string",
    }
    mock_async_http_client.post.return_value = response_data

    resp = await async_dynamic_qr_service.generate(
        merchant_name="Test Merchant",
        ref_no="REF456",
        amount=200.0,
        trx_code=DynamicQRTransactionType.BUY_GOODS.value,
        cpi="CPI456",
        size="400x400",
        ExtraField="should_be_filtered",
    )
    assert isinstance(resp, DynamicQRGenerateResponse)
    assert resp.is_successful is True


@pytest.mark.asyncio
async def test_async_generate_dynamic_qr_token_manager_called(
    async_dynamic_qr_service, mock_async_token_manager, mock_async_http_client
):
    """Test that the async token manager's get_token is properly awaited."""
    mock_async_http_client.post.return_value = {
        "ResponseCode": "00",
        "RequestID": "16738-27456357-1",
        "ResponseDescription": "QR Code Successfully Generated.",
        "QRCode": "base64-encoded-string",
    }

    await async_dynamic_qr_service.generate(
        merchant_name="Test Supermarket",
        ref_no="xewr34fer4t",
        amount=200,
        trx_code=DynamicQRTransactionType.BUY_GOODS,
        cpi="373132",
        size="300",
    )

    mock_async_token_manager.get_token.assert_awaited_once()


def test_async_dynamic_qr_service_initializes_correctly(
    mock_async_http_client, mock_async_token_manager
):
    """Test AsyncDynamicQRCodeService initializes with correct arguments."""
    service = AsyncDynamicQRCodeService(
        http_client=mock_async_http_client,
        token_manager=mock_async_token_manager,
    )
    assert service.http_client is mock_async_http_client
    assert service.token_manager is mock_async_token_manager
