"""Unit tests for the DynamicQRCodeService class in mpesakit.services.dynamic_qr module."""

import pytest
from mpesakit.services.dynamic_qr import (
    DynamicQRCodeService,
    AsyncDynamicQRCodeService,
)
from mpesakit.dynamic_qr_code import (
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
        "QRCode": "iVBORw0KGgoAAAANSUhEUgAAASwAAAEsCAIAAAD2HxkiAAAHtElEQVR42...",
    }

    mock_http_client.post.return_value = response_data

    resp = dynamic_qr_service.generate(
        merchant_name="Test Merchant",
        ref_no="REF123",
        amount=100.0,
        trx_code=DynamicQRTransactionType.BUY_GOODS,
        cpi="CPI123",
        size="300",
    )
    assert isinstance(resp, DynamicQRGenerateResponse)
    assert resp.is_successful is True


def test_generate_filters_kwargs(dynamic_qr_service, mock_http_client):
    """Test that extra kwargs are filtered out in the request."""
    response_data = {
        "QRCode": "base64string",
        "MerchantName": "Test Merchant",
        "RefNo": "REF456",
        "Amount": 200.0,
        "TrxCode": DynamicQRTransactionType.BUY_GOODS.value,
        "CPI": "CPI456",
        "Size": "400x400",
        "ResponseCode": "0",
        "ResponseDescription": "QR code generated successfully.",
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
async def test_async_generate_success(
    async_dynamic_qr_service, mock_async_http_client
):
    """Test successful async generation of a dynamic QR code."""
    response_data = {
        "ResponseCode": "00",
        "RequestID": "16738-27456357-1",
        "ResponseDescription": "QR Code Successfully Generated.",
        "QRCode": "iVBORw0KGgoAAAANSUhEUgAAASwAAAEsCAIAAAD2HxkiAAAHtElEQVR42...",
    }
    mock_async_http_client.post.return_value = response_data

    resp = await async_dynamic_qr_service.generate(
        merchant_name="Test Merchant",
        ref_no="REF123",
        amount=100.0,
        trx_code=DynamicQRTransactionType.BUY_GOODS,
        cpi="CPI123",
        size="300",
    )
    assert isinstance(resp, DynamicQRGenerateResponse)
    assert resp.is_successful is True


@pytest.mark.asyncio
async def test_async_generate_filters_kwargs(
    async_dynamic_qr_service, mock_async_http_client
):
    """Test that async generate filters out unexpected kwargs."""
    response_data = {
        "QRCode": "base64string",
        "MerchantName": "Test Merchant",
        "RefNo": "REF456",
        "Amount": 200.0,
        "TrxCode": DynamicQRTransactionType.BUY_GOODS.value,
        "CPI": "CPI456",
        "Size": "400x400",
        "ResponseCode": "0",
        "ResponseDescription": "QR code generated successfully.",
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
    assert service.qr_code.http_client is mock_async_http_client
    assert service.qr_code.token_manager is mock_async_token_manager
