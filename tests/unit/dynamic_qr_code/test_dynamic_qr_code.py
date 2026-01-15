"""Unit tests for the Dynamic QR Code functionality of the Mpesa SDK."""

import pytest
from unittest.mock import MagicMock
from mpesakit.dynamic_qr_code import (
    DynamicQRGenerateRequest,
    DynamicQRCode,
    DynamicQRTransactionType,
)
from mpesakit.auth import TokenManager
from mpesakit.http_client.mpesa_http_client import MpesaHttpClient


@pytest.fixture
def mock_token_manager():
    """Mock TokenManager to return a fixed token for testing."""
    mock = MagicMock(spec=TokenManager)
    mock.get_token.return_value = "test_token"
    return mock


@pytest.fixture
def mock_http_client():
    """Mock MpesaHttpClient for testing."""
    return MagicMock(spec=MpesaHttpClient)


@pytest.fixture
def dynamic_qr_service(mock_http_client, mock_token_manager):
    """Fixture to create an instance of DynamicQRCode with mocked dependencies."""
    return DynamicQRCode(http_client=mock_http_client, token_manager=mock_token_manager)


def test_generate_dynamic_qr_success(dynamic_qr_service, mock_http_client):
    """Test successful Dynamic QR Code generation."""
    request = DynamicQRGenerateRequest(
        MerchantName="Test Supermarket",
        RefNo="xewr34fer4t",
        Amount=200,
        TrxCode=DynamicQRTransactionType.BUY_GOODS,
        CPI="373132",
        Size="300",
    )
    response_data = {
        "ResponseCode": "00",
        "ResponseDescription": "Success",
        "QRCode": "base64-encoded-string",
    }
    mock_http_client.post.return_value = response_data

    response = dynamic_qr_service.generate(request)

    assert response.is_successful() is True

    # Adjust the response class if needed
    assert hasattr(response, "QRCode") or hasattr(response, "qr_code")
    assert (
        getattr(response, "QRCode", None) == "base64-encoded-string"
        or getattr(response, "qr_code", None) == "base64-encoded-string"
    )
    mock_http_client.post.assert_called_once()
    args, kwargs = mock_http_client.post.call_args
    assert "Authorization" in kwargs["headers"]
    assert kwargs["headers"]["Authorization"] == "Bearer test_token"


def test_generate_dynamic_qr_handles_http_error(dynamic_qr_service, mock_http_client):
    """Test that an HTTP error during Dynamic QR Code generation is handled."""
    request = DynamicQRGenerateRequest(
        MerchantName="Test Supermarket",
        RefNo="xewr34fer4t",
        Amount=200,
        TrxCode=DynamicQRTransactionType.BUY_GOODS,
        CPI="373132",
        Size="300",
    )
    mock_http_client.post.side_effect = Exception("HTTP error")

    with pytest.raises(Exception) as excinfo:
        dynamic_qr_service.generate(request)
    assert "HTTP error" in str(excinfo.value)


def test_generate_dynamic_qr_invalid_trx_code():
    """Test that providing an invalid TrxCode raises a ValueError."""
    # Use an invalid TrxCode value
    invalid_trx_code = "INVALID_CODE"
    with pytest.raises(ValueError) as excinfo:
        DynamicQRGenerateRequest(
            MerchantName="Test Supermarket",
            RefNo="xewr34fer4t",
            Amount=200,
            TrxCode=invalid_trx_code,
            CPI="373132",
            Size="300",
        )
    assert "TrxCode must be one of:" in str(excinfo.value)


def test_generate_dynamic_qr_send_money_cpi_normalization(monkeypatch):
    """Test CPI normalization for SEND_MONEY TrxCode."""
    # Patch normalize_phone_number to simulate normalization
    monkeypatch.setattr(
        "mpesakit.dynamic_qr_code.schemas.normalize_phone_number",
        lambda cpi: "254712345678"
        if cpi in ["0712345678", "+254712345678", "254712345678"]
        else None,
    )

    # Should normalize '0712345678' to '254712345678'
    req = DynamicQRGenerateRequest(
        MerchantName="Test",
        RefNo="ref",
        Amount=100,
        TrxCode=DynamicQRTransactionType.SEND_MONEY,
        CPI="0712345678",
        Size="300",
    )
    assert req.CPI == "254712345678"

    # Should normalize '+254712345678' to '254712345678'
    req = DynamicQRGenerateRequest(
        MerchantName="Test",
        RefNo="ref",
        Amount=100,
        TrxCode=DynamicQRTransactionType.SEND_MONEY,
        CPI="+254712345678",
        Size="300",
    )
    assert req.CPI == "254712345678"

    # Should keep '254712345678' as is
    req = DynamicQRGenerateRequest(
        MerchantName="Test",
        RefNo="ref",
        Amount=100,
        TrxCode=DynamicQRTransactionType.SEND_MONEY,
        CPI="254712345678",
        Size="300",
    )
    assert req.CPI == "254712345678"

    # Should raise ValueError for invalid CPI
    with pytest.raises(ValueError) as excinfo:
        DynamicQRGenerateRequest(
            MerchantName="Test",
            RefNo="ref",
            Amount=100,
            TrxCode=DynamicQRTransactionType.SEND_MONEY,
            CPI="12345",
            Size="300",
        )
    assert "CPI for SEND_MONEY must be a valid Kenyan phone number" in str(
        excinfo.value
    )

def test_generate_dynamic_qr_string_response_code_no_type_error(dynamic_qr_service, mock_http_client):
    """Ensure ResponseCode as a string does not cause type comparison errors in is_successful()."""
    request = DynamicQRGenerateRequest(
        MerchantName="Test Supermarket",
        RefNo="xewr34fer4t",
        Amount=200,
        TrxCode=DynamicQRTransactionType.BUY_GOODS,
        CPI="373132",
        Size="300",
    )
    # ResponseCode provided as a string (common in some APIs)
    response_data = {
        "ResponseCode": "00",
        "ResponseDescription": "Success",
        "QRCode": "base64-encoded-string",
    }
    mock_http_client.post.return_value = response_data

    # Should not raise a TypeError when evaluating is_successful()
    response = dynamic_qr_service.generate(request)
    assert response.is_successful() is True

