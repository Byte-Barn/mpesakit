"""Unit tests for the schema and data validation for Dynamic QR Code."""

import pytest

from mpesakit.dynamic_qr_code.schemas import (
    DynamicQRGenerateRequest,
    DynamicQRTransactionType,
)


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
        lambda cpi: (
            "254712345678"
            if cpi in ["0712345678", "+254712345678", "254712345678"]
            else None
        ),
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
