"""Unit tests for the schema and data validation for Dynamic QR Code."""

import pytest
from pydantic import ValidationError

from mpesakit.dynamic_qr_code.schemas import DynamicQRTransactionType


def test_raises_validation_err_when_using_invalid_trx_code(generate_qr_request):
    """Test that providing an invalid TrxCode raises a ValidationError."""
    code = "INVALID_CODE"

    with pytest.raises(ValidationError) as excinfo:
        generate_qr_request(TrxCode=code)

    assert "Input should be" in str(excinfo.value)


@pytest.mark.parametrize(
    "actual_cpi, expected_cpi",
    [
        ("0712345678", "254712345678"),
        ("+254712345678", "254712345678"),
        ("254712345678", "254712345678"),
    ],
)
def test_generate_dynamic_qr_send_money_cpi_normalization(
    actual_cpi, expected_cpi, generate_qr_request
):
    """Test CPI normalization for SEND_MONEY TrxCode using the real utility function."""
    request = generate_qr_request(
        CPI=actual_cpi,
        TrxCode=DynamicQRTransactionType.SEND_MONEY,
    )
    assert request.CPI == expected_cpi


def test_generate_dynamic_qr_send_money_invalid_cpi(generate_qr_request):
    """Test that an un-normalizable CPI raises a ValidationError."""
    cpi = "12345"

    with pytest.raises(ValidationError) as excinfo:
        generate_qr_request(
            CPI=cpi,
            TrxCode=DynamicQRTransactionType.SEND_MONEY,
        )

    assert "CPI for SEND_MONEY must be a valid Kenyan phone number" in str(
        excinfo.value
    )
