"""End-to-End Test for M-Pesa Dynamic QR Code Generation."""

import pytest

from mpesakit.dynamic_qr_code.schemas import (
    DynamicQRGenerateRequest,
)
from mpesakit.services.dynamic_qr import DynamicQRCodeService

pytestmark = pytest.mark.live


def test_dynamic_qr_code_generate(
    service: DynamicQRCodeService,
    payload: DynamicQRGenerateRequest
):
    """End-to-end test for M-Pesa Dynamic QR Code generation."""

    response = service.generate(
        merchant_name=payload.MerchantName,
        ref_no=payload.RefNo,
        amount=payload.Amount,
        trx_code=payload.TrxCode.value,
        cpi=payload.CPI,
        size=payload.Size,
    )

    # Basic assertions - adapt as needed for your SDK's response structure
    assert response is not None
    assert hasattr(response, "QRCode") or hasattr(response, "qr_code")
    assert getattr(response, "QRCode", None) or getattr(response, "qr_code", None)
