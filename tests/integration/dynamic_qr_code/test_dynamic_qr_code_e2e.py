"""End-to-End Test for M-Pesa Dynamic QR Code Generation."""

from typing import Callable

import pytest

from mpesakit.dynamic_qr_code.schemas import (
    DynamicQRGenerateRequest,
)
from mpesakit.services.dynamic_qr import DynamicQRCodeService

pytestmark = pytest.mark.live


def test_dynamic_qr_code_generate(
    service: DynamicQRCodeService,
    generate_qr_request: Callable[[], DynamicQRGenerateRequest],
):
    """End-to-end test for M-Pesa Dynamic QR Code generation."""
    request = generate_qr_request()
    response = service.generate(request)

    # Basic assertions - adapt as needed for your SDK's response structure
    assert response is not None
    assert hasattr(response, "QRCode") or hasattr(response, "qr_code")
    assert getattr(response, "QRCode", None) or getattr(response, "qr_code", None)
