"""Shareable fixtures and configs for Dynamic QR Code integration tests."""

import pytest

from mpesakit.dynamic_qr_code.schemas import (
    DynamicQRGenerateRequest,
    DynamicQRTransactionType,
)


@pytest.fixture(scope="session")
def service(mpesa_http_client, token_manager):
    """Init the M-Pesa Dynamic QR Code service with authentication.

    Service instance shared across ALL Dynamic QR Code tests.

    Note:
      Since it's session-scoped, it reuses the token_manager without
      re-authenticating.
    """
    from mpesakit.services.dynamic_qr import DynamicQRCodeService

    return DynamicQRCodeService(
        http_client=mpesa_http_client, token_manager=token_manager
    )


@pytest.fixture(scope="function")
def payload():
    """Provide a sample Dynamic QR Code request payload for tests."""
    return DynamicQRGenerateRequest(
        MerchantName="Test Supermarket",
        RefNo="xewr34fer4t",
        Amount=200,
        TrxCode=DynamicQRTransactionType.BUY_GOODS,
        CPI="373132",
        Size="300",
    )
