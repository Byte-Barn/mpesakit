"""Shareable fixtures and configs for module: Dynamic QR Code.

Note: Test structure for this library results in tests shared across various
scripts and modules. This file provides fixtures that can be reused across
multiple test modules, ensuring consistency and reducing redundancy.
"""

import pytest

from mpesakit.dynamic_qr_code.schemas import (
    DynamicQRGenerateRequest,
    DynamicQRGenerateResponse,
    DynamicQRTransactionType,
)
from mpesakit.services.dynamic_qr import (
    DynamicQRCodeService,
    AsyncDynamicQRCodeService,
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


@pytest.fixture
def generate_qr_request():
    """A factory fixture that yields a valid request DTO, allowing field overrides."""

    def _factory(**overrides):
        default_data = {
            "MerchantName": "Test Supermarket",
            "RefNo": "xewr34fer4t",
            "Amount": 200,
            "TrxCode": DynamicQRTransactionType.BUY_GOODS,
            "CPI": "373132",
            "Size": "300",
        }
        # Overwrite defaults with any specific test overrides provided.
        final_data = {**default_data, **overrides}
        # Triggers initial and post re-assignment validation.
        return DynamicQRGenerateRequest(**final_data)

    return _factory


@pytest.fixture
def generate_qr_success_response():
    """A factory fixture that yields a success response DTO, allowing field overrides."""

    def _factory(**overrides):
        default_data = {
            "ResponseCode": "0",
            "RequestID": "16738-27456357-1",
            "ResponseDescription": "QR Code Successfully Generated.",
            "QRCode": "base64-encoded-string",
        }
        # Overwrite defaults with any specific test overrides provided.
        final_data = {**default_data, **overrides}
        # Triggers initial and post re-assignment validation.
        return DynamicQRGenerateResponse(**final_data)

    return _factory
