"""Shareable fixtures and configs for Dynamic QR Code integration tests."""

import pytest

from tests.unit.dynamic_qr_code.conftest import (
    generate_qr_request,
    generate_qr_success_response,
)


@pytest.fixture(scope="session")
def service(mpesa_http_client, token_manager):
    """Fixture of live Dynamic QR Code service with authenticated client.

    Service instance shared across ALL Dynamic QR Code tests.

    Note:
      Since it's session-scoped, it reuses the token_manager without
      re-authenticating after the first token fetch.
    """
    from mpesakit.services.dynamic_qr import DynamicQRCodeService

    return DynamicQRCodeService(
        http_client=mpesa_http_client, token_manager=token_manager
    )


# Expose the imported factories as local fixtures for registration
__all__ = [
    "generate_qr_request",
    "generate_qr_success_response",
]
