"""Shared fixtures for unit tests."""

import asyncio
import time

import pytest
from unittest.mock import AsyncMock, MagicMock

from mpesakit.auth import AsyncTokenManager, TokenManager
from mpesakit.dynamic_qr_code.schemas import (
    DynamicQRGenerateRequest,
    DynamicQRGenerateResponse,
    DynamicQRTransactionType,
)
from mpesakit.http_client import AsyncHttpClient, HttpClient
from mpesakit.services.dynamic_qr import (
    DynamicQRCodeService,
    AsyncDynamicQRCodeService,
)


@pytest.fixture(autouse=True)
def _no_real_sleep(monkeypatch):
    """Skip real delays from tenacity's retry backoff during tests.

    MpesaHttpClient/MpesaAsyncHttpClient retry transient errors with
    wait_random_exponential (tenacity), which sleeps via time.sleep
    (sync) / asyncio.sleep (async). Left real, each retry test spends
    real wall-clock seconds waiting, making the suite slow. Tenacity
    looks up time.sleep/asyncio.sleep at call time, so patching them
    here is enough to make backoff instant without touching retry logic.
    """
    monkeypatch.setattr(time, "sleep", lambda seconds: None)

    async def _instant_async_sleep(seconds, result=None):
        return result

    monkeypatch.setattr(asyncio, "sleep", _instant_async_sleep)


@pytest.fixture
def mock_token_manager():
    """Mock TokenManager to return a fixed token."""
    mock = MagicMock(spec=TokenManager)
    mock.get_token.return_value = "test_token"
    return mock


@pytest.fixture
def mock_http_client():
    """Mock HttpClient to simulate HTTP requests."""
    return MagicMock(spec=HttpClient)


@pytest.fixture
def mock_async_token_manager():
    """Mock AsyncTokenManager to return a fixed token."""
    mock = AsyncMock(spec=AsyncTokenManager)
    mock.get_token.return_value = "test_token"
    return mock


@pytest.fixture
def mock_async_http_client():
    """Mock AsyncHttpClient to simulate async HTTP requests."""
    return AsyncMock(spec=AsyncHttpClient)


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
