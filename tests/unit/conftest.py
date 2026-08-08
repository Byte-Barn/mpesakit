"""Shared fixtures for unit tests."""

import asyncio
import time

import pytest
from unittest.mock import AsyncMock, MagicMock

from mpesakit.auth import AsyncTokenManager, TokenManager
from mpesakit.http_client import AsyncHttpClient, HttpClient


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
