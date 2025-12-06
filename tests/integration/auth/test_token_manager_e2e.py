"""This module contains tests for the M-Pesa authorization process.

It ensures that the TokenManager can successfully obtain and manage tokens both synchronous and asynchronous.
"""

from dotenv import load_dotenv
import os
import pytest
from mpesakit.auth import TokenManager,AsyncTokenManager
from mpesakit.http_client import MpesaHttpClient
from mpesakit.errors import MpesaApiException

load_dotenv()


@pytest.fixture(scope="module")
def valid_credentials():
    """Provide valid M-Pesa credentials for testing."""
    # Set these as environment variables for live testing
    return {
        "consumer_key": os.getenv("MPESA_CONSUMER_KEY"),
        "consumer_secret": os.getenv("MPESA_CONSUMER_SECRET"),
    }


@pytest.fixture(scope="module")
def invalid_credentials():
    """Provide invalid M-Pesa credentials for testing."""
    return {
        "consumer_key": "invalid_key",
        "consumer_secret": "invalid_secret",
    }


@pytest.fixture(scope="module")
def http_client():
    """Provide an instance of MpesaHttpClient for testing."""
    # Use sandbox environment for testing
    return MpesaHttpClient(env=os.getenv("MPESA_ENV", "sandbox"))


def test_get_token_success(valid_credentials, http_client):
    """Test that a valid token can be retrieved."""
    tm = TokenManager(
        consumer_key=valid_credentials["consumer_key"],
        consumer_secret=valid_credentials["consumer_secret"],
        http_client=http_client,
    )
    token = tm.get_token()
    assert isinstance(token, str)
    assert len(token) > 10


def test_token_caching(valid_credentials, http_client):
    """Test that the token is cached and reused until it expires."""
    tm = TokenManager(
        consumer_key=valid_credentials["consumer_key"],
        consumer_secret=valid_credentials["consumer_secret"],
        http_client=http_client,
    )
    token1 = tm.get_token()
    token2 = tm.get_token()
    assert token1 == token2  # Should be cached


def test_force_refresh_token(valid_credentials, http_client):
    """Test that forcing a token refresh retrieves a new token."""
    tm = TokenManager(
        consumer_key=valid_credentials["consumer_key"],
        consumer_secret=valid_credentials["consumer_secret"],
        http_client=http_client,
    )
    token2 = tm.get_token(force_refresh=True)
    assert isinstance(token2, str)
    # Token may or may not change, but should be valid
    assert len(token2) > 10


def test_invalid_credentials_raises(http_client, invalid_credentials):
    """Test that invalid credentials raise an exception."""
    tm = TokenManager(
        consumer_key=invalid_credentials["consumer_key"],
        consumer_secret=invalid_credentials["consumer_secret"],
        http_client=http_client,
    )
    with pytest.raises(MpesaApiException) as excinfo:
        tm.get_token()
    assert (
        "Invalid credentials" in str(excinfo.value)
        or excinfo.value.error.status_code == 403
    )


def test_invalid_grant_type(http_client, valid_credentials, monkeypatch):
    """Test that an invalid grant type raises an exception."""
    tm = TokenManager(
        consumer_key=valid_credentials["consumer_key"],
        consumer_secret=valid_credentials["consumer_secret"],
        http_client=http_client,
    )
    # Patch the get_token method to use an invalid grant_type
    original_get = http_client.get

    def fake_get(url, headers=None, params=None):
        params = params or {}
        params["grant_type"] = "invalid_grant"
        return original_get(url, headers=headers, params=params)

    monkeypatch.setattr(http_client, "get", fake_get)
    with pytest.raises(MpesaApiException) as excinfo:
        tm.get_token(force_refresh=True)
    assert (
        excinfo.value.error.status_code == 403
    )  # Blocked by Imperva before reaching Daraja API


def test_invalid_auth_type(http_client, valid_credentials, monkeypatch):
    """Test that an invalid auth type raises an exception."""
    tm = TokenManager(
        consumer_key=valid_credentials["consumer_key"],
        consumer_secret=valid_credentials["consumer_secret"],
        http_client=http_client,
    )
    # Patch the _get_basic_auth_header to return a wrong type
    monkeypatch.setattr(tm, "_get_basic_auth_header", lambda: "Bearer something")
    with pytest.raises(MpesaApiException) as excinfo:
        tm.get_token(force_refresh=True)
    assert (
        excinfo.value.error.status_code == 403
    )  # Blocked by Imperva before reaching Daraja API

@pytest.mark.asyncio
async def test_async_get_token_success(valid_credentials, async_http_client):
    """Test that a valid token can be retrieved asynchronously."""
    if not valid_credentials.get("consumer_key"):
        pytest.skip("MPESA_CONSUMER_KEY not set for integration test.")

    tm = AsyncTokenManager(
        consumer_key=valid_credentials["consumer_key"],
        consumer_secret=valid_credentials["consumer_secret"],
        http_client=async_http_client,
    )
    token = await tm.get_token()
    assert isinstance(token, str)
    assert len(token) > 10


@pytest.mark.asyncio
async def test_async_token_caching(valid_credentials, async_http_client):
    """Test that the token is cached and reused until it expires asynchronously."""
    if not valid_credentials.get("consumer_key"):
        pytest.skip("MPESA_CONSUMER_KEY not set for integration test.")

    tm = AsyncTokenManager(
        consumer_key=valid_credentials["consumer_key"],
        consumer_secret=valid_credentials["consumer_secret"],
        http_client=async_http_client,
    )
    token1 = await tm.get_token()
    token2 = await tm.get_token()
    assert token1 == token2


@pytest.mark.asyncio
async def test_async_force_refresh_token(valid_credentials, async_http_client):
    """Test that forcing a token refresh retrieves a new token asynchronously."""
    if not valid_credentials.get("consumer_key"):
        pytest.skip("MPESA_CONSUMER_KEY not set for integration test.")

    tm = AsyncTokenManager(
        consumer_key=valid_credentials["consumer_key"],
        consumer_secret=valid_credentials["consumer_secret"],
        http_client=async_http_client,
    )
    _ = await tm.get_token()
    token2 = await tm.get_token(force_refresh=True)
    assert isinstance(token2, str)
    assert len(token2) > 10


@pytest.mark.asyncio
async def test_async_invalid_credentials_raises(async_http_client, invalid_credentials):
    """Test that invalid credentials raise an exception asynchronously."""
    tm = AsyncTokenManager(
        consumer_key=invalid_credentials["consumer_key"],
        consumer_secret=invalid_credentials["consumer_secret"],
        http_client=async_http_client,
    )
    with pytest.raises(MpesaApiException) as excinfo:
        await tm.get_token()


    assert (
        "Invalid credentials" in str(excinfo.value)
        or excinfo.value.error.status_code == 400
        or excinfo.value.error.status_code == 403
    )


@pytest.mark.asyncio
async def test_async_invalid_grant_type(async_http_client, valid_credentials, monkeypatch):
    """Test that an invalid grant type raises an exception asynchronously."""
    if not valid_credentials.get("consumer_key"):
        pytest.skip("MPESA_CONSUMER_KEY not set for integration test.")

    tm = AsyncTokenManager(
        consumer_key=valid_credentials["consumer_key"],
        consumer_secret=valid_credentials["consumer_secret"],
        http_client=async_http_client,
    )

    original_get = async_http_client.get

    async def fake_async_get(url, headers=None, params=None):
        params = params or {}
        params["grant_type"] = "invalid_grant"

        return await original_get(url, headers=headers, params=params)

    monkeypatch.setattr(async_http_client, "get", fake_async_get)
    with pytest.raises(MpesaApiException) as excinfo:
        await tm.get_token(force_refresh=True)

    assert (
        excinfo.value.error.status_code == 403
    )


@pytest.mark.asyncio
async def test_async_invalid_auth_type(async_http_client, valid_credentials, monkeypatch):
    """Test that an invalid auth type raises an exception asynchronously."""
    if not valid_credentials.get("consumer_key"):
        pytest.skip("MPESA_CONSUMER_KEY not set for integration test.")

    tm = AsyncTokenManager(
        consumer_key=valid_credentials["consumer_key"],
        consumer_secret=valid_credentials["consumer_secret"],
        http_client=async_http_client,
    )

    monkeypatch.setattr(tm, "_get_basic_auth_header", lambda: "Bearer something")
    with pytest.raises(MpesaApiException) as excinfo:
        await tm.get_token(force_refresh=True)

    assert (
        excinfo.value.error.status_code == 403
    )
