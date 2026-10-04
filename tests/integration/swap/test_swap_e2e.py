"""End-to-End Tests for M-Pesa Swap Request.

These tests simulate sending live Swap queries to the M-Pesa Daraja API.
They require valid Daraja credentials configured in your environment or .env file.
"""

import os
import pytest
from dotenv import load_dotenv

from mpesakit.auth import AsyncTokenManager, TokenManager
from mpesakit.errors import MpesaApiException
from mpesakit.http_client import MpesaAsyncHttpClient, MpesaHttpClient
from mpesakit.swap import AsyncSwap, Swap, SwapRequest, SwapResponse

load_dotenv()

pytestmark = pytest.mark.live


@pytest.fixture
def env():
    """Return configured MPESA environment (default: sandbox)."""
    return os.getenv("MPESA_ENV", "sandbox")


@pytest.fixture
def recipient_phone():
    """Return configured test phone number."""
    return os.getenv("MPESA_RECIPIENT_PHONE", "254722000000")


@pytest.fixture
def swap_service(env):
    """Initialize synchronous M-Pesa Swap service."""
    consumer_key = os.getenv("MPESA_CONSUMER_KEY")
    consumer_secret = os.getenv("MPESA_CONSUMER_SECRET")

    if not consumer_key or not consumer_secret:
        pytest.skip(
            "Missing MPESA_CONSUMER_KEY or MPESA_CONSUMER_SECRET in environment."
        )

    http_client = MpesaHttpClient(env=env)
    token_manager = TokenManager(
        consumer_key=consumer_key,
        consumer_secret=consumer_secret,
        http_client=http_client,
    )

    return Swap(
        http_client=http_client,
        token_manager=token_manager,
    )


@pytest.fixture
def async_swap_service(env):
    """Initialize asynchronous M-Pesa Swap service."""
    consumer_key = os.getenv("MPESA_CONSUMER_KEY")
    consumer_secret = os.getenv("MPESA_CONSUMER_SECRET")

    if not consumer_key or not consumer_secret:
        pytest.skip(
            "Missing MPESA_CONSUMER_KEY or MPESA_CONSUMER_SECRET in environment."
        )

    http_client = MpesaAsyncHttpClient(env=env)
    token_manager = AsyncTokenManager(
        consumer_key=consumer_key,
        consumer_secret=consumer_secret,
        http_client=http_client,
    )

    return AsyncSwap(
        http_client=http_client,
        token_manager=token_manager,
    )


def test_swap_e2e_success(swap_service, recipient_phone):
    """Test live synchronous Swap query against M-Pesa Daraja API."""
    print(f"\n [Sync E2E] Initiating Swap query for {recipient_phone}")

    request = SwapRequest(customerNumber=recipient_phone)
    print(f" Payload: {request.model_dump()}")

    try:
        response = swap_service.swap_request(request)
        print(f" Response: {response.model_dump()}")

        assert isinstance(response, SwapResponse)
        assert response.requestRefID is not None
        assert response.responseCode == "200"
        assert response.is_successful is True, f"Swap failed: {response.responseDesc}"

        if response.is_recently_swapped:
            print(f" SIM was swapped recently on: {response.lastSwapDate}")
        else:
            print(" SIM has not been swapped within the last 3 months.")

    except MpesaApiException as e:
        pytest.fail(f"API Request failed with status code {e.status_code}: {e.message}")


def test_swap_e2e_invalid_phone_format(swap_service):
    """Test client-side phone validation before sending E2E network call."""
    print("\n[Sync E2E] Testing client-side validation with invalid phone")

    with pytest.raises(ValueError, match="Invalid Kenyan MSISDN"):
        SwapRequest(customerNumber="000000")


@pytest.mark.asyncio
async def test_async_swap_e2e_success(async_swap_service, recipient_phone):
    """Test live asynchronous Swap query against M-Pesa Daraja API."""
    print(f"\n [Async E2E] Initiating Swap query for {recipient_phone}")

    request = SwapRequest(customerNumber=recipient_phone)
    print(f" Async Payload: {request.model_dump()}")

    try:
        response = await async_swap_service.swap_request(request)
        print(f" Async Response: {response.model_dump()}")

        assert isinstance(response, SwapResponse)
        assert response.requestRefID is not None
        assert response.responseCode == "200"
        assert response.is_successful is True, (
            f"Async Swap failed: {response.responseDesc}"
        )

    except MpesaApiException as e:
        pytest.fail(
            f"Async API Request failed with status code {e.status_code}: {e.message}"
        )
