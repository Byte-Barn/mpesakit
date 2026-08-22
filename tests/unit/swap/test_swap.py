"""Unit tests for the Swap class in the mpesakit.swap module."""

from unittest.mock import AsyncMock, MagicMock
import pytest

from mpesakit.auth import AsyncTokenManager, TokenManager
from mpesakit.errors import MpesaApiException
from mpesakit.http_client import AsyncHttpClient, HttpClient
from mpesakit.swap import AsyncSwap, Swap, SwapRequest, SwapResponse
from mpesakit.swap.swap import SWAP_ENDPOINT


@pytest.fixture
def mock_http_client():
    """Mock synchronous HTTP client with spec to satisfy Pydantic type checks."""
    client = MagicMock(spec=HttpClient)
    client.post = MagicMock()
    return client


@pytest.fixture
def mock_token_manager():
    """Mock synchronous token manager with spec to satisfy Pydantic type checks."""
    manager = MagicMock(spec=TokenManager)
    manager.get_token.return_value = "mocked_access_token"
    return manager


@pytest.fixture
def swap_client(mock_http_client, mock_token_manager):
    """Fixture providing a synchronous Swap client instance."""
    return Swap(
        http_client=mock_http_client,
        token_manager=mock_token_manager,
    )


@pytest.fixture
def valid_swap_request():
    """Return a valid SwapRequest instance."""
    return SwapRequest(customerNumber="254722000000")


@pytest.fixture
def mock_success_response():
    """Sample successful API response dictionary (default 1900 date)."""
    return {
        "requestRefID": "4277-415525-1",
        "responseCode": "200",
        "responseDesc": "Success",
        "lastSwapDate": "01-01-1900 00:00",
    }


@pytest.fixture
def mock_swapped_response():
    """Sample successful API response dictionary for a recently swapped SIM."""
    return {
        "requestRefID": "4277-415525-2",
        "responseCode": "200",
        "responseDesc": "Success",
        "lastSwapDate": "15-08-2026 10:30",
    }


def make_mock_exception(code: str, message: str) -> MpesaApiException:
    """Helper to construct MpesaApiException with an MpesaError mock payload."""
    mock_error = MagicMock()
    mock_error.error_code = code
    mock_error.error_message = message
    mock_error.__str__.return_value = f"[{code}] {message}"
    return MpesaApiException(mock_error)


def test_swap_request_valid_phone():
    """Test that a valid phone number normalizes properly to 254 format string."""
    req = SwapRequest(customerNumber="0722000000")
    assert req.customerNumber == "254722000000"


def test_swap_request_invalid_phone():
    """Test that an invalid phone number raises ValueError during validation."""
    with pytest.raises(ValueError, match="Invalid Kenyan MSISDN"):
        SwapRequest(customerNumber="12345")


def test_swap_response_helper_properties(mock_success_response, mock_swapped_response):
    """Test SwapResponse helper properties like is_successful and is_recently_swapped."""
    response = SwapResponse(**mock_success_response)
    assert response.is_successful is True
    assert response.is_recently_swapped is False


    swapped_response = SwapResponse(**mock_swapped_response)
    assert swapped_response.is_successful is True
    assert swapped_response.is_recently_swapped is True


def test_swap_response_failed_status():
    """Test that is_recently_swapped evaluates to False on unsuccessful response codes."""
    failed_response = SwapResponse(
        requestRefID="4277-415525-3",
        responseCode="500",
        responseDesc="Internal Server Error",
        lastSwapDate="15-08-2026 10:30",
    )
    assert failed_response.is_successful is False
    assert failed_response.is_recently_swapped is False


def test_swap_request_success(
    swap_client,
    mock_http_client,
    mock_token_manager,
    valid_swap_request,
    mock_success_response,
):
    """Test successful synchronous Swap request execution using endpoint path."""
    mock_http_client.post.return_value = mock_success_response

    response = swap_client.swap_request(valid_swap_request)

    assert isinstance(response, SwapResponse)
    assert response.responseCode == "200"
    assert response.is_successful is True

    mock_http_client.post.assert_called_once_with(
        SWAP_ENDPOINT,
        json={"customerNumber": "254722000000"},
        headers={
            "Authorization": "Bearer mocked_access_token",
            "Content-Type": "application/json",
        },
    )


def test_swap_request_http_error(swap_client, mock_http_client, valid_swap_request):
    """Test synchronous POST request propagates MpesaApiException on HTTP failure."""
    api_error = make_mock_exception("HTTP_400", "Bad Request")
    mock_http_client.post.side_effect = api_error

    with pytest.raises(MpesaApiException) as exc:
        swap_client.swap_request(valid_swap_request)

    assert exc.value.error_code == "HTTP_400"
    assert "Bad Request" in exc.value.error.error_message


@pytest.fixture
def mock_async_token_manager():
    """Mock asynchronous token manager with spec to satisfy Pydantic type checks."""
    manager = MagicMock(spec=AsyncTokenManager)
    manager.get_token = AsyncMock(return_value="mocked_async_access_token")
    return manager


@pytest.fixture
def mock_async_http_client():
    """Mock asynchronous HTTP client with spec to satisfy Pydantic type checks."""
    client = MagicMock(spec=AsyncHttpClient)
    client.post = AsyncMock()
    return client


@pytest.fixture
def async_swap_client(mock_async_http_client, mock_async_token_manager):
    """Fixture providing an asynchronous Swap client instance."""
    return AsyncSwap(
        http_client=mock_async_http_client,
        token_manager=mock_async_token_manager,
    )


@pytest.mark.asyncio
async def test_async_swap_request_success(
    async_swap_client,
    mock_async_http_client,
    mock_async_token_manager,
    valid_swap_request,
    mock_success_response,
):
    """Test successful asynchronous Swap request execution using endpoint path."""
    mock_async_http_client.post.return_value = mock_success_response

    response = await async_swap_client.swap_request(valid_swap_request)

    assert isinstance(response, SwapResponse)
    assert response.responseCode == "200"
    assert response.is_successful is True

    mock_async_http_client.post.assert_called_once_with(
        SWAP_ENDPOINT,
        json={"customerNumber": "254722000000"},
        headers={
            "Authorization": "Bearer mocked_async_access_token",
            "Content-Type": "application/json",
        },
    )


@pytest.mark.asyncio
async def test_async_swap_request_http_error(
    async_swap_client, mock_async_http_client, valid_swap_request
):
    """Test asynchronous POST request propagates MpesaApiException on HTTP failure."""
    api_error = make_mock_exception("HTTP_401", "Unauthorized")
    mock_async_http_client.post.side_effect = api_error

    with pytest.raises(MpesaApiException) as exc:
        await async_swap_client.swap_request(valid_swap_request)

    assert exc.value.error_code == "HTTP_401"
    assert "Unauthorized" in exc.value.error.error_message
