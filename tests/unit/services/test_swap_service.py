"""Unit tests for SwapService and AsyncSwapService Facades."""

from unittest.mock import AsyncMock, MagicMock
import pytest

from mpesakit.auth import AsyncTokenManager, TokenManager
from mpesakit.errors import MpesaApiException
from mpesakit.http_client import AsyncHttpClient, HttpClient
from mpesakit.services.swap import AsyncSwapService, SwapService
from mpesakit.swap import AsyncSwap, Swap, SwapRequest, SwapResponse


@pytest.fixture(params=["sandbox", "production"])
def env(request):
    """Parametrized fixture providing both sandbox and production environments."""
    return request.param


@pytest.fixture
def mock_http_client():
    """Mock that passes Pydantic instance check."""
    client = MagicMock(spec=HttpClient)
    client.post = MagicMock()
    return client


@pytest.fixture
def mock_token_manager():
    """Mock that passes Pydantic instance check."""
    manager = MagicMock(spec=TokenManager)
    manager.get_token.return_value = "mocked_access_token"
    return manager


@pytest.fixture
def swap_client(mock_http_client, mock_token_manager, env):
    """Constructs SwapService bypassing Pydantic checks strictly in test setup."""
    service = SwapService.__new__(SwapService)
    service.http_client = mock_http_client
    service.token_manager = mock_token_manager
    service.environment = env
    service._swap = Swap.model_construct(
        http_client=mock_http_client,
        token_manager=mock_token_manager,
        environment=env,
    )
    return service


@pytest.fixture
def async_swap_client(mock_async_http_client, mock_async_token_manager, env):
    """Constructs AsyncSwapService bypassing Pydantic checks strictly in test setup."""
    service = AsyncSwapService.__new__(AsyncSwapService)
    service.http_client = mock_async_http_client
    service.token_manager = mock_async_token_manager
    service.environment = env
    service._swap = AsyncSwap.model_construct(
        http_client=mock_async_http_client,
        token_manager=mock_async_token_manager,
        environment=env,
    )
    return service


@pytest.fixture
def mock_success_response():
    """Sample successful API response dictionary."""
    return {
        "requestRefID": "4277-415525-1",
        "responseCode": "200",
        "responseDesc": "Success",
        "lastSwapDate": "01-01-1900 00:00",
    }


def make_mock_exception(code: str, message: str) -> MpesaApiException:
    """Helper to construct MpesaApiException with mock payload."""
    mock_error = MagicMock()
    mock_error.error_code = code
    mock_error.error_message = message
    mock_error.__str__.return_value = f"[{code}] {message}"
    return MpesaApiException(mock_error)


def test_swap_query_success(swap_client, mock_http_client, mock_success_response, env):
    """Test facade swap_query method normalizes string phone and calls HTTP client."""
    mock_http_client.post.return_value = mock_success_response

    response = swap_client.swap_query("0722000000")

    assert isinstance(response, SwapResponse)
    assert response.is_successful is True

    expected_domain = (
        "sandbox.safaricom.co.ke" if env == "sandbox" else "api.safaricom.co.ke"
    )
    mock_http_client.post.assert_called_once_with(
        f"https://{expected_domain}/imsi/v2/checkATI",
        json={"customerNumber": "254722000000"},
        headers={
            "Authorization": "Bearer mocked_access_token",
            "Content-Type": "application/json",
        },
    )


def test_swap_request_direct_model_success(
    swap_client, mock_http_client, mock_success_response, env
):
    """Test facade swap_request method accepts SwapRequest model directly."""
    mock_http_client.post.return_value = mock_success_response
    request_model = SwapRequest(customerNumber="254711000000")

    response = swap_client.swap_request(request_model)

    assert isinstance(response, SwapResponse)
    assert response.is_successful is True

    expected_domain = (
        "sandbox.safaricom.co.ke" if env == "sandbox" else "api.safaricom.co.ke"
    )
    mock_http_client.post.assert_called_once_with(
        f"https://{expected_domain}/imsi/v2/checkATI",
        json={"customerNumber": "254711000000"},
        headers={
            "Authorization": "Bearer mocked_access_token",
            "Content-Type": "application/json",
        },
    )


def test_swap_query_invalid_phone_raises_validation_error(swap_client):
    """Test client-side validation fails before network call on invalid phone format."""
    with pytest.raises(ValueError, match="Invalid Kenyan MSISDN"):
        swap_client.swap_query("12345")


def test_swap_query_http_error(swap_client, mock_http_client):
    """Test error propagation through synchronous facade layer."""
    mock_http_client.post.side_effect = make_mock_exception("HTTP_400", "Bad Request")

    with pytest.raises(MpesaApiException) as exc:
        swap_client.swap_query("0722000000")

    assert exc.value.error_code == "HTTP_400"
    assert "Bad Request" in exc.value.error.error_message


@pytest.fixture
def mock_async_http_client():
    """Mock that passes Pydantic instance check."""
    client = MagicMock(spec=AsyncHttpClient)
    client.post = AsyncMock()
    return client


@pytest.fixture
def mock_async_token_manager():
    """Mock that passes Pydantic instance check."""
    manager = MagicMock(spec=AsyncTokenManager)
    manager.get_token = AsyncMock(return_value="mocked_async_access_token")
    return manager


@pytest.mark.asyncio
async def test_async_swap_query_success(
    async_swap_client, mock_async_http_client, mock_success_response, env
):
    """Test async facade swap_query method normalizes string phone and calls async client."""
    mock_async_http_client.post.return_value = mock_success_response

    response = await async_swap_client.swap_query("0722000000")

    assert isinstance(response, SwapResponse)
    assert response.is_successful is True

    expected_domain = (
        "sandbox.safaricom.co.ke" if env == "sandbox" else "api.safaricom.co.ke"
    )
    mock_async_http_client.post.assert_called_once_with(
        f"https://{expected_domain}/imsi/v2/checkATI",
        json={"customerNumber": "254722000000"},
        headers={
            "Authorization": "Bearer mocked_async_access_token",
            "Content-Type": "application/json",
        },
    )


@pytest.mark.asyncio
async def test_async_swap_request_direct_model_success(
    async_swap_client, mock_async_http_client, mock_success_response, env
):
    """Test async facade swap_request method accepts SwapRequest model directly."""
    mock_async_http_client.post.return_value = mock_success_response
    request_model = SwapRequest(customerNumber="254711000000")

    response = await async_swap_client.swap_request(request_model)

    assert isinstance(response, SwapResponse)
    assert response.is_successful is True

    expected_domain = (
        "sandbox.safaricom.co.ke" if env == "sandbox" else "api.safaricom.co.ke"
    )
    mock_async_http_client.post.assert_called_once_with(
        f"https://{expected_domain}/imsi/v2/checkATI",
        json={"customerNumber": "254711000000"},
        headers={
            "Authorization": "Bearer mocked_async_access_token",
            "Content-Type": "application/json",
        },
    )


@pytest.mark.asyncio
async def test_async_swap_query_invalid_phone_raises_validation_error(
    async_swap_client,
):
    """Test async client-side validation fails before network call on invalid phone format."""
    with pytest.raises(ValueError, match="Invalid Kenyan MSISDN"):
        await async_swap_client.swap_query("invalid_phone")


@pytest.mark.asyncio
async def test_async_swap_query_http_error(async_swap_client, mock_async_http_client):
    """Test error propagation through asynchronous facade layer."""
    mock_async_http_client.post.side_effect = make_mock_exception(
        "HTTP_401", "Unauthorized"
    )

    with pytest.raises(MpesaApiException) as exc:
        await async_swap_client.swap_query("0722000000")

    assert exc.value.error_code == "HTTP_401"
    assert "Unauthorized" in exc.value.error.error_message
