"""Unit tests for the DynamicQRCodeService class in mpesakit.services.dynamic_qr module."""

import pytest
from mpesakit.services.dynamic_qr import (
    DynamicQRCodeService,
    AsyncDynamicQRCodeService,
)
from mpesakit.dynamic_qr_code.schemas import (
    DynamicQRGenerateResponse,
)


pytest_plugins = ["tests.unit.dynamic_qr_code.conftest"]


@pytest.mark.parametrize("service", [DynamicQRCodeService, AsyncDynamicQRCodeService])
def test_dynamic_qr_service_initializes_with_service_dependencies(
    service,
    mock_http_client,
    mock_token_manager,
):
    """Test DynamicQRCodeService initializes with correct arguments."""
    instance = service(http_client=mock_http_client, token_manager=mock_token_manager)

    assert isinstance(instance.http_client, type(mock_http_client))
    assert isinstance(instance.token_manager, type(mock_token_manager))


def test_generate_success(
    dynamic_qr_service,
    mock_http_client,
    generate_qr_request,
    generate_qr_success_response,
):
    """Test successful generation of a dynamic QR code."""
    response = generate_qr_success_response()
    mock_http_client.post.return_value = response.model_dump(by_alias=True)

    request = generate_qr_request()
    response = dynamic_qr_service.generate(request)

    assert isinstance(response, DynamicQRGenerateResponse)
    assert response.is_successful is True

    # Adjust the response class if needed
    # Validate QRCode string is in response body.
    assert hasattr(response, "QRCode") or hasattr(response, "qr_code")
    assert (
        getattr(response, "QRCode", None) == "base64-encoded-string"
        or getattr(response, "qr_code", None) == "base64-encoded-string"
    )
    mock_http_client.post.assert_called_once()
    # Validate auth header present in successful request(?)
    args, kwargs = mock_http_client.post.call_args
    assert "Authorization" in kwargs["headers"]
    assert kwargs["headers"]["Authorization"] == "Bearer test_token"


@pytest.mark.asyncio
async def test_async_generate_success(
    async_dynamic_qr_service,
    mock_async_http_client,
    generate_qr_request,
    generate_qr_success_response,
):
    """Test successful async generation of a dynamic QR code."""
    response = generate_qr_success_response()
    mock_async_http_client.post.return_value = response.model_dump(by_alias=True)

    request = generate_qr_request()
    response = await async_dynamic_qr_service.generate(request)
    assert isinstance(response, DynamicQRGenerateResponse)
    assert response.is_successful is True
    assert (
        getattr(response, "QRCode", None) == "base64-encoded-string"
        or getattr(response, "qr_code", None) == "base64-encoded-string"
    )
    mock_async_http_client.post.assert_called_once()
    args, kwargs = mock_async_http_client.post.call_args
    assert "Authorization" in kwargs["headers"]
    assert kwargs["headers"]["Authorization"] == "Bearer test_token"


def test_generate_dynamic_qr_handles_http_error(
    dynamic_qr_service, mock_http_client, generate_qr_request
):
    """Test that an HTTP error during Dynamic QR Code generation is handled."""
    mock_http_client.post.side_effect = Exception("HTTP error")

    with pytest.raises(Exception) as excinfo:
        request = generate_qr_request()
        dynamic_qr_service.generate(request)
    assert "HTTP error" in str(excinfo.value)


@pytest.mark.asyncio
async def test_async_generate_dynamic_qr_handles_http_error(
    async_dynamic_qr_service, mock_async_http_client, generate_qr_request
):
    """Test that an HTTP error during async Dynamic QR Code generation is handled."""
    mock_async_http_client.post.side_effect = Exception("Async HTTP error")

    with pytest.raises(Exception) as excinfo:
        request = generate_qr_request()
        await async_dynamic_qr_service.generate(request)
    assert "Async HTTP error" in str(excinfo.value)


def test_generate_dynamic_qr_string_response_code_no_type_error(
    dynamic_qr_service,
    mock_http_client,
    generate_qr_request,
    generate_qr_success_response,
):
    """Ensure ResponseCode as a string does not cause type comparison errors in is_successful."""
    # ResponseCode provided as a string (common in some APIs)
    response = generate_qr_success_response()
    mock_http_client.post.return_value = response.model_dump(by_alias=True)

    # Should not raise a TypeError when evaluating is_successful
    request = generate_qr_request()
    response = dynamic_qr_service.generate(request)
    assert response.is_successful is True


@pytest.mark.asyncio
async def test_async_generate_dynamic_qr_token_manager_called(
    async_dynamic_qr_service,
    mock_async_token_manager,
    mock_async_http_client,
    generate_qr_request,
    generate_qr_success_response,
):
    """Test that the async token manager's get_token is properly awaited."""
    response = generate_qr_success_response()
    mock_async_http_client.post.return_value = response.model_dump(by_alias=True)

    request = generate_qr_request()
    await async_dynamic_qr_service.generate(request)

    mock_async_token_manager.get_token.assert_awaited_once()
