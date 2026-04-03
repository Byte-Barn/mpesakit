"""Unit tests for the MpesaAsyncHttpClient HTTP client.

This module tests the MpesaAsyncHttpClient class for correct base URL selection,
asynchronous HTTP POST and GET request handling, and error handling for various scenarios.
"""

import pytest
from unittest.mock import Mock, AsyncMock, patch
import httpx

from mpesakit.http_client.mpesa_async_http_client import MpesaAsyncHttpClient
from mpesakit.errors import MpesaApiException



@pytest.fixture
def async_client():
    """Fixture to provide a MpesaAsyncHttpClient instance in sandbox environment."""
    with patch("mpesakit.http_client.mpesa_async_http_client.httpx.AsyncClient"):
        client = MpesaAsyncHttpClient(env="sandbox")
        yield client


def test_base_url_sandbox():
    """Test that the base URL is correct for the sandbox environment."""
    client = MpesaAsyncHttpClient(env="sandbox")
    assert client.base_url == "https://sandbox.safaricom.co.ke"


def test_base_url_production():
    """Test that the base URL is correct for the production environment."""
    client = MpesaAsyncHttpClient(env="production")
    assert client.base_url == "https://api.safaricom.co.ke"



@pytest.mark.asyncio
async def test_post_success(async_client):
    """Test successful ASYNC POST request returns expected JSON."""
    with patch.object(async_client._client, "post", new_callable=AsyncMock) as mock_post:

        mock_response = Mock(status_code=200, is_success=True)
        mock_response.json.return_value = {"foo": "bar"}
        mock_post.return_value = mock_response


        result = await async_client.post("/test", json={"a": 1}, headers={"h": "v"})

        assert result == {"foo": "bar"}
        mock_post.assert_called_once()
        mock_post.assert_called_with("/test",
         json={"a": 1},
         headers={"h": "v"},
           timeout=10)


@pytest.mark.asyncio
async def test_post_http_error(async_client):
    """Test ASYNC POST request returns MpesaApiException on HTTP error."""
    with patch.object(async_client._client, "post", new_callable=AsyncMock) as mock_post:
        mock_response = Mock(status_code=400, is_success=False)
        mock_response.json.return_value = {"errorMessage": "Bad Async Request"}
        mock_post.return_value = mock_response

        with pytest.raises(MpesaApiException) as exc:
            await async_client.post("/fail", json={}, headers={})

        assert exc.value.error.error_code == "HTTP_400"
        assert "Bad Async Request" in exc.value.error.error_message


@pytest.mark.asyncio
async def test_post_json_decode_error(async_client):
    """Test ASYNC POST request handles JSON decode error gracefully on HTTP error."""
    with patch.object(async_client._client, "post", new_callable=AsyncMock) as mock_post:
        mock_response = Mock(status_code=500, is_success=False)
        mock_response.json.side_effect = ValueError()
        mock_response.text = "Internal Server Error"
        mock_post.return_value = mock_response

        with pytest.raises(MpesaApiException) as exc:
            await async_client.post("/fail", json={}, headers={})

        assert exc.value.error.error_code == "HTTP_500"
        assert "Internal Server Error" in exc.value.error.error_message



@pytest.mark.asyncio
async def test_post_timeout(async_client):
    """Test ASYNC POST request raises MpesaApiException on timeout."""
    with patch.object(
        async_client._client,
        "post",
        new_callable=AsyncMock,
        side_effect=httpx.TimeoutException("timeout"),
    ):
        with pytest.raises(MpesaApiException) as exc:
            await async_client.post("/timeout", json={}, headers={})

        assert exc.value.error.error_code == "REQUEST_TIMEOUT"


@pytest.mark.asyncio
async def test_post_connection_error(async_client):
    """Test ASYNC POST request raises MpesaApiException on connection error."""
    with patch.object(
        async_client._client,
        "post",
        new_callable=AsyncMock,
        side_effect=httpx.ConnectError("conn error", request=Mock()),
    ):
        with pytest.raises(MpesaApiException) as exc:
            await async_client.post("/conn", json={}, headers={})

        assert exc.value.error.error_code == "CONNECTION_ERROR"


@pytest.mark.asyncio
async def test_post_generic_httpx_error(async_client):
    """Test ASYNC POST request raises MpesaApiException on generic httpx error."""
    with patch.object(
        async_client._client,
        "post",
        new_callable=AsyncMock,
        side_effect=httpx.ProtocolError("protocol error"),
    ):
        with pytest.raises(MpesaApiException) as exc:
            await async_client.post("/error", json={}, headers={})

        assert exc.value.error.error_code == "REQUEST_FAILED"
        assert "protocol error" in exc.value.error.error_message

@pytest.mark.asyncio
async def test_post_idempotency_key_persistence(async_client):
    """Test that the same idempotency key is reused across retries."""
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.is_success = True
    mock_response.json.return_value = {"Result": "Success"}

    with patch.object(
        async_client._client,
        "post",
        side_effect=[httpx.ConnectError("Fail 1"), httpx.ConnectError("Fail 2"),mock_response],
        new_callable=AsyncMock,
        return_value=mock_response
    ) as mock_post:

        await async_client.post("/stkpush",json={"Amount":1},headers={},idempotent=True)
        assert mock_post.call_count == 3

        first_call_headers = mock_post.call_args_list[0].kwargs['headers']
        third_call_headers = mock_post.call_args_list[2].kwargs['headers']

        assert "X-Idempotency-Key" in first_call_headers
        assert first_call_headers["X-Idempotency-Key"] == third_call_headers["X-Idempotency-Key"]

@pytest.mark.asyncio
async def test_post_does_not_retry_on_400(async_client):
    """Test that 400 errors fail immediately without retrying."""
    mock_response = Mock()
    mock_response.status_code = 400
    mock_response.is_success = False
    mock_response.json.return_value={"errorMessage":"Invalid MSISDN"}

    with patch.object(async_client._client,"post",new_callable=AsyncMock,return_value=mock_response) as mock_post:
            with pytest.raises(MpesaApiException):
                await async_client.post("/stkpush",json={"bad":"data"})

            assert mock_post.call_count == 1

@pytest.mark.asyncio
async def test_post_retries_until_stop_limit(async_client):
    """Verify that the client attempts 3 times for connection errors."""
    with patch.object(
        async_client._client,
        "post",
        side_effect=httpx.ConnectError("Dead pipe")
    )as mock_post:

        with pytest.raises(MpesaApiException):
            await async_client.post("/any",json={},idempotent=True)

        assert mock_post.call_count == 3
@pytest.mark.asyncio
async def test_get_success(async_client):
    """Test successful ASYNC GET request returns expected JSON."""
    with patch.object(async_client._client, "get", new_callable=AsyncMock) as mock_get:
        mock_response = Mock(status_code=200, is_success=True)
        mock_response.json.return_value = {"foo": "bar"}
        mock_get.return_value = mock_response

        result = await async_client.get("/test", params={"a": 1}, headers={"h": "v"})

        assert result == {"foo": "bar"}
        mock_get.assert_called_once()
        mock_get.assert_called_with("/test", params={"a": 1}, headers={"h": "v"}, timeout=10)


@pytest.mark.asyncio
async def test_get_http_error(async_client):
    """Test ASYNC GET request returns MpesaApiException on HTTP error."""
    with patch.object(async_client._client, "get", new_callable=AsyncMock) as mock_get:
        mock_response = Mock(status_code=404, is_success=False)
        mock_response.json.return_value = {"errorMessage": "Async Not Found"}
        mock_get.return_value = mock_response

        with pytest.raises(MpesaApiException) as exc:
            await async_client.get("/fail")

        assert exc.value.error.error_code == "HTTP_404"
        assert "Async Not Found" in exc.value.error.error_message

@pytest.mark.asyncio
async def test_get_timeout(async_client):
    """Test ASYNC GET request raises MpesaApiException on timeout."""
    with patch.object(
        async_client._client,
        "get",
        new_callable=AsyncMock,
        side_effect=httpx.TimeoutException("Test Timeout"),
    ):
        with pytest.raises(MpesaApiException) as exc:
            await async_client.get("/timeout")

        assert exc.value.error.error_code == "REQUEST_TIMEOUT"
        assert "Test Timeout" in exc.value.error.error_message


@pytest.mark.asyncio
async def test_get_connection_error(async_client):
    """Test ASYNC GET request raises MpesaApiException on connection error."""
    with patch.object(
        async_client._client,
        "get",
        new_callable=AsyncMock,
        side_effect=httpx.ConnectError("conn error", request=Mock()), # Use httpx's ConnectError
    ):
        with pytest.raises(MpesaApiException) as exc:
            await async_client.get("/conn")

        assert exc.value.error.error_code == "CONNECTION_ERROR"
        assert "conn error" in exc.value.error.error_message


@pytest.mark.asyncio
async def test_get_generic_httpx_error(async_client):
    """Test ASYNC GET request raises MpesaApiException on a generic httpx error."""
    with patch.object(
        async_client._client,
        "get",
        new_callable=AsyncMock,
        side_effect=httpx.ProtocolError("protocol error"),
    ):
        with pytest.raises(MpesaApiException) as exc:
            await async_client.get("/error")

        assert exc.value.error.error_code == "REQUEST_FAILED"
        assert "protocol error" in exc.value.error.error_message
