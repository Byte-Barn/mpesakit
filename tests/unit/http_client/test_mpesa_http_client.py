"""Unit tests for the MpesaHttpClient HTTP client.

This module tests the MpesaHttpClient class for correct base URL selection,
HTTP POST and GET request handling, and error handling for various scenarios.
"""

from unittest.mock import Mock, patch

import httpx
import pytest
from httpx import Response

from mpesakit.errors import MpesaApiException
from mpesakit.http_client.mpesa_http_client import MpesaHttpClient


@pytest.fixture(params=[True, False])
def client(request):
    """Fixture to provide a MpesaHttpClient instance in sandbox environment both in session and non-session modes."""
    use_session = request.param
    client = MpesaHttpClient(env="sandbox", use_session=use_session)

    try:
        yield client
    finally:
        if client._client:
            client.close()


def test_base_url_sandbox():
    """Test that the base URL is correct for the sandbox environment."""
    client = MpesaHttpClient(env="sandbox")
    assert client.base_url == "https://sandbox.safaricom.co.ke"


def test_base_url_production():
    """Test that the base URL is correct for the production environment."""
    client = MpesaHttpClient(env="production")
    assert client.base_url == "https://api.safaricom.co.ke"


def test_post_success(client):
    """Test successful POST request returns expected JSON."""
    with patch.object(client, "_raw_post") as mock_raw_post:
        mock_response = Response(status_code=200, json={"foo": "bar"})
        mock_raw_post.return_value = mock_response

        result = client.post("/test", json={"a": 1}, headers={"h": "v"})
        assert result == {"foo": "bar"}
        mock_raw_post.assert_called_once()


def test_post_http_error(client):
    """Test POST request returns MpesaApiException on HTTP error."""
    with patch.object(client, "_raw_post") as mock_raw_post:
        mock_response = Response(status_code=400, json={"errorMessage": "Bad Request"})
        mock_raw_post.return_value = mock_response

    with pytest.raises(MpesaApiException) as exc:
        client.post("/fail", json={}, headers={})

        assert exc.value.error.error_code == "HTTP_400"
        assert "Bad Request" in exc.value.error.error_message


def test_post_json_decode_error(client):
    """Test POST request handles JSON decode error gracefully."""
    with patch.object(client, "_raw_post") as mock_raw_post:
        mock_response = Mock()
        mock_response.status_code = 500
        mock_response.json.side_effect = ValueError()
        mock_response.text = "Internal Server Error"
        mock_raw_post.return_value = mock_response

        with pytest.raises(MpesaApiException) as exc:
            client.post("/fail", json={}, headers={})

            assert exc.value.error.error_code == "HTTP_500"
            assert "Internal Server Error" in exc.value.error.error_message


def test_post_request_exception_is_not_retried_and_raises_api_exception(client):
    """Test that a non-retryable exception immediately raises MpesaApiException."""
    with patch.object(client, "_raw_post", side_effect=httpx.RequestError("boom")):
        with pytest.raises(MpesaApiException) as exc:
            client.post("/fail", json={}, headers={})

        assert exc.value.error.error_code == "REQUEST_FAILED"


def test_post_retries_and_succeeds(client):
    """Test that a POST request succeeds after transient failures.

    This test ensures the retry mechanism works as intended.
    """
    with patch("httpx.Client.post") as mock_httpx_post:
        mock_httpx_post.side_effect = [
            httpx.TimeoutException("Read timed out."),
            httpx.TimeoutException("Read timed out."),
            Response(200, json={"ResultCode": 0}),
        ]

        result = client.post("/test", json={"a": 1}, headers={"h": "v"})

        assert mock_httpx_post.call_count == 3
        assert result == {"ResultCode": 0}


def test_post_fails_after_max_retries(client):
    """Test that a POST request raises an exception after all retries fail.

    This test ensures the retry mechanism eventually gives up.
    """
    with patch.object(client, "_raw_post") as mock_raw_post:
        mock_raw_post.side_effect = httpx.ConnectError("Connection failed.")

        with pytest.raises(MpesaApiException) as exc:
            client.post("/test", json={"a": 1}, headers={"h": "v"})

            assert mock_raw_post.call_count == 3
            assert exc.value.error.error_code == "CONNECTION_ERROR"


def test_get_success(client):
    """Test successful GET request returns expected JSON."""
    with patch.object(client, "_raw_get") as mock_raw_get:
        mock_response = Response(status_code=200, json={"foo": "bar"})
        mock_raw_get.return_value = mock_response

        result = client.get("/test", params={"a": 1}, headers={"h": "v"})
        assert result == {"foo": "bar"}
        mock_raw_get.assert_called_once()


def test_get_http_error(client):
    """Test GET request returns MpesaApiException on HTTP error."""
    with patch.object(client, "_raw_get") as mock_raw_get:
        mock_response = Response(status_code=404, json={"errorMessage": "Not Found"})
        mock_raw_get.return_value = mock_response

        with pytest.raises(MpesaApiException) as exc:
            client.get("/fail")

        assert exc.value.error.error_code == "HTTP_404"
        assert "Not Found" in exc.value.error.error_message


def test_get_json_decode_error(client):
    """Test GET request handles JSON decode error gracefully."""
    with patch.object(client, "_raw_get") as mock_raw_get:
        mock_response = Response(status_code=500, text="Internal Server Error")
        mock_response.json = Mock(side_effect=ValueError())
        mock_raw_get.return_value = mock_response

        with pytest.raises(MpesaApiException) as exc:
            client.get("/fail")

            assert exc.value.error.error_code == "HTTP_500"
            assert "Internal Server Error" in exc.value.error.error_message


def test_get_request_exception_is_not_retried_and_raises_api_exception(client):
    """Test that a non-retryable exception immediately raises MpesaApiException."""
    with patch.object(client, "_raw_get", side_effect=httpx.RequestError("boom")):
        with pytest.raises(MpesaApiException) as exc:
            client.get("/fail")

        assert exc.value.error.error_code == "REQUEST_FAILED"


def test_get_retries_and_succeeds(client):
    """Test that a GET request succeeds after transient failures."""
    with patch("httpx.Client.get") as mock_httpx_get:
        mock_httpx_get.side_effect = [
            httpx.TimeoutException("Read timed out."),
            httpx.TimeoutException("Read timed out."),
            Response(200, json={"ResultCode": 0}),
        ]

        result = client.get("/test")

        assert mock_httpx_get.call_count == 3
        assert result == {"ResultCode": 0}


def test_get_fails_after_max_retries(client):
    """Test that a GET request raises an exception after all retries fail."""
    with patch.object(client, "_raw_get") as mock_raw_get:
        mock_raw_get.side_effect = httpx.TimeoutException("Read timed out.")

        with pytest.raises(MpesaApiException) as exc:
            client.get("/test")

            assert mock_raw_get.call_count == 3
            assert exc.value.error.error_code == "REQUEST_TIMEOUT"
