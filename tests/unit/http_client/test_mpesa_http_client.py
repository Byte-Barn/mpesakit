"""Unit tests for the MpesaHttpClient HTTP client.

This module tests the MpesaHttpClient class for correct base URL selection,
HTTP POST and GET request handling, and error handling for various scenarios.
"""

import httpx
import pytest
from unittest.mock import Mock, patch
from mpesakit.http_client.mpesa_http_client import MpesaHttpClient
from mpesakit.errors import MpesaApiException


@pytest.fixture
def client():
    """Fixture to provide a MpesaHttpClient instance in sandbox environment."""
    client = MpesaHttpClient(env="sandbox")
    return client


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
    with patch("mpesakit.http_client.mpesa_http_client.httpx.Client.post") as mock_post:
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"foo": "bar"}
        mock_post.return_value = mock_response

        result = client.post("/test", json={"a": 1}, headers={"h": "v"})
        assert result == {"foo": "bar"}
        mock_post.assert_called_once()


def test_post_http_error(client):
    """Test POST request returns MpesaApiException on HTTP error."""
    with patch("mpesakit.http_client.mpesa_http_client.httpx.Client.post") as mock_post:
        mock_response = Mock()
        mock_response.status_code = 400
        mock_response.json.return_value = {"errorMessage": "Bad Request"}
        mock_post.return_value = mock_response

        with pytest.raises(MpesaApiException) as exc:
            client.post("/fail", json={}, headers={})
        assert exc.value.error.error_code == "HTTP_400"
        assert "Bad Request" in exc.value.error.error_message


def test_post_json_decode_error(client):
    """Test POST request handles JSON decode error gracefully."""
    with patch("mpesakit.http_client.mpesa_http_client.httpx.Client.post") as mock_post:
        mock_response = Mock()
        mock_response.status_code = 500
        mock_response.json.side_effect = ValueError()
        mock_response.text = "Internal Server Error"
        mock_post.return_value = mock_response

        with pytest.raises(MpesaApiException) as exc:
            client.post("/fail", json={}, headers={})
        assert exc.value.error.error_code == "HTTP_500"
        assert "Internal Server Error" in exc.value.error.error_message


def test_post_request_exception(client):
    """Test POST request raises MpesaApiException on generic exception."""
    with patch(
        "mpesakit.http_client.mpesa_http_client.httpx.Client.post",
        side_effect=httpx.RequestError("boom"),
    ):
        with pytest.raises(MpesaApiException) as exc:
            client.post("/fail", json={}, headers={})
        assert exc.value.error.error_code == "REQUEST_FAILED"


def test_post_timeout(client):
    """Test POST request raises MpesaApiException on timeout."""
    with patch(
        "mpesakit.http_client.mpesa_http_client.httpx.Client.post",
        side_effect=httpx.TimeoutException,
    ):
        with pytest.raises(MpesaApiException) as exc:
            client.post("/timeout", json={}, headers={})
        assert exc.value.error.error_code == "REQUEST_TIMEOUT"


def test_post_connection_error(client):
    """Test POST request raises MpesaApiException on connection error."""
    with patch(
        "mpesakit.http_client.mpesa_http_client.httpx.Client.post",
        side_effect=httpx.ConnectError,
    ):
        with pytest.raises(MpesaApiException) as exc:
            client.post("/conn", json={}, headers={})
        assert exc.value.error.error_code == "CONNECTION_ERROR"


def test_get_success(client):
    """Test successful GET request returns expected JSON."""
    with patch("mpesakit.http_client.mpesa_http_client.httpx.Client.get") as mock_get:
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"foo": "bar"}
        mock_get.return_value = mock_response

        result = client.get("/test", params={"a": 1}, headers={"h": "v"})
        assert result == {"foo": "bar"}
        mock_get.assert_called_once()


def test_get_http_error(client):
    """Test GET request returns MpesaApiException on HTTP error."""
    with patch("mpesakit.http_client.mpesa_http_client.httpx.Client.get") as mock_get:
        mock_response = Mock()
        mock_response.status_code = 404
        mock_response.json.return_value = {"errorMessage": "Not Found"}
        mock_get.return_value = mock_response

        with pytest.raises(MpesaApiException) as exc:
            client.get("/fail")
        assert exc.value.error.error_code == "HTTP_404"
        assert "Not Found" in exc.value.error.error_message


def test_get_json_decode_error(client):
    """Test GET request handles JSON decode error gracefully."""
    with patch("mpesakit.http_client.mpesa_http_client.httpx.Client.get") as mock_get:
        mock_response = Mock()
        mock_response.status_code = 500
        mock_response.json.side_effect = ValueError()
        mock_response.text = "Internal Server Error"
        mock_get.return_value = mock_response

        with pytest.raises(MpesaApiException) as exc:
            client.get("/fail")
        assert exc.value.error.error_code == "HTTP_500"
        assert "Internal Server Error" in exc.value.error.error_message


def test_get_request_exception(client):
    """Test GET request raises MpesaApiException on generic exception."""
    with patch(
        "mpesakit.http_client.mpesa_http_client.httpx.Client.get",
        side_effect=httpx.RequestError("boom"),
    ):
        with pytest.raises(MpesaApiException) as exc:
            client.get("/fail")
        assert exc.value.error.error_code == "REQUEST_FAILED"


def test_get_timeout(client):
    """Test GET request raises MpesaApiException on timeout."""
    with patch(
        "mpesakit.http_client.mpesa_http_client.httpx.Client.get",
        side_effect=httpx.TimeoutException,
    ):
        with pytest.raises(MpesaApiException) as exc:
            client.get("/timeout")
        assert exc.value.error.error_code == "REQUEST_TIMEOUT"


def test_get_connection_error(client):
    """Test GET request raises MpesaApiException on connection error."""
    with patch(
        "mpesakit.http_client.mpesa_http_client.httpx.Client.get",
        side_effect=httpx.ConnectError,
    ):
        with pytest.raises(MpesaApiException) as exc:
            client.get("/conn")
        assert exc.value.error.error_code == "CONNECTION_ERROR"
