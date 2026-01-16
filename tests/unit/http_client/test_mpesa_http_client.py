"""Unit tests for the MpesaHttpClient HTTP client.

This module tests the MpesaHttpClient class for correct base URL selection,
HTTP POST and GET request handling, and error handling for various scenarios.
"""

from unittest.mock import Mock, patch

import pytest
import requests

from mpesakit.errors import MpesaApiException
from mpesakit.http_client.mpesa_http_client import MpesaHttpClient


@pytest.fixture(params=[True, False])
def client(request):
    """Fixture to provide a MpesaHttpClient instance in sandbox environment both in session and non-session modes."""
    use_session = request.param
    return MpesaHttpClient(env="sandbox", use_session=use_session)


def get_patch_target(client, method):
    """A helper function to determine the correct patch target."""
    if client._session:
        return f"mpesakit.http_client.mpesa_http_client.requests.Session.{method}"
    else:
        return f"mpesakit.http_client.mpesa_http_client.requests.{method}"


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
    patch_target = get_patch_target(client, "post")

    with patch(patch_target) as mock_post:
        mock_response = Mock()
        mock_response.ok = True
        mock_response.json.return_value = {"foo": "bar"}
        mock_post.return_value = mock_response

        result = client.post("/test", json={"a": 1}, headers={"h": "v"})
        assert result == {"foo": "bar"}
        mock_post.assert_called_once()


def test_post_http_error(client):
    """Test POST request returns MpesaApiException on HTTP error."""
    patch_target = get_patch_target(client, "post")

    with patch(patch_target) as mock_post:
        mock_response = Mock()
        mock_response.ok = False
        mock_response.status_code = 400
        mock_response.json.return_value = {"errorMessage": "Bad Request"}
        mock_post.return_value = mock_response

        with pytest.raises(MpesaApiException) as exc:
            client.post("/fail", json={}, headers={})
        assert exc.value.error.error_code == "HTTP_400"
        assert "Bad Request" in exc.value.error.error_message


def test_post_json_decode_error(client):
    """Test POST request handles JSON decode error gracefully."""
    patch_target = get_patch_target(client, "post")

    with patch(patch_target) as mock_post:
        mock_response = Mock()
        mock_response.ok = False
        mock_response.status_code = 500
        mock_response.json.side_effect = ValueError()
        mock_response.text = "Internal Server Error"
        mock_post.return_value = mock_response

        with pytest.raises(MpesaApiException) as exc:
            client.post("/fail", json={}, headers={})
        assert exc.value.error.error_code == "HTTP_500"
        assert "Internal Server Error" in exc.value.error.error_message


def test_post_request_exception_is_not_retried_and_raises_api_exception(client):
    """Test that a non-retryable exception immediately raises MpesaApiException."""
    patch_target = get_patch_target(client, "post")

    with patch(
        patch_target,
        side_effect=requests.RequestException("boom"),
    ):
        with pytest.raises(MpesaApiException) as exc:
            client.post("/fail", json={}, headers={})

        assert exc.value.error.error_code == "REQUEST_FAILED"


def test_post_retries_and_succeeds(client):
    """Test that a POST request succeeds after transient failures.

    This test ensures the retry mechanism works as intended.
    """
    patch_target = get_patch_target(client, "post")

    with patch(patch_target) as mock_post:
        mock_success_response = Mock()
        mock_success_response.ok = True
        mock_success_response.json.return_value = {"ResultCode": 0}

        mock_post.side_effect = [
            requests.exceptions.Timeout("Read timed out."),
            requests.exceptions.Timeout("Read timed out."),
            mock_success_response,
        ]

        result = client.post("/test", json={"a": 1}, headers={"h": "v"})

        assert mock_post.call_count == 3
        assert result == {"ResultCode": 0}


def test_post_fails_after_max_retries(client):
    """Test that a POST request raises an exception after all retries fail.

    This test ensures the retry mechanism eventually gives up.
    """
    patch_target = get_patch_target(client, "post")

    with patch(patch_target) as mock_post:
        mock_post.side_effect = requests.exceptions.ConnectionError(
            "Connection failed."
        )

        with pytest.raises(MpesaApiException) as exc:
            client.post("/test", json={"a": 1}, headers={"h": "v"})

        assert mock_post.call_count == 3
        assert exc.value.error.error_code == "CONNECTION_ERROR"


def test_get_success(client):
    """Test successful GET request returns expected JSON."""
    patch_target = get_patch_target(client, "get")

    with patch(patch_target) as mock_get:
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"foo": "bar"}
        mock_get.return_value = mock_response

        result = client.get("/test", params={"a": 1}, headers={"h": "v"})
        assert result == {"foo": "bar"}
        mock_get.assert_called_once()


def test_get_http_error(client):
    """Test GET request returns MpesaApiException on HTTP error."""
    patch_target = get_patch_target(client, "get")

    with patch(patch_target) as mock_get:
        mock_response = Mock()
        mock_response.ok = False
        mock_response.status_code = 404
        mock_response.json.return_value = {"errorMessage": "Not Found"}
        mock_get.return_value = mock_response

        with pytest.raises(MpesaApiException) as exc:
            client.get("/fail")
        assert exc.value.error.error_code == "HTTP_404"
        assert "Not Found" in exc.value.error.error_message


def test_get_json_decode_error(client):
    """Test GET request handles JSON decode error gracefully."""
    patch_target = get_patch_target(client, "get")

    with patch(patch_target) as mock_get:
        mock_response = Mock()
        mock_response.ok = False
        mock_response.status_code = 500
        mock_response.json.side_effect = ValueError()
        mock_response.text = "Internal Server Error"
        mock_get.return_value = mock_response

        with pytest.raises(MpesaApiException) as exc:
            client.get("/fail")
        assert exc.value.error.error_code == "HTTP_500"
        assert "Internal Server Error" in exc.value.error.error_message


def test_get_request_exception_is_not_retried_and_raises_api_exception(client):
    """Test that a non-retryable exception immediately raises MpesaApiException."""
    patch_target = get_patch_target(client, "get")

    with patch(
        patch_target,
        side_effect=requests.RequestException("boom"),
    ):
        with pytest.raises(MpesaApiException) as exc:
            client.get("/fail")

        assert exc.value.error.error_code == "REQUEST_FAILED"


def test_get_retries_and_succeeds(client):
    """Test that a GET request succeeds after transient failures."""
    patch_target = get_patch_target(client, "get")

    with patch(patch_target) as mock_get:
        mock_success_response = Mock()
        mock_success_response.ok = True
        mock_success_response.json.return_value = {"ResultCode": 0}

        mock_get.side_effect = [
            requests.exceptions.ConnectionError("Connection failed."),
            mock_success_response,
        ]

        result = client.get("/test")

        assert mock_get.call_count == 2
        assert result == {"ResultCode": 0}


def test_get_fails_after_max_retries(client):
    """Test that a GET request raises an exception after all retries fail."""
    patch_target = get_patch_target(client, "get")

    with patch(patch_target) as mock_get:
        mock_get.side_effect = requests.exceptions.Timeout("Read timed out.")

        with pytest.raises(MpesaApiException) as exc:
            client.get("/test")

        assert mock_get.call_count == 3
        assert exc.value.error.error_code == "REQUEST_TIMEOUT"
