"""Unit tests for MCP Server module using FastMCP Client."""

import os
from unittest.mock import MagicMock, patch

import pytest
from fastmcp import Client

from mpesakit.mcp.server import (
    check_transaction_status,
    enforce_security_guard,
    get_client,
    mcp,
    stk_push,
    c2b_register_url,
    b2b_paybill,
)
from mpesakit.errors import MpesaApiException, MpesaError


@pytest.fixture
def mock_env():
    """Fixture to provide a standard set of environment variables."""
    env_vars = {
        "MPESA_CONSUMER_KEY": "test_key",
        "MPESA_CONSUMER_SECRET": "test_secret",
        "MPESA_ENV": "sandbox",
        "MPESA_BUSINESS_SHORTCODE": "174379",
        "MPESA_PASSKEY": "test_passkey",
        "MPESA_CALLBACK_URL": "https://example.com/callback",
        "MPESA_INITIATOR_NAME": "testapiuser",
        "MPESA_SECURITY_CREDENTIAL": "test_credential",
    }
    with patch.dict(os.environ, env_vars, clear=True):
        yield


def test_get_client(mock_env):
    """Test MpesaClient initialization."""
    client = get_client()
    assert client is not None


def test_get_client_missing_credentials():
    """Test MpesaClient initialization with missing credentials."""
    with patch.dict(os.environ, {}, clear=True):
        with pytest.raises(ValueError, match="Missing environment credentials"):
            get_client()


def test_enforce_security_guard_sandbox(mock_env):
    """Test security guard in sandbox."""
    # Should not raise an error
    enforce_security_guard("stk_push")


def test_enforce_security_guard_production_blocked():
    """Test security guard blocks mutations in production by default."""
    env_vars = {"MPESA_ENV": "production"}
    with patch.dict(os.environ, env_vars, clear=True):
        with pytest.raises(PermissionError, match="Security Exception"):
            enforce_security_guard("stk_push")


def test_enforce_security_guard_production_allowed():
    """Test security guard allows mutations in production if flag is set."""
    env_vars = {
        "MPESA_ENV": "production",
        "MPESA_MCP_ALLOW_LIVE_MUTATIONS": "true",
    }
    with patch.dict(os.environ, env_vars, clear=True):
        # Should not raise an error
        enforce_security_guard("stk_push")


@pytest.fixture
def mcp_server():
    """Returns the FastMCP server instance."""
    return mcp


@pytest.mark.asyncio
@patch("mpesakit.mcp.server.get_client")
async def test_stk_push_mcp_success(mock_get_client, mock_env, mcp_server):
    """Test successful stk_push tool execution via MCP client."""
    mock_client = MagicMock()
    mock_response = MagicMock()
    mock_response.model_dump.return_value = {"CheckoutRequestID": "ws_123"}
    mock_client.stk_push.return_value = mock_response
    mock_get_client.return_value = mock_client

    async with Client(mcp_server) as client:
        result = await client.call_tool(
            "stk_push", 
            {
                "phone_number": "254712345678", 
                "amount": 500, 
                "account_reference": "REF123", 
                "transaction_desc": "Test transaction"
            }
        )

    assert "ws_123" in str(result)
    mock_client.stk_push.assert_called_once_with(
        business_short_code=174379,
        transaction_type="CustomerPayBillOnline",
        amount=500,
        party_a="254712345678",
        party_b="174379",
        phone_number="254712345678",
        callback_url="https://example.com/callback",
        account_reference="REF123",
        transaction_desc="Test transaction",
        passkey="test_passkey",
    )


@pytest.mark.asyncio
@patch("mpesakit.mcp.server.get_client")
async def test_c2b_register_url_success(mock_get_client, mock_env, mcp_server):
    """Test C2B URL registration tool."""
    mock_client = MagicMock()
    mock_response = MagicMock()
    mock_response.model_dump.return_value = {"ResponseCode": "0"}
    mock_client.c2b.register_url.return_value = mock_response
    mock_get_client.return_value = mock_client

    async with Client(mcp_server) as client:
        result = await client.call_tool(
            "c2b_register_url", 
            {"response_type": "Completed"}
        )

    assert "ResponseCode" in str(result)
    mock_client.c2b.register_url.assert_called_once_with(
        short_code=174379,
        response_type="Completed",
        confirmation_url="https://example.com/callback",
        validation_url="https://example.com/callback",
    )


@pytest.mark.asyncio
@patch("mpesakit.mcp.server.get_client")
async def test_stk_push_mcp_api_error(mock_get_client, mock_env, mcp_server):
    """Test stk_push tool execution with API error via MCP client."""
    mock_client = MagicMock()
    mock_client.stk_push.side_effect = MpesaApiException(MpesaError(error_message="API Error"))
    mock_get_client.return_value = mock_client

    async with Client(mcp_server) as client:
        result = await client.call_tool(
            "stk_push", 
            {
                "phone_number": "254712345678", 
                "amount": 500, 
                "account_reference": "REF123", 
                "transaction_desc": "Test transaction"
            }
        )
    
    assert "failed" in str(result)
    assert "API Error" in str(result)


@pytest.mark.asyncio
@patch("mpesakit.mcp.server.get_client")
async def test_check_transaction_status_mcp_success(mock_get_client, mock_env, mcp_server):
    """Test successful check_transaction_status tool execution via MCP client."""
    mock_client = MagicMock()
    mock_response = MagicMock()
    mock_response.model_dump.return_value = {"ResultCode": "0"}
    mock_client.stk_query.return_value = mock_response
    mock_get_client.return_value = mock_client

    async with Client(mcp_server) as client:
        result = await client.call_tool(
            "check_transaction_status", 
            {"checkout_request_id": "ws_123"}
        )

    assert "ResultCode" in str(result)
    assert "0" in str(result)
    mock_client.stk_query.assert_called_once_with(
        business_short_code=174379,
        passkey="test_passkey",
        checkout_request_id="ws_123",
    )
