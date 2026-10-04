"""Unit tests for the IMSI / SIM Swap functionality of the Mpesa SDK."""

import pytest

from mpesakit.imsi import (
    IMSIRequest,
    IMSIV1Response,
    IMSIV2Response,
    IMSIV3Response,
)
from mpesakit.services import AsyncIMSIService,IMSIService


@pytest.fixture
def imsi_service(mock_http_client, mock_token_manager):
    """Fixture to create an instance of IMSIService with mocked dependencies."""
    return IMSIService(http_client=mock_http_client, token_manager=mock_token_manager)


@pytest.fixture
def async_imsi_service(mock_async_http_client, mock_async_token_manager):
    """Fixture to create an instance of AsyncIMSIService with mocked dependencies."""
    return AsyncIMSIService(
        http_client=mock_async_http_client, token_manager=mock_async_token_manager
    )



def test_imsi_request_msisdn_normalization():
    """Test that IMSIRequest normalizes local and international phone formats."""
    req1 = IMSIRequest(customerNumber="0722000000")
    assert req1.customerNumber == "254722000000"

    req2 = IMSIRequest(customerNumber="+254722000000")
    assert req2.customerNumber == "254722000000"

    req3 = IMSIRequest(customerNumber="254722000000")
    assert req3.customerNumber == "254722000000"




def test_imsi_v1_response_is_recently_swapped():
    """Test is_recently_swapped evaluation on IMSIV1Response."""
    resp_safe = IMSIV1Response(
        requestRefID="REF123",
        responseCode="200",
        responseDesc="Success",
        lastSwapDate="01-01-1900 00:00",
    )
    assert resp_safe.is_recently_swapped is False


    resp_swapped = IMSIV1Response(
        requestRefID="REF123",
        responseCode="200",
        responseDesc="Success",
        lastSwapDate="15-08-2024 14:30",
    )
    assert resp_swapped.is_recently_swapped is True


def test_imsi_v3_response_null_swap_fields():
    """Test that V3 response handles unpopulated swap attributes correctly."""
    resp = IMSIV3Response(
        requestRefID="REF123",
        imsi="hashed_imsi_string",
    )
    assert getattr(resp, "lastSwapDate", None) is None
    assert getattr(resp, "is_recently_swapped", None) is None

def test_query_v1_success(imsi_service, mock_http_client):
    """Test that V1 checkATI query completes successfully."""
    response_data = {
        "requestRefID": "REF_V1_001",
        "responseCode": "200",
        "responseDesc": "Success",
        "lastSwapDate": "01-01-1900 00:00",
    }
    mock_http_client.post.return_value = response_data

    response = imsi_service.query_v1("0722000000")

    assert isinstance(response, IMSIV1Response)
    assert response.requestRefID == "REF_V1_001"
    assert response.lastSwapDate == "01-01-1900 00:00"
    assert response.is_recently_swapped is False

    mock_http_client.post.assert_called_once()
    args, kwargs = mock_http_client.post.call_args
    assert args[0] == "/imsi/v1/checkATI"
    assert kwargs["headers"]["Authorization"] == "Bearer test_token"
    assert kwargs["json"]["customerNumber"] == "254722000000"


def test_query_v2_success(imsi_service, mock_http_client):
    """Test that V2 checkATI query completes successfully."""
    response_data = {
        "requestRefID": "REF_V2_001",
        "responseCode": "200",
        "msisdnRegistrationDate": "10-05-2018 10:00",
    }
    mock_http_client.post.return_value = response_data

    response = imsi_service.query_v2("0722000000")

    assert isinstance(response, IMSIV2Response)
    assert response.requestRefID == "REF_V2_001"
    assert response.msisdnRegistrationDate == "10-05-2018 10:00"

    mock_http_client.post.assert_called_once()
    args, _ = mock_http_client.post.call_args
    assert args[0] == "/imsi/v2/checkATI"


def test_query_v3_success(imsi_service, mock_http_client):
    """Test that V3 checkATI query completes successfully."""
    response_data = {
        "requestRefID": "REF_V3_001",
        "imsi": "abc123hashedimsi",
        "lastSwapDate": "12-02-2025 18:00",
    }
    mock_http_client.post.return_value = response_data

    response = imsi_service.query_v3("0722000000")

    assert isinstance(response, IMSIV3Response)
    assert response.requestRefID == "REF_V3_001"
    assert response.imsi == "abc123hashedimsi"
    assert getattr(response,"is_recently_swapped",None) is None

    mock_http_client.post.assert_called_once()
    args, _ = mock_http_client.post.call_args
    assert args[0] == "/imsi/v3/checkATI"


def test_imsi_query_handles_http_error(imsi_service, mock_http_client):
    """Test that IMSI service handles HTTP errors gracefully."""
    mock_http_client.post.side_effect = Exception("HTTP 500 Internal Error")

    with pytest.raises(Exception) as excinfo:
        imsi_service.query_v1("0722000000")

    assert "HTTP 500 Internal Error" in str(excinfo.value)



@pytest.mark.asyncio
async def test_async_query_v1_success(async_imsi_service, mock_async_http_client):
    """Test that async V1 checkATI query completes successfully."""
    response_data = {
        "requestRefID": "ASYNC_REF_V1",
        "responseCode": "200",
        "responseDesc": "Success",
        "lastSwapDate": "01-01-1900 00:00",
    }
    mock_async_http_client.post.return_value = response_data

    response = await async_imsi_service.query_v1("0722000000")

    assert isinstance(response, IMSIV1Response)
    assert response.requestRefID == "ASYNC_REF_V1"
    assert response.is_recently_swapped is False

    mock_async_http_client.post.assert_called_once()
    args, kwargs = mock_async_http_client.post.call_args
    assert args[0] == "/imsi/v1/checkATI"
    assert kwargs["headers"]["Authorization"] == "Bearer test_token"
    assert kwargs["json"]["customerNumber"] == "254722000000"


@pytest.mark.asyncio
async def test_async_query_v2_success(async_imsi_service, mock_async_http_client):
    """Test that async V2 checkATI query completes successfully."""
    response_data = {
        "requestRefID": "ASYNC_REF_V2",
        "responseCode": "200",
        "msisdnRegistrationDate": "15-06-2020 08:30",
    }
    mock_async_http_client.post.return_value = response_data

    response = await async_imsi_service.query_v2("0722000000")

    assert isinstance(response, IMSIV2Response)
    assert response.requestRefID == "ASYNC_REF_V2"
    assert response.msisdnRegistrationDate == "15-06-2020 08:30"

    mock_async_http_client.post.assert_called_once()
    args, _ = mock_async_http_client.post.call_args
    assert args[0] == "/imsi/v2/checkATI"


@pytest.mark.asyncio
async def test_async_query_v3_success(async_imsi_service, mock_async_http_client):
    """Test that async V3 checkATI query completes successfully."""
    response_data = {
        "requestRefID": "ASYNC_REF_V3",
        "imsi": "xyz987hashedimsi",
        "lastSwapDate": "05-01-2025 11:20",
    }
    mock_async_http_client.post.return_value = response_data

    response = await async_imsi_service.query_v3("0722000000")

    assert isinstance(response, IMSIV3Response)
    assert response.requestRefID == "ASYNC_REF_V3"
    assert response.imsi == "xyz987hashedimsi"
    assert getattr(response,"is_recently_swapped",None) is None

    mock_async_http_client.post.assert_called_once()
    args, _ = mock_async_http_client.post.call_args
    assert args[0] == "/imsi/v3/checkATI"


@pytest.mark.asyncio
async def test_async_imsi_query_handles_http_error(async_imsi_service, mock_async_http_client):
    """Test that async IMSI service handles HTTP errors gracefully."""
    mock_async_http_client.post.side_effect = Exception("Async Connection Timeout")

    with pytest.raises(Exception) as excinfo:
        await async_imsi_service.query_v1("0722000000")

    assert "Async Connection Timeout" in str(excinfo.value)
