"""Unit tests for the B2Pochi (Business to Pochi) functionality of the Mpesa SDK."""

import pytest

from mpesakit.b2p import (
    B2P,
    B2PCommandIDType,
    B2PRequest,
    B2PResponse,
    B2PResultCallback,
    B2PResultCallbackResponse,
    B2PResultMetadata,
    B2PTimeoutCallback,
    B2PTimeoutCallbackResponse,
)


@pytest.fixture
def b2p(mock_http_client, mock_token_manager):
    """Fixture to create an instance of B2P with mocked dependencies."""
    return B2P(http_client=mock_http_client, token_manager=mock_token_manager)


def valid_b2p_request(**overrides):
    """Return a valid B2PRequest instance."""
    payload = dict(
        OriginatorConversationID="600997_Test_32et3241ed8yu",
        InitiatorName="testapi",
        SecurityCredential="encrypted_credential",
        CommandID=B2PCommandIDType.BusinessPayToPochi.value,
        Amount=10,
        PartyA=600992,
        PartyB=254705912645,
        Remarks="remarked",
        QueueTimeOutURL="https://example.com/timeout",
        ResultURL="https://example.com/result",
        Occassion="ChristmasPay",
    )
    payload.update(overrides)
    return B2PRequest(**payload)


def test_send_payment_success(b2p, mock_http_client):
    """Test that a successful B2Pochi payment can be performed."""
    request = valid_b2p_request()
    response_data = {
        "ConversationID": "AG_20240706_20106e9209f64bebd05b",
        "OriginatorConversationID": "600997_Test_32et3241ed8yu",
        "ResponseCode": "0",
        "ResponseDescription": "Accept the service request successfully.",
    }
    mock_http_client.post.return_value = response_data

    response = b2p.send_payment(request)

    assert isinstance(response, B2PResponse)
    assert response.ConversationID == response_data["ConversationID"]
    assert (
        response.OriginatorConversationID == response_data["OriginatorConversationID"]
    )
    assert response.ResponseCode == response_data["ResponseCode"]
    assert response.ResponseDescription == response_data["ResponseDescription"]
    assert response.is_successful is True
    mock_http_client.post.assert_called_once()
    args, kwargs = mock_http_client.post.call_args
    assert args[0] == "/mpesa/b2pochi/v1/paymentrequest"
    assert kwargs["headers"]["Authorization"] == "Bearer test_token"
    assert kwargs["json"]["CommandID"] == "BusinessPayToPochi"
    assert kwargs["json"]["Occassion"] == "ChristmasPay"


def test_send_payment_omits_none_occassion(b2p, mock_http_client):
    """Test that optional Occassion is omitted from the payload when unset."""
    request = valid_b2p_request(Occassion=None)
    mock_http_client.post.return_value = {
        "ConversationID": "AG_20240706_20106e9209f64bebd05b",
        "OriginatorConversationID": "600997_Test_32et3241ed8yu",
        "ResponseCode": "0",
        "ResponseDescription": "Accept the service request successfully.",
    }

    b2p.send_payment(request)

    assert "Occassion" not in mock_http_client.post.call_args.kwargs["json"]


def test_send_payment_http_error(b2p, mock_http_client):
    """Test that B2Pochi payment handles HTTP errors gracefully."""
    request = valid_b2p_request()
    mock_http_client.post.side_effect = Exception("HTTP error")

    with pytest.raises(Exception) as excinfo:
        b2p.send_payment(request)
    assert "HTTP error" in str(excinfo.value)


def test_b2p_request_defaults_command_id():
    """Test that CommandID defaults to BusinessPayToPochi."""
    request = valid_b2p_request()
    request_without_command = B2PRequest(
        OriginatorConversationID="600997_Test_32et3241ed8yu",
        InitiatorName="testapi",
        SecurityCredential="encrypted_credential",
        Amount=10,
        PartyA=600992,
        PartyB=254705912645,
        Remarks="remarked",
        QueueTimeOutURL="https://example.com/timeout",
        ResultURL="https://example.com/result",
    )
    assert request.CommandID == B2PCommandIDType.BusinessPayToPochi.value
    assert request_without_command.CommandID == "BusinessPayToPochi"


def test_b2p_request_accepts_command_id_enum():
    """Test that CommandID accepts the B2PCommandIDType enum."""
    request = valid_b2p_request(CommandID=B2PCommandIDType.BusinessPayToPochi)
    assert request.CommandID == "BusinessPayToPochi"


@pytest.mark.parametrize(
    "invalid_command_id", ["InvalidCommand", "", "BusinessPayment"]
)
def test_b2p_request_invalid_command_id_raises(invalid_command_id):
    """Test that B2PRequest raises ValueError for invalid CommandID."""
    with pytest.raises(ValueError) as excinfo:
        valid_b2p_request(CommandID=invalid_command_id)
    assert "CommandID must be one of" in str(excinfo.value)


def test_b2p_request_invalid_partyb_raises():
    """Test that B2PRequest raises ValueError for invalid PartyB phone number."""
    with pytest.raises(ValueError) as excinfo:
        valid_b2p_request(PartyB="notaphone")
    assert "PartyB must be a valid Kenyan phone number" in str(excinfo.value)


def test_b2p_request_normalizes_partyb():
    """Test that PartyB is normalized to a 12-digit MSISDN."""
    request = valid_b2p_request(PartyB="0705912645")
    assert request.PartyB == 254705912645


def test_b2p_request_remarks_too_short_raises():
    """Test that B2PRequest raises ValueError for Remarks shorter than 2 chars."""
    with pytest.raises(ValueError) as excinfo:
        valid_b2p_request(Remarks="A")
    assert "Remarks must be between 2 and 100 characters." in str(excinfo.value)


def test_b2p_request_remarks_too_long_raises():
    """Test that B2PRequest raises ValueError for Remarks longer than 100 chars."""
    with pytest.raises(ValueError) as excinfo:
        valid_b2p_request(Remarks="A" * 101)
    assert "Remarks must be between 2 and 100 characters." in str(excinfo.value)


def test_b2p_request_occassion_too_long_raises():
    """Test that B2PRequest raises ValueError for Occassion longer than 100 chars."""
    with pytest.raises(ValueError) as excinfo:
        valid_b2p_request(Occassion="A" * 101)
    assert "Occassion must not exceed 100 characters." in str(excinfo.value)


def test_b2p_response_is_successful_zero_code():
    """Test is_successful returns True for ResponseCode '0'."""
    resp = B2PResponse(
        ConversationID="AG_20240706_20106e9209f64bebd05b",
        OriginatorConversationID="600997_Test_32et3241ed8yu",
        ResponseCode="0",
        ResponseDescription="Accept the service request successfully.",
    )
    assert resp.is_successful is True


def test_b2p_response_is_successful_all_zeros():
    """Test is_successful returns True for ResponseCode '00000000'."""
    resp = B2PResponse(
        ConversationID="AG_20240706_20106e9209f64bebd05b",
        OriginatorConversationID="600997_Test_32et3241ed8yu",
        ResponseCode="00000000",
        ResponseDescription="Accept the service request successfully.",
    )
    assert resp.is_successful is True


def test_b2p_response_is_successful_non_zero_code():
    """Test is_successful returns False for non-success ResponseCode."""
    resp = B2PResponse(
        ConversationID="AG_20240706_20106e9209f64bebd05b",
        OriginatorConversationID="600997_Test_32et3241ed8yu",
        ResponseCode="1",
        ResponseDescription="Failed.",
    )
    assert resp.is_successful is False


def test_b2p_response_is_successful_empty_code():
    """Test is_successful returns False for empty ResponseCode."""
    resp = B2PResponse(
        ConversationID="AG_20240706_20106e9209f64bebd05b",
        OriginatorConversationID="600997_Test_32et3241ed8yu",
        ResponseCode="",
        ResponseDescription="Failed.",
    )
    assert resp.is_successful is False


def test_result_callback_success_payload():
    """Test parsing of a successful B2Pochi callback from Daraja."""
    payload = {
        "Result": {
            "ResultType": 0,
            "ResultCode": 0,
            "ResultDesc": "The service request is processed successfully.",
            "OriginatorConversationID": "53e3-4aa8-9fe0-8fb5e4092cdd3533373",
            "ConversationID": "AG_20240706_2010364430d9bbdaf872",
            "TransactionID": "SG632NMUAB",
            "ResultParameters": {
                "ResultParameter": [
                    {"Key": "TransactionAmount", "Value": 10},
                    {"Key": "TransactionReceipt", "Value": "SG632NMUAB"},
                    {
                        "Key": "ReceiverPartyPublicName",
                        "Value": "254705912645 - NICHOLAS JOHN SONGOK",
                    },
                    {
                        "Key": "TransactionCompletedDateTime",
                        "Value": "06.07.2024 22:48:52",
                    },
                    {"Key": "B2CUtilityAccountAvailableFunds", "Value": 8959269.6},
                    {"Key": "B2CWorkingAccountAvailableFunds", "Value": 1199371.0},
                    {"Key": "B2CRecipientIsRegisteredCustomer", "Value": "Y"},
                    {"Key": "B2CChargesPaidAccountAvailableFunds", "Value": -1980.0},
                ]
            },
            "ReferenceData": {
                "ReferenceItem": {
                    "Key": "QueueTimeoutURL",
                    "Value": "https://internalsandbox.safaricom.co.ke/mpesa/b2cresults/v1/submit",
                }
            },
        }
    }
    callback = B2PResultCallback(**payload)
    assert callback.is_successful is True
    assert callback.Result.TransactionID == "SG632NMUAB"
    assert callback.Result.transaction_amount == 10
    assert callback.Result.transaction_receipt == "SG632NMUAB"
    assert callback.Result.recipient_is_registered is True
    assert (
        callback.Result.receiver_party_public_name
        == "254705912645 - NICHOLAS JOHN SONGOK"
    )
    assert callback.Result.transaction_completed_datetime == "06.07.2024 22:48:52"
    assert callback.Result.utility_account_available_funds == 8959269.6
    assert callback.Result.working_account_available_funds == 1199371.0
    assert callback.Result.charges_paid_account_available_funds == -1980.0
    assert callback.Result.ReferenceData.ReferenceItem.Key == "QueueTimeoutURL"


def test_result_callback_unsuccessful_payload():
    """Test parsing of an unsuccessful B2Pochi callback."""
    payload = {
        "Result": {
            "ResultType": 0,
            "ResultCode": 2001,
            "ResultDesc": "The initiator information is invalid.",
            "OriginatorConversationID": "53e3-4aa8-9fe0-8fb5e4092cdd3544366",
            "ConversationID": "AG_20240707_201062f6f6f5804f7a33",
            "TransactionID": "SG722NMVXQ",
            "ReferenceData": {
                "ReferenceItem": {
                    "Key": "QueueTimeoutURL",
                    "Value": "https://internalsandbox.safaricom.co.ke/mpesa/b2cresults/v1/submit",
                }
            },
        }
    }
    callback = B2PResultCallback(**payload)
    assert callback.is_successful is False
    assert callback.Result.ResultCode == 2001
    assert callback.Result.transaction_amount is None
    assert "invalid" in callback.Result.ResultDesc


def test_result_metadata_accepts_flat_result_parameters():
    """Test that a flat ResultParameters list is accepted for robustness."""
    meta = B2PResultMetadata(
        ResultType=0,
        ResultCode=0,
        ResultDesc="Success",
        OriginatorConversationID="conv-id",
        ConversationID="conv-id-2",
        TransactionID="SG632NMUAB",
        ResultParameters=[
            {"Key": "TransactionAmount", "Value": 1500},
            {"Key": "TransactionReceipt", "Value": "SG632NMUAB"},
            {"Key": "B2CRecipientIsRegisteredCustomer", "Value": "N"},
        ],
    )
    assert meta.transaction_amount == 1500
    assert meta.transaction_receipt == "SG632NMUAB"
    assert meta.recipient_is_registered is False


def test_result_metadata_properties_none_parameters():
    """Test that B2PResultMetadata handles missing parameters correctly."""
    meta = B2PResultMetadata(
        ResultType=0,
        ResultCode=0,
        ResultDesc="Success",
        OriginatorConversationID="conv-id",
        ConversationID="conv-id-2",
        TransactionID="SG632NMUAB",
        ResultParameters=None,
    )
    assert meta.transaction_amount is None
    assert meta.transaction_receipt is None
    assert meta.recipient_is_registered is None
    assert meta.receiver_party_public_name is None
    assert meta.transaction_completed_datetime is None
    assert meta.charges_paid_account_available_funds is None
    assert meta.utility_account_available_funds is None
    assert meta.working_account_available_funds is None


def test_result_metadata_recipient_is_registered_none():
    """Test that an unexpected registered-customer flag returns None."""
    meta = B2PResultMetadata(
        ResultType=0,
        ResultCode=0,
        ResultDesc="Success",
        OriginatorConversationID="conv-id",
        ConversationID="conv-id-2",
        TransactionID="SG632NMUAB",
        ResultParameters={
            "ResultParameter": [
                {"Key": "B2CRecipientIsRegisteredCustomer", "Value": "X"},
            ]
        },
    )
    assert meta.recipient_is_registered is None


def test_result_callback_is_successful_string_zero():
    """Test is_successful handles ResultCode as a string without TypeError."""
    callback = B2PResultCallback(
        Result={
            "ResultType": 0,
            "ResultCode": "0",
            "ResultDesc": "Success",
            "OriginatorConversationID": "conv-id",
            "ConversationID": "conv-id-2",
            "TransactionID": "SG632NMUAB",
        }
    )
    assert callback.is_successful is True


def test_result_callback_response_defaults():
    """Test the response schema for B2Pochi result callback."""
    resp = B2PResultCallbackResponse()
    assert resp.ResultCode == 0
    assert "processed successfully" in resp.ResultDesc


def test_timeout_callback():
    """Test parsing of a B2Pochi timeout callback."""
    payload = {
        "Result": {
            "ResultType": 1,
            "ResultCode": "1",
            "ResultDesc": "The service request timed out.",
            "OriginatorConversationID": "8521-4298025-1",
            "ConversationID": "AG_20181005_00004d7ee675c0c7ee0b",
        }
    }
    callback = B2PTimeoutCallback(**payload)
    assert callback.Result.ResultType == 1
    assert callback.Result.ResultCode == "1"
    assert "timed out" in callback.Result.ResultDesc


def test_timeout_callback_response():
    """Test the response schema for B2Pochi timeout callback."""
    resp = B2PTimeoutCallbackResponse()
    assert resp.ResultCode == 0
    assert "Timeout notification received" in resp.ResultDesc
