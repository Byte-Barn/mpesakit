"""Unit tests for MpesaClient and its services."""

import pytest
from mpesakit.mpesa_client import MpesaClient, MpesaCallbackMixin
from mpesakit.auth import TokenManager
from mpesakit.http_client import MpesaHttpClient

from mpesakit.services import (
    B2BService,
    B2CService,
    BalanceService,
    BillService,
    C2BService,
    DynamicQRCodeService,
    StkPushService,
    RatibaService,
    ReversalService,
    TaxService,
    TransactionService,
)


@pytest.fixture
def client():
    """Creates a MpesaClient instance for testing."""
    return MpesaClient("dummy_key", "dummy_secret")


def test_uses_callback_mixin(client):
    """Test that MpesaClient inherits shared callback helpers."""
    assert isinstance(client, MpesaCallbackMixin)


def test_http_client_instance(client):
    """Test that the http_client is an instance of MpesaHttpClient."""
    assert isinstance(client.http_client, MpesaHttpClient)


def test_token_manager_instance(client):
    """Test that the token_manager is an instance of TokenManager."""
    assert isinstance(client.token_manager, TokenManager)


def test_express_service_instance(client):
    """Test that the express service is an instance of StkPushService."""
    assert isinstance(client.express, StkPushService)


def test_b2c_service_instance(client):
    """Test that the b2c service is an instance of B2CService."""
    assert isinstance(client.b2c, B2CService)


def test_b2b_service_instance(client):
    """Test that the b2b service is an instance of B2BService."""
    assert isinstance(client.b2b, B2BService)


def test_transactions_service_instance(client):
    """Test that the transactions service is an instance of TransactionService."""
    assert isinstance(client.transactions, TransactionService)


def test_tax_service_instance(client):
    """Test that the tax service is an instance of TaxService."""
    assert isinstance(client.tax, TaxService)


def test_balance_service_instance(client):
    """Test that the balance service is an instance of BalanceService."""
    assert isinstance(client.balance, BalanceService)


def test_reversal_service_instance(client):
    """Test that the reversal service is an instance of ReversalService."""
    assert isinstance(client.reversal, ReversalService)


def test_bill_service_instance(client):
    """Test that the bill service is an instance of BillService."""
    assert isinstance(client.bill, BillService)


def test_dynamic_qr_service_instance(client):
    """Test that the dynamic QR service is an instance of DynamicQRCodeService."""
    assert isinstance(client.dynamic_qr, DynamicQRCodeService)


def test_c2b_service_instance(client):
    """Test that the c2b service is an instance of C2BService."""
    assert isinstance(client.c2b, C2BService)


def test_ratiba_service_instance(client):
    """Test that the ratiba service is an instance of RatibaService."""
    assert isinstance(client.ratiba, RatibaService)


# Tests for callback processing methods
class TestCallbackProcessing:
    """Tests for MpesaClient callback processing methods."""

    def test_process_stk_callback(self, client):
        """Test processing STK Push callback payload."""
        payload = {
            "Body": {
                "stkCallback": {
                    "MerchantRequestID": "29115-34620561-1",
                    "CheckoutRequestID": "ws_CO_191220191020363925",
                    "ResultCode": 0,
                    "ResultDesc": "The service request is processed successfully.",
                    "CallbackMetadata": {
                        "Item": [
                            {"Name": "Amount", "Value": 1.0},
                            {"Name": "MpesaReceiptNumber", "Value": "LHG31AA5TX"},
                            {"Name": "Balance"},
                            {"Name": "TransactionDate", "Value": 20191219102115},
                            {"Name": "PhoneNumber", "Value": 254712345678},
                        ]
                    },
                }
            }
        }
        result = client.process_stk_callback(payload)
        assert result.Body.stkCallback.MerchantRequestID == "29115-34620561-1"
        assert result.Body.stkCallback.ResultCode == 0
        assert result.amount == 1.0
        assert result.mpesa_receipt_number == "LHG31AA5TX"

    def test_process_stk_query_callback(self, client):
        """Test processing STK Push query callback payload."""
        payload = {
        "MerchantRequestID": "12345",
        "CheckoutRequestID": "ws_CO_260520211133524545",
        "ResponseCode": 0,
        "ResponseDescription": "Success",
        "ResultCode": 0,
        "ResultDesc": "The service request is processed successfully.",
    }
        result = client.process_stk_query_callback(payload)
        assert result.ResultCode == 0
        assert result.MerchantRequestID == "12345"

    def test_process_account_balance_callback(self, client):
        """Test processing account balance callback payload."""
        payload = {
            "Result": {
                "ResultType": 0,
                "ResultCode": 0,
                "ResultDesc": "The service request has been processed successfully.",
                "OriginatorConversationID": "10571-774651-1",
                "ConversationID": "AN41320161328197f28cc1d183985ef4f1",
                "TransactionID": "LHG31AA5TX",
                "ResultParameters": {
                    "ResultParameter": [
                        {
                            "Key": "AccountBalance",
                            "Value": "580000.00",
                        }
                    ]
                },
                "ReferenceData": {
                    "ReferenceItem": {
                        "Key": "QueueOfficeNumber",
                        "Value": "00000",
                    }
                },
            }
        }
        result = client.process_account_balance_callback(payload)
        assert result.Result.ResultCode == 0
        assert result.Result.ConversationID == "AN41320161328197f28cc1d183985ef4f1"

    def test_process_account_balance_timeout(self, client):
        """Test processing account balance timeout callback payload."""
        payload = {
            "Result": {
                "ResultType": 0,
                "ResultCode": 2001,
                "ResultDesc": "The service request has timed out.",
                "OriginatorConversationID": "10571-774651-1",
                "ConversationID": "AN41320161328197f28cc1d183985ef4f1",
                "TransactionID": "LHG31AA5TX",
            }
        }
        result = client.process_account_balance_timeout(payload)
        assert result.Result.ResultCode == 2001
        assert "timed out" in result.Result.ResultDesc

    def test_process_b2c_callback(self, client):
        """Test processing B2C callback payload."""
        payload = {
            "Result": {
                "ResultType": 0,
                "ResultCode": 0,
                "ResultDesc": "The service request has been processed successfully.",
                "OriginatorConversationID": "10571-774651-1",
                "ConversationID": "AN41320161328197f28cc1d183985ef4f1",
                "TransactionID": "LHG31AA5TX",
                    "ResultParameters": [
                        {"Key": "TransactionAmount", "Value": "100.00"},
                        {"Key": "TransactionReceipt", "Value": "LHG31AA5TX"},
                        {"Key": "B2CRecipientIsLocked", "Value": "false"},
                        {"Key": "B2CChargesPaidAccountAvailableFunds", "Value": "49900.00"},
                        {"Key": "B2CUtilityAccountAvailableFunds", "Value": "199900.00"},
                        {"Key": "TransactionCompletedDateTime", "Value": "31.12.2021 23:59:59"},
                        {"Key": "B2CRecipientPhoneNumber", "Value": "254712345678"},
                    ],
                
            }
        }
        result = client.process_b2c_callback(payload)
        assert result.Result.ResultCode == 0
        assert result.Result.ConversationID == "AN41320161328197f28cc1d183985ef4f1"

    def test_process_b2b_callback(self, client):
        """Test processing B2B callback payload."""
        payload = {
                "resultCode": "0",
                "resultDesc": "The service request is processed successfully.",
                "amount": "71.0",
                "requestId": "404e1aec-19e0-4ce3-973d-bd92e94c8021",
                "resultType": "0",
                "conversationID": "AG_20230426_2010434680d9f5a73766",
                "transactionId": "RDQ01NFT1Q",
                "status": "SUCCESS",
            }
        result = client.process_b2b_callback(payload)
        assert result.conversationID == "AG_20230426_2010434680d9f5a73766"
        assert result.resultCode == "0"

    def test_process_transactions_callback(self, client):
        """Test processing transaction status callback payload."""
        payload = {
            "Result": {
                "ResultType": 0,
                "ResultCode": 0,
                "ResultDesc": "The service request has been processed successfully.",
                "OriginatorConversationID": "10571-774651-1",
                "ConversationID": "AN41320161328197f28cc1d183985ef4f1",
                "TransactionID": "LHG31AA5TX",
                    "ResultParameters": [
                        {"Key": "TransactionAmount", "Value": "100.00"},
                        {"Key": "TransactionStatus", "Value": "Completed"},
                        {"Key": "TransactionDate", "Value": "31.12.2021 23:59:59"},
                        {"Key": "ReceiptNo", "Value": "LHG31AA5TX"},
                    ],
                
            }
        }
        result = client.process_transactions_callback(payload)
        assert result.Result.ResultCode == 0
        assert result.Result.ConversationID == "AN41320161328197f28cc1d183985ef4f1"

    def test_process_bill_manager_callback(self, client):
        """Test processing bill manager callback payload."""
        payload = {
                "transactionId": "RJB53MYR1N",
                "paidAmount": 5000,
                "msisdn": "254722000000",
                "dateCreated": "2021-10-01",
                "accountReference": "BC001",
                "shortCode": 456545,
            }
        result = client.process_bill_manager_callback(payload)
        assert result.msisdn == "254722000000"
        assert result.paidAmount == 5000

    def test_process_tax_remittance_callback(self, client):
        """Test processing tax remittance callback payload."""
        payload = {
            "Result": {
                "ResultType": 0,
                "ResultCode": 0,
                "ResultDesc": "The service request has been processed successfully.",
                "OriginatorConversationID": "10571-774651-1",
                "ConversationID": "AN41320161328197f28cc1d183985ef4f1",
                "TransactionID": "LHG31AA5TX",
                "ResultParameters": {
                    "ResultParameter": [
                        {"Key": "TransactionAmount", "Value": "100.00"},
                        {"Key": "TransactionReceipt", "Value": "LHG31AA5TX"},
                        {"Key": "TransactionDate", "Value": "31.12.2021 23:59:59"},
                    ]
                },
            }
        }
        result = client.process_tax_remittance_callback(payload)
        assert result.Result.ResultCode == 0
        assert result.Result.ConversationID == "AN41320161328197f28cc1d183985ef4f1"

    def test_process_dynamic_qr_code_callback(self, client):
        """Test processing dynamic QR code callback payload."""
        payload = {
            "ResponseCode": "00000000",
            "ResponseDescription": "success",
            "QRCode": "00000101010101010101",
        }
        result = client.process_dynamic_qr_code_callback(payload)
        assert result.ResponseCode == "00000000"
        assert result.ResponseDescription == "success"

    def test_process_ratiba_service_callback(self, client):
        """Test processing ratiba service callback payload."""
        payload ={
                "ResponseHeader": {
                    "responseRefID": "0acc0239-20fa-4a52-8b9d-9bd64c0465c3",
                    "requestRefID": "0acc0239-20fa-4a52-8b9d-9bd64c0465c3",
                    "responseCode": "0",
                    "responseDescription": "The service request is processed successfully",
                },
                "ResponseBody": {
                    "ResponseData": [
                        {"Name": "TransactionID", "Value": "SC8F2IQMH5"},
                        {"Name": "responseCode", "Value": "0"},
                        {"Name": "Status", "Value": "OKAY"},
                        {"Name": "Msisdn", "Value": "254******867"},
                    ]
                },
            }
        
        result = client.process_ratiba_service_callback(payload)
        assert result.ResponseHeader.requestRefID == "0acc0239-20fa-4a52-8b9d-9bd64c0465c3"
        assert any(
        item.Name == "TransactionID" and item.Value == "SC8F2IQMH5"
        for item in result.ResponseBody.ResponseData
    )

        
    def test_process_reversal_callback(self, client):
        """Test processing reversal callback payload."""
        payload = {
            "Result": {
                "ResultType": 0,
                "ResultCode": "21",
                "ResultDesc": "The service request has been processed successfully.",
                "OriginatorConversationID": "10571-774651-1",
                "ConversationID": "AN41320161328197f28cc1d183985ef4f1",
                "TransactionID": "LHG31AA5TX",
                "ResultParameters": {
                    "ResultParameter": [
                        {"Key": "TransactionAmount", "Value": "100.00"},
                        {"Key": "TransactionReceipt", "Value": "LHG31AA5TX"},
                        {"Key": "TransactionDate", "Value": "31.12.2021 23:59:59"},
                    ]
                },
            }
        }
        result = client.process_reversal_callback(payload)
        assert result.Result.ResultCode == '21'
        assert result.Result.ConversationID == "AN41320161328197f28cc1d183985ef4f1"
