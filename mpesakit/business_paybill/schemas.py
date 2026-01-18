"""This module defines schemas for M-Pesa Business PayBill API requests and responses."""

from typing import List, Optional

from pydantic import BaseModel, ConfigDict, Field


class BusinessPayBillRequest(BaseModel):
    """Request schema for Business PayBill."""

    Initiator: str = Field(..., description="M-Pesa API operator username.")
    SecurityCredential: str = Field(..., description="Encrypted security credential.")
    Amount: int = Field(..., description="Transaction amount.")
    PartyA: int = Field(..., description="Shortcode from which money is deducted.")
    PartyB: int = Field(..., description="Shortcode to which money is credited.")
    AccountReference: str = Field(
        ..., description="Account number associated with the payment."
    )
    Requester: Optional[str] = Field(
        None, description="Consumer's mobile number (optional)."
    )
    Remarks: str = Field(
        ..., description="Additional transaction information (max 100 chars)."
    )
    QueueTimeOutURL: str = Field(..., description="URL for timeout notifications.")
    ResultURL: str = Field(..., description="URL for result notifications.")

    CommandID: str = "BusinessPayBill"  # Business PayBill command ID. Only this command is supported.
    SenderIdentifierType: int = 4  # Only 4 is supported.
    RecieverIdentifierType: int = 4  # Only 4 is supported.

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "Initiator": "API_Username",
                "SecurityCredential": "encrypted_credential",
                "CommandID": "BusinessPayBill",
                "SenderIdentifierType": 4,
                "RecieverIdentifierType": 4,
                "Amount": 239,
                "PartyA": 123456,
                "PartyB": 000000,
                "AccountReference": "353353",
                "Requester": "254700000000",
                "Remarks": "OK",
                "QueueTimeOutURL": "http://0.0.0.0:0000/ResultsListener.php",
                "ResultURL": "http://0.0.0.0:8888/TimeOutListener.php",
            }
        }
    )


class BusinessPayBillResponse(BaseModel):
    """Response schema for Business PayBill request."""

    OriginatorConversationID: Optional[str] = Field(
        ..., description="Unique ID for the request message."
    )
    ConversationID: Optional[str] = Field(
        ..., description="Unique ID for the transaction."
    )
    ResponseCode: str = Field(..., description="Status code, 0 means success.")
    ResponseDescription: str = Field(..., description="Status message.")

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "OriginatorConversationID": "5118-111210482-1",
                "ConversationID": "AG_20230420_2010759fd5662ef6d054",
                "ResponseCode": "0",
                "ResponseDescription": "Accept the service request successfully.",
            }
        }
    )

    def is_successful(self) -> bool:
        """Check if the response indicates success."""
        return self.ResponseCode == "0"


class BusinessPayBillResultParameter(BaseModel):
    """Parameter item in Business PayBill result notification."""

    Key: str = Field(..., description="Parameter name.")
    Value: str = Field(..., description="Parameter value.")


class BusinessPayBillReferenceItem(BaseModel):
    """Reference item in Business PayBill result notification."""

    Key: str = Field(..., description="Reference parameter name.")
    Value: str = Field(..., description="Reference parameter value.")


class BusinessPayBillReferenceData(BaseModel):
    """Reference data in Business PayBill result notification."""

    ReferenceItem: List[BusinessPayBillReferenceItem] = Field(
        ..., description="List of reference items."
    )


class BusinessPayBillResultParameters(BaseModel):
    """Result parameters container."""

    ResultParameter: List[BusinessPayBillResultParameter] = Field(
        ..., description="List of result parameters."
    )


class BusinessPayBillResultMetadata(BaseModel):
    """Metadata for Business PayBill result notification."""

    ResultType: int = Field(..., description="Type of result (0=Success, 1=Failure).")
    ResultCode: int | str = Field(..., description="Result code (0=Success).")
    ResultDesc: str = Field(..., description="Result description.")
    OriginatorConversationID: str = Field(
        ..., description="Originator conversation ID."
    )
    ConversationID: str = Field(..., description="Conversation ID.")
    TransactionID: Optional[str] = Field(
        None, description="M-Pesa transaction ID (if available)."
    )
    ResultParameters: Optional[BusinessPayBillResultParameters] = Field(
        None, description="Result parameters container."
    )
    ReferenceData: Optional[BusinessPayBillReferenceData] = Field(
        None, description="Reference data."
    )

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "ResultType": 0,
                "ResultCode": 0,
                "ResultDesc": "The service request is processed successfully",
                "OriginatorConversationID": "626f6ddf-ab37-4650-b882-b1de92ec9aa4",
                "ConversationID": "AG_20181005_00004d7ee675c0c7ee0b",
                "TransactionID": "QKA81LK5CY",
                "ResultParameters": {
                    "ResultParameter": [
                        {
                            "Key": "DebitAccountBalance",
                            "Value": "{Amount={CurrencyCode=KES, MinimumAmount=618683, BasicAmount=6186.83}}",
                        },
                        {"Key": "Amount", "Value": "190.00"},
                        {
                            "Key": "DebitPartyAffectedAccountBalance",
                            "Value": "Working Account|KES|346568.83|6186.83|340382.00|0.00",
                        },
                        {"Key": "TransCompletedTime", "Value": "20221110110717"},
                        {"Key": "DebitPartyCharges", "Value": ""},
                        {
                            "Key": "ReceiverPartyPublicName",
                            "Value": "000000– Biller Company",
                        },
                        {"Key": "Currency", "Value": "KES"},
                        {
                            "Key": "InitiatorAccountCurrentBalance",
                            "Value": "{Amount={CurrencyCode=KES, MinimumAmount=618683, BasicAmount=6186.83}}",
                        },
                    ]
                },
                "ReferenceData": {
                    "ReferenceItem": [
                        {"Key": "BillReferenceNumber", "Value": "19008"},
                        {
                            "Key": "QueueTimeoutURL",
                            "Value": "http://172.31.234.68:8888/Listener.php",
                        },
                    ]
                },
            }
        }
    )


class BusinessPayBillResultCallback(BaseModel):
    """Schema for Business PayBill result notification sent to ResultURL."""

    Result: BusinessPayBillResultMetadata = Field(..., description="Result metadata.")

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "Result": {
                    "ResultType": 0,
                    "ResultCode": 0,
                    "ResultDesc": "The service request is processed successfully",
                    "OriginatorConversationID": "626f6ddf-ab37-4650-b882-b1de92ec9aa4",
                    "ConversationID": "AG_20181005_00004d7ee675c0c7ee0b",
                    "TransactionID": "QKA81LK5CY",
                    "ResultParameters": {
                        "ResultParameter": [
                            {
                                "Key": "DebitAccountBalance",
                                "Value": "{Amount={CurrencyCode=KES, MinimumAmount=618683, BasicAmount=6186.83}}",
                            },
                            {"Key": "Amount", "Value": "190.00"},
                            {
                                "Key": "DebitPartyAffectedAccountBalance",
                                "Value": "Working Account|KES|346568.83|6186.83|340382.00|0.00",
                            },
                            {"Key": "TransCompletedTime", "Value": "20221110110717"},
                            {"Key": "DebitPartyCharges", "Value": ""},
                            {
                                "Key": "ReceiverPartyPublicName",
                                "Value": "000000– Biller Company",
                            },
                            {"Key": "Currency", "Value": "KES"},
                            {
                                "Key": "InitiatorAccountCurrentBalance",
                                "Value": "{Amount={CurrencyCode=KES, MinimumAmount=618683, BasicAmount=6186.83}}",
                            },
                        ]
                    },
                    "ReferenceData": {
                        "ReferenceItem": [
                            {"Key": "BillReferenceNumber", "Value": "19008"},
                            {
                                "Key": "QueueTimeoutURL",
                                "Value": "http://172.31.234.68:8888/Listener.php",
                            },
                        ]
                    },
                }
            }
        }
    )

    def is_successful(self) -> bool:
        """Return True if ResultCode indicates success (e.g., '0', '00000000')."""
        code = str(self.Result.ResultCode)
        return code.strip("0") == "" and code != ""


class BusinessPayBillResultCallbackResponse(BaseModel):
    """Response schema for Business PayBill result callback."""

    ResultCode: int | str = 0
    ResultDesc: str = "Callback received successfully."

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "ResultCode": 0,
                "ResultDesc": "The service request is processed successfully.",
            }
        }
    )


class BusinessPayBillTimeoutCallback(BaseModel):
    """Schema for Business PayBill timeout notification sent to QueueTimeOutURL."""

    Result: BusinessPayBillResultMetadata = Field(..., description="Result metadata.")

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "Result": {
                    "ResultType": 1,
                    "ResultCode": 2001,
                    "ResultDesc": "The initiator information is invalid.",
                    "OriginatorConversationID": "12337-23509183-5",
                    "ConversationID": "AG_20200120_0000657265d5fa9ae5c0",
                }
            }
        }
    )


class BusinessPayBillTimeoutCallbackResponse(BaseModel):
    """Response schema for Business PayBill timeout callback."""

    ResultCode: int | str = 0
    ResultDesc: str = "Timeout notification received successfully."

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "ResultCode": 0,
                "ResultDesc": "The service request timed out response received.",
            }
        }
    )
