"""Schemas for M-PESA B2C Account TopUp APIs."""

from typing import Any, List, Optional

from pydantic import BaseModel, ConfigDict, Field, HttpUrl


class B2CAccountTopUpRequest(BaseModel):
    """Request schema for B2C Account TopUp."""

    Initiator: str = Field(..., description="M-Pesa API operator username.")
    SecurityCredential: str = Field(
        ..., description="Encrypted password of the API operator."
    )
    CommandID: str = "BusinessPayToBulk"
    SenderIdentifierType: int = 4
    RecieverIdentifierType: int = 4
    Amount: int = Field(..., description="Transaction amount.")
    PartyA: int = Field(..., description="Shortcode from which money will be deducted.")
    PartyB: int = Field(..., description="Shortcode to which money will be moved.")
    AccountReference: str = Field(..., description="Reference for the transaction.")
    Requester: Optional[str] = Field(
        None, description="Consumer’s mobile number on behalf of whom you are paying."
    )
    Remarks: Optional[str] = Field(
        None, description="Additional information for the transaction."
    )
    QueueTimeOutURL: HttpUrl = Field(..., description="URL for timeout notification.")
    ResultURL: HttpUrl = Field(
        ..., description="URL for transaction result notification."
    )

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "Initiator": "testapi",
                "SecurityCredential": "SecurityCredential",
                "CommandID": "BusinessPayToBulk",
                "SenderIdentifierType": 4,
                "RecieverIdentifierType": 4,
                "Amount": 239,
                "PartyA": 600979,
                "PartyB": 600000,
                "AccountReference": "353353",
                "Requester": "254708374149",
                "Remarks": "OK",
                "QueueTimeOutURL": "https://mydomain/path/timeout",
                "ResultURL": "https://mydomain/path/result",
            }
        }
    )


class B2CAccountTopUpResponse(BaseModel):
    """Immediate response schema for B2C Account TopUp request."""

    OriginatorConversationID: str = Field(
        ..., description="Unique request identifier assigned by Daraja."
    )
    ConversationID: str = Field(
        ..., description="Unique request identifier assigned by M-Pesa."
    )
    ResponseCode: str = Field(
        ..., description="Status code for request submission. 0 indicates success."
    )
    ResponseDescription: str = Field(
        ..., description="Descriptive message of the request submission status."
    )

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
        """Check if the response indicates a successful submission."""
        return self.ResponseCode == "0"


class ResultParameterItem(BaseModel):
    """Result parameter for B2C Account TopUp callback."""

    Key: str = Field(..., description="Parameter key.")
    Value: Any = Field(..., description="Parameter value.")


class RefItem(BaseModel):
    """Reference item for B2C Account TopUp callback."""

    Key: str = Field(..., description="Reference item key.")
    Value: Optional[Any] = Field(None, description="Reference item value.")


class ResultParams(BaseModel):
    """Result parameters for B2C Account TopUp callback."""

    ResultParameter: List[ResultParameterItem] = Field(
        default_factory=list, description="List of result parameters."
    )


class RefData(BaseModel):
    """Reference data for B2C Account TopUp callback."""

    ReferenceItem: List[RefItem] = Field(
        default_factory=list, description="List of reference items."
    )

    model_config = ConfigDict(arbitrary_types_allowed=True)


class B2CAccountTopUpCallbackResult(BaseModel):
    """Callback result schema for B2C Account TopUp."""

    ResultType: int = Field(
        ..., description="Status code for transaction sent to listener."
    )
    ResultCode: int | str = Field(
        ..., description="Transaction result status code. 0 means success."
    )
    ResultDesc: str = Field(
        ..., description="Descriptive message for the transaction result."
    )
    OriginatorConversationID: str = Field(
        ..., description="Unique request identifier assigned by API gateway."
    )
    ConversationID: str = Field(
        ..., description="Unique request identifier assigned by M-Pesa."
    )
    TransactionID: str = Field(
        ..., description="Unique M-PESA transaction ID for the payment request."
    )
    ResultParameters: Optional[ResultParams] = Field(
        None, description="Additional transaction details."
    )
    ReferenceData: Optional[RefData] = Field(
        None, description="Additional transaction reference data."
    )

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "ResultType": 0,
                "ResultCode": 0,
                "ResultDesc": "The service request is processed successfully",
                "OriginatorConversationID": "626f6ddf-ab37-4650-b882-b1de92ec9aa4",
                "ConversationID": "12345677dfdf89099B3",
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
                            "Value": "https://mydomain.com/b2b/businessbuygoods/queue/",
                        },
                    ]
                },
            }
        }
    )


class B2CAccountTopUpCallback(BaseModel):
    """Callback schema for B2C Account TopUp."""

    Result: B2CAccountTopUpCallbackResult = Field(
        ..., description="Result object containing transaction details."
    )

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "Result": {
                    "ResultType": 0,
                    "ResultCode": 0,
                    "ResultDesc": "The service request is processed successfully",
                    "OriginatorConversationID": "626f6ddf-ab37-4650-b882-b1de92ec9aa4",
                    "ConversationID": "12345677dfdf89099B3",
                    "TransactionID": "QKA81LK5CY",
                    "ResultParameters": {
                        "ResultParameter": [
                            {
                                "Key": "DebitAccountBalance",
                                "Value": "{Amount={CurrencyCode=KES, MinimumAmount=618683, BasicAmount=6186.83}}",
                            },
                            {"Key": "Amount", "Value": "190.00"},
                        ]
                    },
                    "ReferenceData": {
                        "ReferenceItem": [
                            {"Key": "BillReferenceNumber", "Value": "19008"},
                            {
                                "Key": "QueueTimeoutURL",
                                "Value": "https://mydomain.com/b2b/businessbuygoods/queue/",
                            },
                        ]
                    },
                }
            }
        }
    )

    def is_successful(self) -> bool:
        """Check if the callback indicates a successful transaction."""
        code = str(self.Result.ResultCode)
        return code.strip("0") == "" and code != ""


class B2CAccountTopUpCallbackResponse(BaseModel):
    """Response schema for B2C Account TopUp callback."""

    ResultCode: int | str = Field(
        default=0, description="Result code of the callback. 0 indicates success."
    )
    ResultDesc: str = Field(
        default="Callback processed successfully",
        description="Descriptive message of the callback result.",
    )


class B2CAccountTopUpTimeoutResultMetadata(BaseModel):
    """Result metadata for B2C Account TopUp timeout callback."""

    ResultType: int = Field(..., description="Type of result, 1 indicates timeout.")
    ResultCode: str = Field(..., description="Result code, 1 indicates timeout.")
    ResultDesc: str = Field(..., description="Description of the timeout event.")
    OriginatorConversationID: str = Field(
        ..., description="Unique request identifier assigned by Daraja."
    )
    ConversationID: str = Field(
        ..., description="Unique request identifier assigned by M-Pesa."
    )

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "ResultType": 1,
                "ResultCode": "1",
                "ResultDesc": "The service request timed out.",
                "OriginatorConversationID": "8521-4298025-1",
                "ConversationID": "AG_20181005_00004d7ee675c0c7ee0b",
            }
        }
    )


class B2CAccountTopUpTimeoutCallback(BaseModel):
    """Schema for B2C Account TopUp sent to QueueTimeOutURL."""

    Result: B2CAccountTopUpTimeoutResultMetadata = Field(
        ..., description="Result metadata."
    )

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "Result": {
                    "ResultType": 1,
                    "ResultCode": "1",
                    "ResultDesc": "The service request timed out.",
                    "OriginatorConversationID": "8521-4298025-1",
                    "ConversationID": "AG_20181005_00004d7ee675c0c7ee0b",
                }
            }
        }
    )


class B2CAccountTopUpTimeoutCallbackResponse(BaseModel):
    """Schema for response to B2C Account TopUp timeout callback."""

    ResultCode: int | str = Field(
        default=0,
        description="Result code (0=Success, other=Failure).",
    )
    ResultDesc: str = Field(
        default="Timeout notification received and processed successfully.",
        description="Result description.",
    )

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "ResultCode": 0,
                "ResultDesc": "Timeout notification received and processed successfully.",
            }
        }
    )
