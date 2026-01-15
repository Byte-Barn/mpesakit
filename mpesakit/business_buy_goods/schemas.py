"""Schemas for M-Pesa Business Buy Goods API requests and responses."""

from pydantic import BaseModel, Field, ConfigDict
from typing import Optional, List, Union


class BusinessBuyGoodsRequest(BaseModel):
    """Request schema for Business Buy Goods."""

    Initiator: str = Field(..., description="M-Pesa API operator username.")
    SecurityCredential: str = Field(..., description="Encrypted security credential.")
    CommandID: str = "BusinessBuyGoods"
    SenderIdentifierType: int = 4
    RecieverIdentifierType: int = 4
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
    Occassion: Optional[str] = Field(
        None, description="Additional transaction info (optional)."
    )

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "Initiator": "API_Username",
                "SecurityCredential": "encrypted_credential",
                "CommandID": "BusinessBuyGoods",
                "SenderIdentifierType": 4,
                "RecieverIdentifierType": 4,
                "Amount": 239,
                "PartyA": 123456,
                "PartyB": 000000,
                "AccountReference": "353353",
                "Requester": "254700000000",
                "Remarks": "OK",
                "QueueTimeOutURL": "https://mydomain.com/b2b/businessbuygoods/queue/",
                "ResultURL": "https://mydomain.com/b2b/businessbuygoods/result/",
                "Occassion": "Payment for goods",
            }
        }
    )


class BusinessBuyGoodsResponse(BaseModel):
    """Response schema for Business Buy Goods request."""

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
        """Check if the response indicates a successful transaction."""
        code = str(self.ResponseCode)
        return code.strip("0") == "" and code != ""


class BusinessBuyGoodsResultParameter(BaseModel):
    """Represents a single result parameter in the Business Buy Goods response."""

    Key: str = Field(..., description="Parameter name.")
    Value: Union[str, int] = Field(..., description="Parameter value.")


class BusinessBuyGoodsReferenceItem(BaseModel):
    """Represents a single reference item in the Business Buy Goods response."""

    Key: str = Field(..., description="Reference parameter name.")
    Value: Union[str, int] = Field(..., description="Reference parameter value.")


class BusinessBuyGoodsReferenceData(BaseModel):
    """Container for reference data in the Business Buy Goods response."""

    ReferenceItem: Union[
        List[BusinessBuyGoodsReferenceItem], BusinessBuyGoodsReferenceItem
    ] = Field(..., description="List of reference items or single item.")


class BusinessBuyGoodsResultParameters(BaseModel):
    """Container for result parameters in the Business Buy Goods response."""

    ResultParameter: Union[
        List[BusinessBuyGoodsResultParameter], BusinessBuyGoodsResultParameter
    ] = Field(..., description="List of result parameters or single parameter.")


class BusinessBuyGoodsResultMetadata(BaseModel):
    """Metadata for the result of a Business Buy Goods transaction."""

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
    ResultParameters: Optional[BusinessBuyGoodsResultParameters] = Field(
        None, description="Result parameters container."
    )
    ReferenceData: Optional[BusinessBuyGoodsReferenceData] = Field(
        None, description="Reference data."
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


class BusinessBuyGoodsResultCallback(BaseModel):
    """Represents the result callback for Business Buy Goods transactions."""

    Result: BusinessBuyGoodsResultMetadata = Field(..., description="Result metadata.")

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
        }
    )

    def is_successful(self) -> bool:
        """Check if the result indicates a successful transaction."""
        code = str(self.Result.ResultCode)
        return code.strip("0") == "" and code != ""


class BusinessBuyGoodsResultCallbackResponse(BaseModel):
    """Response schema for Business Buy Goods result callback."""

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


class BusinessBuyGoodsTimeoutCallback(BaseModel):
    """Represents the timeout callback for Business Buy Goods transactions."""

    Result: BusinessBuyGoodsResultMetadata = Field(..., description="Result metadata.")

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "Result": {
                    "ResultType": 0,
                    "ResultCode": 2001,
                    "ResultDesc": "The initiator information is invalid.",
                    "OriginatorConversationID": "12337-23509183-5",
                    "ConversationID": "AG_20200120_0000657265d5fa9ae5c0",
                    "TransactionID": "OAK0000000",
                    "ResultParameters": {
                        "ResultParameter": {
                            "Key": "BOCompletedTime",
                            "Value": 20200120164825,
                        }
                    },
                    "ReferenceData": {
                        "ReferenceItem": {
                            "Key": "QueueTimeoutURL",
                            "Value": "https://mydomain.com/b2b/businessbuygoods/queue/",
                        }
                    },
                }
            }
        }
    )


class BusinessBuyGoodsTimeoutCallbackResponse(BaseModel):
    """Response schema for Business Buy Goods timeout callback."""

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
