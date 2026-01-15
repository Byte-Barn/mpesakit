"""This module defines schemas for M-Pesa Tax Remittance API requests and responses."""

from pydantic import BaseModel, Field, ConfigDict
from typing import Optional, List


class TaxRemittanceRequest(BaseModel):
    """Request schema for Tax Remittance."""

    Initiator: str = Field(..., description="Username used to initiate the request.")
    SecurityCredential: str = Field(..., description="Encrypted security credential.")
    Amount: int = Field(..., description="Transaction amount (in KES).")
    PartyA: int = Field(..., description="Shortcode from which money is deducted.")
    AccountReference: str = Field(
        ..., description="Payment registration number (PRN) issued by KRA."
    )
    Remarks: str = Field(
        ..., description="Additional information for the transaction (max 100 chars)."
    )
    QueueTimeOutURL: str = Field(..., description="URL for timeout notifications.")
    ResultURL: str = Field(..., description="URL for result notifications.")

    PartyB: int = 572572
    CommandID: str = "PayTaxToKRA"
    SenderIdentifierType: int = 4
    RecieverIdentifierType: int = 4

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "Initiator": "TaxPayer",
                "SecurityCredential": "encrypted_credential",
                "CommandID": "PayTaxToKRA",
                "SenderIdentifierType": 4,
                "ReceiverIdentifierType": 4,
                "Amount": 239,
                "PartyA": 888880,
                "PartyB": 572572,
                "AccountReference": "353353",
                "Remarks": "OK",
                "QueueTimeOutURL": "https://mydomain.com/b2b/remittax/queue/",
                "ResultURL": "https://mydomain.com/b2b/remittax/result/",
            }
        }
    )


class TaxRemittanceResponse(BaseModel):
    """Response schema for Tax Remittance request."""

    OriginatorConversationID: Optional[str] = Field(
        ..., description="Unique ID for the request message."
    )
    ConversationID: Optional[str] = Field(
        ..., description="Unique ID for the transaction."
    )
    ResponseCode: str | int = Field(..., description="Status code, 0 means success.")
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
        code = str(self.ResponseCode)
        return code.strip("0") == "" and code != ""


class TaxRemittanceResultParameter(BaseModel):
    """Parameter item in Tax Remittance result notification."""

    Key: str = Field(..., description="Parameter name.")
    Value: str = Field(..., description="Parameter value.")


class TaxRemittanceReferenceItem(BaseModel):
    """Reference item in Tax Remittance result notification."""

    Key: str = Field(..., description="Reference parameter name.")
    Value: str = Field(..., description="Reference parameter value.")


class TaxRemittanceReferenceData(BaseModel):
    """Reference data in Tax Remittance result notification."""

    ReferenceItem: List[TaxRemittanceReferenceItem] = Field(
        ..., description="List of reference items."
    )


class TaxRemittanceResultParameters(BaseModel):
    """Result parameters container."""

    ResultParameter: List[TaxRemittanceResultParameter] = Field(
        ..., description="List of result parameters."
    )


class TaxRemittanceResultMetadata(BaseModel):
    """Metadata for Tax Remittance result notification."""

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
    ResultParameters: Optional[TaxRemittanceResultParameters] = Field(
        None, description="Result parameters container."
    )
    ReferenceData: Optional[TaxRemittanceReferenceData] = Field(
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
                        {"Key": "Amount", "Value": "190.00"},
                        {"Key": "Currency", "Value": "KES"},
                        {"Key": "TransCompletedTime", "Value": "20221110110717"},
                    ]
                },
                "ReferenceData": {
                    "ReferenceItem": [
                        {"Key": "BillReferenceNumber", "Value": "19008"},
                        {
                            "Key": "QueueTimeoutURL",
                            "Value": "https://mydomain.com/b2b/remittax/queue/",
                        },
                    ]
                },
            }
        }
    )


class TaxRemittanceResultCallback(BaseModel):
    """Schema for Tax Remittance result notification sent to ResultURL."""

    Result: TaxRemittanceResultMetadata = Field(..., description="Result metadata.")

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
                            {"Key": "Amount", "Value": "190.00"},
                            {"Key": "Currency", "Value": "KES"},
                            {"Key": "TransCompletedTime", "Value": "20221110110717"},
                        ]
                    },
                    "ReferenceData": {
                        "ReferenceItem": [
                            {"Key": "BillReferenceNumber", "Value": "19008"},
                            {
                                "Key": "QueueTimeoutURL",
                                "Value": "https://mydomain.com/b2b/remittax/queue/",
                            },
                        ]
                    },
                }
            }
        }
    )

    def is_successful(self) -> bool:
        """Check if the result indicates success."""
        code = str(self.Result.ResultCode)
        return code.strip("0") == "" and code != ""


class TaxRemittanceResultCallbackResponse(BaseModel):
    """Schema for response sent back to Daraja API to acknowledge callback receipt."""

    ResultCode: int | str = Field(default=0, description="Code indicating the result status.")
    ResultDesc: str = Field(
        default="Callback received successfully",
        description="Description of the result.",
    )

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "ResultCode": 0,
                "ResultDesc": "Callback received successfully",
            }
        }
    )


class TaxRemittanceTimeoutCallback(BaseModel):
    """Schema Tax Remittance sent to QueueTimeOutURL."""

    Result: TaxRemittanceResultMetadata = Field(..., description="Result metadata.")

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


class TaxRemittanceTimeoutCallbackResponse(BaseModel):
    """Schema for response to Tax Remittance timeout callback."""

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
