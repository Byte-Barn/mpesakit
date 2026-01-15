"""This module defines schemas for M-Pesa Account Balance API requests and responses.

It includes models for account balance queries, responses, and result notifications.
"""

from enum import Enum
from pydantic import BaseModel, Field, ConfigDict, model_validator
from typing import Optional, List


class AccountBalanceIdentifierType(int, Enum):
    """Allowed values for IdentifierType in Account Balance requests."""

    MSISDN = 1  # MSISDN (phone number)
    TILL_NUMBER = 2  # Till Number
    SHORT_CODE = 4  # Organization Short Code


class AccountBalanceRequest(BaseModel):
    """Request schema for Account Balance query."""

    Initiator: str = Field(..., description="Username used to initiate the request.")
    SecurityCredential: str = Field(..., description="Encrypted security credential.")
    CommandID: str = Field(
        default="AccountBalance", description="Type of transaction to perform."
    )
    PartyA: int = Field(..., description="Organization shortcode or MSISDN.")
    IdentifierType: int = Field(..., description="Type of identifier for PartyA.")
    Remarks: str = Field(
        ..., description="Comments for the transaction (max 100 chars)."
    )
    QueueTimeOutURL: str = Field(..., description="URL for timeout notifications.")
    ResultURL: str = Field(..., description="URL for result notifications.")

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "Initiator": "testapiuser",
                "SecurityCredential": "encrypted_credential",
                "CommandID": "AccountBalance",
                "PartyA": 600000,
                "IdentifierType": 4,
                "Remarks": "ok",
                "QueueTimeOutURL": "http://myservice:8080/queuetimeouturl",
                "ResultURL": "http://myservice:8080/result",
            }
        }
    )

    @model_validator(mode="before")
    @classmethod
    def validate(cls, values):
        """Validate IdentifierType and Remarks length."""
        cls._validate_identifier_type(values)
        cls._validate_remarks(values)
        return values

    @classmethod
    def _validate_identifier_type(cls, values):
        identifier_type = values.get("IdentifierType")
        valid_types = [e.value for e in AccountBalanceIdentifierType]
        if identifier_type not in valid_types:
            raise ValueError(
                f"IdentifierType must be one of {valid_types}, got '{identifier_type}'"
            )
        return values

    @classmethod
    def _validate_remarks(cls, values):
        remarks = values.get("Remarks")
        if remarks is not None and len(remarks) > 100:
            raise ValueError("Remarks must not exceed 100 characters.")
        return values


class AccountBalanceResponse(BaseModel):
    """Response schema for Account Balance query."""

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
                "OriginatorConversationID": "515-5258779-3",
                "ConversationID": "AG_20200123_0000417fed8ed666e976",
                "ResponseCode": "0",
                "ResponseDescription": "Accept the service request successfully",
            }
        }
    )

    def is_successful(self) -> bool:
        """Check if the response indicates success."""
        code = str(self.ResponseCode)
        return code.strip("0") == "" and code != ""


class AccountBalanceResultParameter(BaseModel):
    """Parameter item in Account Balance result notification."""

    Key: str = Field(..., description="Parameter name.")
    Value: str | int | float = Field(..., description="Parameter value.")


class AccountBalanceReferenceItem(BaseModel):
    """Reference item in Account Balance result notification."""

    Key: str = Field(..., description="Reference parameter name.")
    Value: str = Field(..., description="Reference parameter value.")


class AccountBalanceReferenceData(BaseModel):
    """Reference data in Account Balance result notification."""

    ReferenceItem: AccountBalanceReferenceItem = Field(
        ..., description="Reference item."
    )


class AccountBalanceResultParameters(BaseModel):
    """Result parameters container."""

    ResultParameters: List[AccountBalanceResultParameter] = Field(
        ..., description="List of result parameters."
    )


class AccountBalanceResultMetadata(BaseModel):
    """Metadata for Account Balance result notification."""

    ResultType: int = Field(..., description="Type of result (0=Success, 1=Waiting).")
    ResultCode: int | str = Field(..., description="Result code (0=Success).")
    ResultDesc: str = Field(..., description="Result description.")
    OriginatorConversationID: str = Field(
        ..., description="Originator conversation ID."
    )
    ConversationID: str = Field(..., description="Conversation ID.")
    TransactionID: Optional[str] = Field(
        None, description="M-Pesa transaction ID (if available)."
    )
    ResultParameter: Optional[AccountBalanceResultParameters] = Field(
        None, description="Result parameters container."
    )
    ReferenceData: Optional[AccountBalanceReferenceData] = Field(
        None, description="Reference data."
    )

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "ResultType": 0,
                "ResultCode": 0,
                "ResultDesc": "The service request is processed successfully",
                "OriginatorConversationID": "16917-22577599-3",
                "ConversationID": "AG_20200206_00005e091a8ec6b9eac5",
                "TransactionID": "OA90000000",
                "ResultParameter": {
                    "ResultParameters": [
                        {
                            "Key": "AccountBalance",
                            "Value": (
                                "Working Account|KES|700000.00|700000.00|0.00|0.00&"
                                "Float Account|KES|0.00|0.00|0.00|0.00&"
                                "Utility Account|KES|228037.00|228037.00|0.00|0.00&"
                                "Charges Paid Account|KES|-1540.00|-1540.00|0.00|0.00&"
                                "Organization Settlement Account|KES|0.00|0.00|0.00|0.00"
                            ),
                        },
                        {
                            "Key": "BOCompletedTime",
                            "Value": "20200109125710",
                        },
                    ]
                },
                "ReferenceData": {
                    "ReferenceItem": {
                        "Key": "QueueTimeoutURL",
                        "Value": "https://internalsandbox.safaricom.co.ke/mpesa/abresults/v1/submit",
                    }
                },
            }
        }
    )


class AccountBalanceResultCallback(BaseModel):
    """Schema for Account Balance result notification sent to ResultURL."""

    Result: AccountBalanceResultMetadata = Field(..., description="Result metadata.")

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "Result": {
                    "ResultType": 0,
                    "ResultCode": 0,
                    "ResultDesc": "The service request is processed successfully",
                    "OriginatorConversationID": "16917-22577599-3",
                    "ConversationID": "AG_20200206_00005e091a8ec6b9eac5",
                    "TransactionID": "OA90000000",
                    "ResultParameter": {
                        "ResultParameters": [
                            {
                                "Key": "AccountBalance",
                                "Value": (
                                    "Working Account|KES|700000.00|700000.00|0.00|0.00&"
                                    "Float Account|KES|0.00|0.00|0.00|0.00&"
                                    "Utility Account|KES|228037.00|228037.00|0.00|0.00&"
                                    "Charges Paid Account|KES|-1540.00|-1540.00|0.00|0.00&"
                                    "Organization Settlement Account|KES|0.00|0.00|0.00|0.00"
                                ),
                            },
                            {
                                "Key": "BOCompletedTime",
                                "Value": "20200109125710",
                            },
                        ]
                    },
                    "ReferenceData": {
                        "ReferenceItem": {
                            "Key": "QueueTimeoutURL",
                            "Value": "https://internalsandbox.safaricom.co.ke/mpesa/abresults/v1/submit",
                        }
                    },
                }
            }
        }
    )


class AccountBalanceResultCallbackResponse(BaseModel):
    """Schema for response to Account Balance result callback."""

    ResultCode: int | str = Field(
        default=0, description="Result code (0=Success, other=Failure)."
    )
    ResultDesc: str = Field(
        default="Result received and processed successfully.",
        description="Result description.",
    )

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "ResultCode": 0,
                "ResultDesc": "Result received and processed successfully.",
            }
        }
    )


class AccountBalanceTimeoutCallback(BaseModel):
    """Schema for Account Balance timeout notification sent to QueueTimeOutURL."""

    Result: AccountBalanceResultMetadata = Field(..., description="Result metadata.")

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "Result": {
                    "ResultType": 1,
                    "ResultCode": 1,
                    "ResultDesc": "The service request timed out.",
                    "OriginatorConversationID": "16917-22577599-3",
                    "ConversationID": "AG_20200206_00005e091a8ec6b9eac5",
                }
            }
        }
    )


class AccountBalanceTimeoutCallbackResponse(BaseModel):
    """Schema for response to Account Balance timeout callback."""

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
