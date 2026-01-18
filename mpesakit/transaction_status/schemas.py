"""This module defines schemas for M-Pesa Transaction Status API requests and responses.

It includes models for transaction status queries, responses, and result notifications.
"""

from enum import Enum
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field, model_validator

from mpesakit.utils.phone import normalize_phone_number


class TransactionStatusIdentifierType(int, Enum):
    """Allowed values for IdentifierType in Transaction Status requests."""

    MSISDN = 1  # MSISDN (phone number)
    TILL_NUMBER = 2  # Till Number
    SHORT_CODE = 4  # Organization Short Code


class TransactionStatusRequest(BaseModel):
    """Request schema for Transaction Status query.

    Attributes:
        Initiator (str): Username used to initiate the request.
        SecurityCredential (str): Encrypted security credential.
        CommandID (str): TransactionStatusQuery.
        TransactionID (str): M-Pesa transaction ID to query.
        PartyA (int): Organization shortcode or MSISDN.
        IdentifierType (str): Type of identifier for PartyA.
        ResultURL (str): URL for result notifications.
        QueueTimeOutURL (str): URL for timeout notifications.
        Remarks (str): Comments for the transaction.
        Occasion (Optional[str]): Optional occasion for the query.
        OriginatorConversationID (Optional[str]): Can be Used to query if you don't have the TransactionID.
    """

    Initiator: str = Field(..., description="Username used to initiate the request.")
    SecurityCredential: str = Field(..., description="Encrypted security credential.")
    CommandID: str = Field(
        default="TransactionStatusQuery", description="Type of transaction to perform."
    )
    TransactionID: Optional[str] = Field(
        None, description="M-Pesa transaction ID to query."
    )
    PartyA: int = Field(..., description="Organization shortcode or MSISDN.")
    IdentifierType: int = Field(..., description="Type of identifier for PartyA.")
    ResultURL: str = Field(..., description="URL for result notifications.")
    QueueTimeOutURL: str = Field(..., description="URL for timeout notifications.")
    Remarks: str = Field(
        default="Status Query", description="Comments for the transaction."
    )
    Occasion: Optional[str] = Field(
        None, description="Optional occasion for the query."
    )
    OriginalConversationID: Optional[str] = Field(
        None,
        description="Can be used to query if you don't have the TransactionID.",
    )

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "Initiator": "testapi",
                "SecurityCredential": "encrypted_credential",
                "CommandID": "TransactionStatusQuery",
                "TransactionID": "LKXXXX1234",
                "PartyA": 600999,
                "IdentifierType": "4",
                "ResultURL": "https://example.com/result",
                "QueueTimeOutURL": "https://example.com/timeout",
                "Remarks": "Status check for transaction",
                "Occasion": "JuneSalary",
                "OriginalConversationID": "12345-67890-1",
            }
        }
    )

    @model_validator(mode="before")
    @classmethod
    def validate(cls, values):
        """Validate IdentifierType and Remarks/Occasion length."""
        cls._validate_identifier_type(values)
        cls._validate_remarks(values)
        cls._validate_occasion(values)
        cls._normalize_party_a_if_msisdn(values)
        cls._validate_original_conversation_id_or_transaction_id(values)
        return values

    @classmethod
    def _validate_original_conversation_id_or_transaction_id(cls, values):
        """Ensure at least one of OriginalConversationID or TransactionID is provided."""
        original_conversation_id = values.get("OriginalConversationID")
        transaction_id = values.get("TransactionID")
        if not original_conversation_id and not transaction_id:
            raise ValueError(
                "At least one of OriginalConversationID or TransactionID must be provided."
            )
        return values

    @classmethod
    def _normalize_party_a_if_msisdn(cls, values):
        """Normalize PartyA if IdentifierType is MSISDN."""
        identifier_type = values.get("IdentifierType")
        party_a = values.get("PartyA")
        if identifier_type == TransactionStatusIdentifierType.MSISDN.value:
            normalized = normalize_phone_number(str(party_a))
            if normalized is None:
                raise ValueError(
                    "PartyA must be a valid Kenyan MSISDN(PhoneNumber) when IdentifierType is '1'."
                )
            values["PartyA"] = int(normalized)
        return values

    @classmethod
    def _validate_identifier_type(cls, values):
        """Ensure IdentifierType is valid."""
        identifier_type = values.get("IdentifierType")
        valid_types = [e.value for e in TransactionStatusIdentifierType]
        if identifier_type not in valid_types:
            raise ValueError(
                f"IdentifierType must be one of {valid_types}, got '{identifier_type}'"
            )
        return values

    @classmethod
    def _validate_remarks(cls, values):
        """Ensure Remarks is not more than 100 characters."""
        remarks = values.get("Remarks")
        if remarks is not None and len(remarks) > 100:
            raise ValueError("Remarks must not exceed 100 characters.")
        return values

    @classmethod
    def _validate_occasion(cls, values):
        """Ensure Occasion is not more than 100 characters."""
        occasion = values.get("Occasion")
        if occasion is not None and len(occasion) > 100:
            raise ValueError("Occasion must not exceed 100 characters.")
        return values


class TransactionStatusResponse(BaseModel):
    """Response schema for Transaction Status query."""

    ConversationID: Optional[str] = Field(
        ..., description="Unique ID for the transaction status request."
    )
    OriginatorConversationID: Optional[str] = Field(
        ..., description="ID for tracking the request."
    )
    ResponseCode: str | int = Field(..., description="Status code, 0 means success.")
    ResponseDescription: str = Field(..., description="Status message.")

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "ConversationID": "AG_20170717_00006c6f7f5b8b6b1a62",
                "OriginatorConversationID": "12345-67890-1",
                "ResponseCode": "0",
                "ResponseDescription": "Accept the service request successfully.",
            }
        }
    )

    def is_successful(self) -> bool:
        """Return True if ResponseCode indicates success (e.g., '0', '00000000')."""
        code = str(self.ResponseCode)
        return code.strip("0") == "" and code != ""


class TransactionStatusResultParameter(BaseModel):
    """Parameter item in Transaction Status result notification."""

    Key: str = Field(..., description="Parameter name.")
    Value: str | int | float = Field(..., description="Parameter value.")


class TransactionStatusResultMetadata(BaseModel):
    """Metadata for Transaction Status result notification."""

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
    ResultParameters: Optional[list[TransactionStatusResultParameter]] = Field(
        None, description="List of result parameters."
    )

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "ResultType": 0,
                "ResultCode": 0,
                "ResultDesc": "The service request is processed successfully.",
                "OriginatorConversationID": "12345-67890-1",
                "ConversationID": "AG_20170717_00006c6f7f5b8b6b1a62",
                "TransactionID": "LKXXXX1234",
                "ResultParameters": [
                    {"Key": "TransactionAmount", "Value": 1000},
                    {"Key": "TransactionReceipt", "Value": "LKXXXX1234"},
                    {"Key": "Status", "Value": "Completed"},
                ],
            }
        }
    )

    def __init__(self, **data):
        """Initialize TransactionStatusResultMetadata and cache parameters as a dictionary."""
        super().__init__(**data)
        self._parameters_dict = {
            param.Key: param.Value for param in self.ResultParameters or []
        }

    @property
    def transaction_amount(self) -> Optional[int | float]:
        """Return the TransactionAmount value if present."""
        return self._parameters_dict.get("TransactionAmount")

    @property
    def transaction_receipt(self) -> Optional[str]:
        """Return the TransactionReceipt value if present."""
        return self._parameters_dict.get("TransactionReceipt")

    @property
    def transaction_status(self) -> Optional[str]:
        """Return the Status value if present (e.g., 'Completed', 'Failed')."""
        return self._parameters_dict.get("Status")

    @property
    def transaction_reason(self) -> Optional[str]:
        """Return the Reason value if present (e.g., reason for failure)."""
        return self._parameters_dict.get("Reason")


class TransactionStatusResultCallback(BaseModel):
    """Schema for Transaction Status result notification sent to ResultURL."""

    Result: TransactionStatusResultMetadata = Field(..., description="Result metadata.")

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "Result": {
                    "ResultType": 0,
                    "ResultCode": 0,
                    "ResultDesc": "The service request is processed successfully.",
                    "OriginatorConversationID": "12345-67890-1",
                    "ConversationID": "AG_20170717_00006c6f7f5b8b6b1a62",
                    "TransactionID": "LKXXXX1234",
                    "ResultParameters": [
                        {"Key": "TransactionAmount", "Value": 1000},
                        {"Key": "TransactionReceipt", "Value": "LKXXXX1234"},
                        {"Key": "Status", "Value": "Completed"},
                    ],
                }
            }
        }
    )

    def is_successful(self) -> bool:
        """Return True if ResultCode indicates success (e.g., '0', '00000000')."""
        code = str(self.Result.ResultCode)
        return code.strip("0") == "" and code != ""


class TransactionStatusResultCallbackResponse(BaseModel):
    """Schema for response to Transaction Status result callback."""

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


class TransactionStatusTimeoutCallback(BaseModel):
    """Schema for Transaction Status timeout notification sent to QueueTimeOutURL."""

    Result: TransactionStatusResultMetadata = Field(..., description="Result metadata.")

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "Result": {
                    "ResultType": 1,
                    "ResultCode": 1,
                    "ResultDesc": "The service request timed out.",
                    "OriginatorConversationID": "12345-67890-1",
                    "ConversationID": "AG_20170717_00006c6f7f5b8b6b1a62",
                }
            }
        }
    )


class TransactionStatusTimeoutCallbackResponse(BaseModel):
    """Schema for response to Transaction Status timeout callback."""

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
