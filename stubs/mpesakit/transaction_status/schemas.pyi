from _typeshed import Incomplete
from enum import Enum
from mpesakit.utils.phone import normalize_phone_number as normalize_phone_number
from pydantic import BaseModel

class TransactionStatusIdentifierType(int, Enum):
    MSISDN = 1
    TILL_NUMBER = 2
    SHORT_CODE = 4

class TransactionStatusRequest(BaseModel):
    Initiator: str
    SecurityCredential: str
    CommandID: str
    TransactionID: str | None
    PartyA: int
    IdentifierType: int
    ResultURL: str
    QueueTimeOutURL: str
    Remarks: str
    Occasion: str | None
    OriginalConversationID: str | None
    model_config: Incomplete
    @classmethod
    def validate(cls, values): ...

class TransactionStatusResponse(BaseModel):
    ConversationID: str | None
    OriginatorConversationID: str | None
    ResponseCode: str | int
    ResponseDescription: str
    model_config: Incomplete
    def is_successful(self) -> bool: ...

class TransactionStatusResultParameter(BaseModel):
    Key: str
    Value: str | int | float

class TransactionStatusResultMetadata(BaseModel):
    ResultType: int
    ResultCode: int | str
    ResultDesc: str
    OriginatorConversationID: str
    ConversationID: str
    TransactionID: str | None
    ResultParameters: list[TransactionStatusResultParameter] | None
    model_config: Incomplete
    def __init__(self, **data) -> None: ...
    @property
    def transaction_amount(self) -> int | float | None: ...
    @property
    def transaction_receipt(self) -> str | None: ...
    @property
    def transaction_status(self) -> str | None: ...
    @property
    def transaction_reason(self) -> str | None: ...

class TransactionStatusResultCallback(BaseModel):
    Result: TransactionStatusResultMetadata
    model_config: Incomplete

class TransactionStatusResultCallbackResponse(BaseModel):
    ResultCode: int | str
    ResultDesc: str
    model_config: Incomplete

class TransactionStatusTimeoutCallback(BaseModel):
    Result: TransactionStatusResultMetadata
    model_config: Incomplete

class TransactionStatusTimeoutCallbackResponse(BaseModel):
    ResultCode: int | str
    ResultDesc: str
    model_config: Incomplete
