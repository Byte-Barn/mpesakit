from _typeshed import Incomplete
from enum import Enum
from mpesakit.utils.phone import normalize_phone_number as normalize_phone_number
from pydantic import BaseModel

class B2CCommandIDType(str, Enum):
    SalaryPayment = 'SalaryPayment'
    BusinessPayment = 'BusinessPayment'
    PromotionPayment = 'PromotionPayment'

class B2CRequest(BaseModel):
    OriginatorConversationID: str
    InitiatorName: str
    SecurityCredential: str
    CommandID: str
    Amount: int
    PartyA: int
    PartyB: int
    Remarks: str
    QueueTimeOutURL: str
    ResultURL: str
    Occasion: str | None
    model_config: Incomplete
    @classmethod
    def validate(cls, values): ...

class B2CResponse(BaseModel):
    ConversationID: str | None
    OriginatorConversationID: str | None
    ResponseCode: str | int
    ResponseDescription: str
    model_config: Incomplete
    def is_successful(self) -> bool: ...

class B2CResultParameter(BaseModel):
    Key: str
    Value: str | int | float

class B2CResultMetadata(BaseModel):
    ResultType: int
    ResultCode: int | str
    ResultDesc: str
    OriginatorConversationID: str
    ConversationID: str
    TransactionID: str | None
    ResultParameters: list[B2CResultParameter] | None
    model_config: Incomplete
    def __init__(self, **data) -> None: ...
    @property
    def transaction_amount(self) -> int | float | None: ...
    @property
    def transaction_receipt(self) -> str | None: ...
    @property
    def recipient_is_registered(self) -> bool | None: ...
    @property
    def receiver_party_public_name(self) -> str | None: ...
    @property
    def transaction_completed_datetime(self) -> str | None: ...
    @property
    def charges_paid_account_available_funds(self) -> float | None: ...
    @property
    def utility_account_available_funds(self) -> float | None: ...
    @property
    def working_account_available_funds(self) -> float | None: ...

class B2CResultCallback(BaseModel):
    Result: B2CResultMetadata
    model_config: Incomplete

class B2CResultCallbackResponse(BaseModel):
    ResultCode: int | str
    ResultDesc: str
    model_config: Incomplete

class B2CTimeoutCallback(BaseModel):
    Result: B2CResultMetadata
    model_config: Incomplete

class B2CTimeoutCallbackResponse(BaseModel):
    ResultCode: int | str
    ResultDesc: str
    model_config: Incomplete
