from _typeshed import Incomplete
from enum import Enum
from pydantic import BaseModel

class AccountBalanceIdentifierType(int, Enum):
    MSISDN = 1
    TILL_NUMBER = 2
    SHORT_CODE = 4

class AccountBalanceRequest(BaseModel):
    Initiator: str
    SecurityCredential: str
    CommandID: str
    PartyA: int
    IdentifierType: int
    Remarks: str
    QueueTimeOutURL: str
    ResultURL: str
    model_config: Incomplete
    @classmethod
    def validate(cls, values): ...

class AccountBalanceResponse(BaseModel):
    OriginatorConversationID: str | None
    ConversationID: str | None
    ResponseCode: str | int
    ResponseDescription: str
    model_config: Incomplete
    def is_successful(self) -> bool: ...

class AccountBalanceResultParameter(BaseModel):
    Key: str
    Value: str | int | float

class AccountBalanceReferenceItem(BaseModel):
    Key: str
    Value: str

class AccountBalanceReferenceData(BaseModel):
    ReferenceItem: AccountBalanceReferenceItem

class AccountBalanceResultParameters(BaseModel):
    ResultParameters: list[AccountBalanceResultParameter]

class AccountBalanceResultMetadata(BaseModel):
    ResultType: int
    ResultCode: int | str
    ResultDesc: str
    OriginatorConversationID: str
    ConversationID: str
    TransactionID: str | None
    ResultParameter: AccountBalanceResultParameters | None
    ReferenceData: AccountBalanceReferenceData | None
    model_config: Incomplete

class AccountBalanceResultCallback(BaseModel):
    Result: AccountBalanceResultMetadata
    model_config: Incomplete

class AccountBalanceResultCallbackResponse(BaseModel):
    ResultCode: int | str
    ResultDesc: str
    model_config: Incomplete

class AccountBalanceTimeoutCallback(BaseModel):
    Result: AccountBalanceResultMetadata
    model_config: Incomplete

class AccountBalanceTimeoutCallbackResponse(BaseModel):
    ResultCode: int | str
    ResultDesc: str
    model_config: Incomplete
