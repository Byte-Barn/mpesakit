from _typeshed import Incomplete
from pydantic import BaseModel, HttpUrl as HttpUrl
from typing import Any

class B2CAccountTopUpRequest(BaseModel):
    Initiator: str
    SecurityCredential: str
    CommandID: str
    SenderIdentifierType: int
    RecieverIdentifierType: int
    Amount: int
    PartyA: int
    PartyB: int
    AccountReference: str
    Requester: str | None
    Remarks: str | None
    QueueTimeOutURL: HttpUrl
    ResultURL: HttpUrl
    model_config: Incomplete

class B2CAccountTopUpResponse(BaseModel):
    OriginatorConversationID: str
    ConversationID: str
    ResponseCode: str
    ResponseDescription: str
    model_config: Incomplete
    def is_successful(self) -> bool: ...

class ResultParameterItem(BaseModel):
    Key: str
    Value: Any

class RefItem(BaseModel):
    Key: str
    Value: Any | None

class ResultParams(BaseModel):
    ResultParameter: list[ResultParameterItem]

class RefData(BaseModel):
    ReferenceItem: list[RefItem]
    model_config: Incomplete

class B2CAccountTopUpCallbackResult(BaseModel):
    ResultType: int
    ResultCode: int | str
    ResultDesc: str
    OriginatorConversationID: str
    ConversationID: str
    TransactionID: str
    ResultParameters: ResultParams | None
    ReferenceData: RefData | None
    model_config: Incomplete

class B2CAccountTopUpCallback(BaseModel):
    Result: B2CAccountTopUpCallbackResult
    model_config: Incomplete
    def is_successful(self) -> bool: ...

class B2CAccountTopUpCallbackResponse(BaseModel):
    ResultCode: int | str
    ResultDesc: str

class B2CAccountTopUpTimeoutResultMetadata(BaseModel):
    ResultType: int
    ResultCode: str
    ResultDesc: str
    OriginatorConversationID: str
    ConversationID: str
    model_config: Incomplete

class B2CAccountTopUpTimeoutCallback(BaseModel):
    Result: B2CAccountTopUpTimeoutResultMetadata
    model_config: Incomplete

class B2CAccountTopUpTimeoutCallbackResponse(BaseModel):
    ResultCode: int | str
    ResultDesc: str
    model_config: Incomplete
