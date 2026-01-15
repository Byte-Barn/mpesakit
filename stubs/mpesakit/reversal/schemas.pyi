from _typeshed import Incomplete
from pydantic import BaseModel

class ReversalRequest(BaseModel):
    Initiator: str
    SecurityCredential: str
    TransactionID: str
    Amount: int
    ReceiverParty: int
    ResultURL: str
    QueueTimeOutURL: str
    Remarks: str
    Occasion: str | None
    CommandID: str
    RecieverIdentifierType: str
    model_config: Incomplete
    @classmethod
    def validate(cls, values): ...

class ReversalResponse(BaseModel):
    OriginatorConversationID: str | None
    ConversationID: str | None
    ResponseCode: str | int
    ResponseDescription: str
    model_config: Incomplete
    def is_successful(self) -> bool: ...

class ReversalResultParameter(BaseModel):
    Key: str
    Value: str

class ReversalReferenceItem(BaseModel):
    Key: str
    Value: str

class ReversalReferenceData(BaseModel):
    ReferenceItem: ReversalReferenceItem

class ReversalResultParameters(BaseModel):
    ResultParameter: list[ReversalResultParameter]

class ReversalResultMetadata(BaseModel):
    ResultType: int
    ResultCode: str
    ResultDesc: str
    OriginatorConversationID: str
    ConversationID: str
    TransactionID: str | None
    ResultParameters: ReversalResultParameters | None
    ReferenceData: ReversalReferenceData | None
    model_config: Incomplete

class ReversalResultCallback(BaseModel):
    Result: ReversalResultMetadata
    model_config: Incomplete

class ReversalResultCallbackResponse(BaseModel):
    ResultCode: int | str
    ResultDesc: str
    model_config: Incomplete

class ReversalTimeoutCallback(BaseModel):
    Result: ReversalResultMetadata
    model_config: Incomplete

class ReversalTimeoutCallbackResponse(BaseModel):
    ResultCode: int | str
    ResultDesc: str
    model_config: Incomplete
