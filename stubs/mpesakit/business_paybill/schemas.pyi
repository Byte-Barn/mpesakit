from _typeshed import Incomplete
from pydantic import BaseModel

class BusinessPayBillRequest(BaseModel):
    Initiator: str
    SecurityCredential: str
    Amount: int
    PartyA: int
    PartyB: int
    AccountReference: str
    Requester: str | None
    Remarks: str
    QueueTimeOutURL: str
    ResultURL: str
    CommandID: str
    SenderIdentifierType: int
    RecieverIdentifierType: int
    model_config: Incomplete

class BusinessPayBillResponse(BaseModel):
    OriginatorConversationID: str | None
    ConversationID: str | None
    ResponseCode: str
    ResponseDescription: str
    model_config: Incomplete
    def is_successful(self) -> bool: ...

class BusinessPayBillResultParameter(BaseModel):
    Key: str
    Value: str

class BusinessPayBillReferenceItem(BaseModel):
    Key: str
    Value: str

class BusinessPayBillReferenceData(BaseModel):
    ReferenceItem: list[BusinessPayBillReferenceItem]

class BusinessPayBillResultParameters(BaseModel):
    ResultParameter: list[BusinessPayBillResultParameter]

class BusinessPayBillResultMetadata(BaseModel):
    ResultType: int
    ResultCode: int | str
    ResultDesc: str
    OriginatorConversationID: str
    ConversationID: str
    TransactionID: str | None
    ResultParameters: BusinessPayBillResultParameters | None
    ReferenceData: BusinessPayBillReferenceData | None
    model_config: Incomplete

class BusinessPayBillResultCallback(BaseModel):
    Result: BusinessPayBillResultMetadata
    model_config: Incomplete
    def is_successful(self) -> bool: ...

class BusinessPayBillResultCallbackResponse(BaseModel):
    ResultCode: int | str
    ResultDesc: str
    model_config: Incomplete

class BusinessPayBillTimeoutCallback(BaseModel):
    Result: BusinessPayBillResultMetadata
    model_config: Incomplete

class BusinessPayBillTimeoutCallbackResponse(BaseModel):
    ResultCode: int | str
    ResultDesc: str
    model_config: Incomplete
