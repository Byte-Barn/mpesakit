from _typeshed import Incomplete
from pydantic import BaseModel

class TaxRemittanceRequest(BaseModel):
    Initiator: str
    SecurityCredential: str
    Amount: int
    PartyA: int
    AccountReference: str
    Remarks: str
    QueueTimeOutURL: str
    ResultURL: str
    PartyB: int
    CommandID: str
    SenderIdentifierType: int
    RecieverIdentifierType: int
    model_config: Incomplete

class TaxRemittanceResponse(BaseModel):
    OriginatorConversationID: str | None
    ConversationID: str | None
    ResponseCode: str | int
    ResponseDescription: str
    model_config: Incomplete
    def is_successful(self) -> bool: ...

class TaxRemittanceResultParameter(BaseModel):
    Key: str
    Value: str

class TaxRemittanceReferenceItem(BaseModel):
    Key: str
    Value: str

class TaxRemittanceReferenceData(BaseModel):
    ReferenceItem: list[TaxRemittanceReferenceItem]

class TaxRemittanceResultParameters(BaseModel):
    ResultParameter: list[TaxRemittanceResultParameter]

class TaxRemittanceResultMetadata(BaseModel):
    ResultType: int
    ResultCode: int | str
    ResultDesc: str
    OriginatorConversationID: str
    ConversationID: str
    TransactionID: str | None
    ResultParameters: TaxRemittanceResultParameters | None
    ReferenceData: TaxRemittanceReferenceData | None
    model_config: Incomplete

class TaxRemittanceResultCallback(BaseModel):
    Result: TaxRemittanceResultMetadata
    model_config: Incomplete
    def is_successful(self) -> bool: ...

class TaxRemittanceResultCallbackResponse(BaseModel):
    ResultCode: int | str
    ResultDesc: str
    model_config: Incomplete

class TaxRemittanceTimeoutCallback(BaseModel):
    Result: TaxRemittanceResultMetadata
    model_config: Incomplete

class TaxRemittanceTimeoutCallbackResponse(BaseModel):
    ResultCode: int | str
    ResultDesc: str
    model_config: Incomplete
