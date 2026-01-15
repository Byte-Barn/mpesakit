from _typeshed import Incomplete
from pydantic import BaseModel

class BusinessBuyGoodsRequest(BaseModel):
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
    Remarks: str
    QueueTimeOutURL: str
    ResultURL: str
    Occassion: str | None
    model_config: Incomplete

class BusinessBuyGoodsResponse(BaseModel):
    OriginatorConversationID: str | None
    ConversationID: str | None
    ResponseCode: str
    ResponseDescription: str
    model_config: Incomplete
    def is_successful(self) -> bool: ...

class BusinessBuyGoodsResultParameter(BaseModel):
    Key: str
    Value: str | int

class BusinessBuyGoodsReferenceItem(BaseModel):
    Key: str
    Value: str | int

class BusinessBuyGoodsReferenceData(BaseModel):
    ReferenceItem: list[BusinessBuyGoodsReferenceItem] | BusinessBuyGoodsReferenceItem

class BusinessBuyGoodsResultParameters(BaseModel):
    ResultParameter: list[BusinessBuyGoodsResultParameter] | BusinessBuyGoodsResultParameter

class BusinessBuyGoodsResultMetadata(BaseModel):
    ResultType: int
    ResultCode: int | str
    ResultDesc: str
    OriginatorConversationID: str
    ConversationID: str
    TransactionID: str | None
    ResultParameters: BusinessBuyGoodsResultParameters | None
    ReferenceData: BusinessBuyGoodsReferenceData | None
    model_config: Incomplete

class BusinessBuyGoodsResultCallback(BaseModel):
    Result: BusinessBuyGoodsResultMetadata
    model_config: Incomplete
    def is_successful(self) -> bool: ...

class BusinessBuyGoodsResultCallbackResponse(BaseModel):
    ResultCode: int | str
    ResultDesc: str
    model_config: Incomplete

class BusinessBuyGoodsTimeoutCallback(BaseModel):
    Result: BusinessBuyGoodsResultMetadata
    model_config: Incomplete

class BusinessBuyGoodsTimeoutCallbackResponse(BaseModel):
    ResultCode: int | str
    ResultDesc: str
    model_config: Incomplete
