from _typeshed import Incomplete
from pydantic import BaseModel, HttpUrl as HttpUrl

class B2BExpressCheckoutRequest(BaseModel):
    primaryShortCode: int
    receiverShortCode: int
    amount: int
    paymentRef: str
    callbackUrl: HttpUrl
    partnerName: str
    RequestRefID: str
    model_config: Incomplete

class B2BExpressCheckoutResponse(BaseModel):
    code: str
    status: str
    model_config: Incomplete
    def is_successful(self) -> bool: ...

class B2BExpressCheckoutCallback(BaseModel):
    resultCode: str
    resultDesc: str
    amount: float | None
    requestId: str
    paymentReference: str | None
    resultType: str | None
    conversationID: str | None
    transactionId: str | None
    status: str | None
    model_config: Incomplete
    def is_successful(self) -> bool: ...

class B2BExpressCallbackResponse(BaseModel):
    ResultCode: int | str
    ResultDesc: str
    model_config: Incomplete
