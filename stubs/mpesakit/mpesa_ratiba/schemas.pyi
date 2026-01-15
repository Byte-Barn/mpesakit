from _typeshed import Incomplete
from enum import Enum
from mpesakit.utils.phone import normalize_phone_number as normalize_phone_number
from pydantic import BaseModel, HttpUrl as HttpUrl
from typing import Any

class FrequencyEnum(str, Enum):
    ONE_OFF = '1'
    DAILY = '2'
    WEEKLY = '3'
    MONTHLY = '4'
    BI_MONTHLY = '5'
    QUARTERLY = '6'
    HALF_YEAR = '7'
    YEARLY = '8'

class TransactionTypeEnum(str, Enum):
    STANDING_ORDER_CUSTOMER_PAY_BILL = 'Standing Order Customer Pay Bill'
    STANDING_ORDER_CUSTOMER_PAY_MERCHANT = 'Standing Order Customer Pay Merchant'

class ReceiverPartyIdentifierTypeEnum(str, Enum):
    MERCHANT_TILL = '2'
    BUSINESS_SHORT_CODE = '4'

class StandingOrderRequest(BaseModel):
    StandingOrderName: str
    StartDate: str
    EndDate: str
    BusinessShortCode: str
    TransactionType: TransactionTypeEnum
    ReceiverPartyIdentifierType: ReceiverPartyIdentifierTypeEnum
    Amount: str
    PartyA: str
    CallBackURL: HttpUrl
    AccountReference: str
    TransactionDesc: str
    Frequency: FrequencyEnum
    model_config: Incomplete
    @classmethod
    def validate(cls, values): ...
    @classmethod
    def format_date(cls, date_str: str) -> str: ...

class StandingOrderResponseHeader(BaseModel):
    responseRefID: str
    requestRefID: str | None
    responseCode: str
    responseDescription: str
    ResultDesc: str | None
    model_config: Incomplete

class StandingOrderResponseBody(BaseModel):
    responseDescription: str | None
    responseCode: str | None
    model_config: Incomplete

class StandingOrderResponse(BaseModel):
    ResponseHeader: StandingOrderResponseHeader
    ResponseBody: StandingOrderResponseBody
    model_config: Incomplete
    def is_successful(self) -> bool: ...

class StandingOrderCallbackDataItem(BaseModel):
    Name: str
    Value: Any
    model_config: Incomplete

class StandingOrderCallbackBody(BaseModel):
    ResponseData: list[StandingOrderCallbackDataItem]
    model_config: Incomplete

class StandingOrderCallback(BaseModel):
    ResponseHeader: StandingOrderResponseHeader
    ResponseBody: StandingOrderCallbackBody
    model_config: Incomplete
    def is_successful(self) -> bool: ...

class StandingOrderCallbackResponse(BaseModel):
    ResultDesc: str
    ResultCode: str
    model_config: Incomplete
