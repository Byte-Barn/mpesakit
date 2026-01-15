from _typeshed import Incomplete
from enum import Enum
from mpesakit.utils.phone import normalize_phone_number as normalize_phone_number
from pydantic import BaseModel

class TransactionType(str, Enum):
    CUSTOMER_PAYBILL_ONLINE = 'CustomerPayBillOnline'
    CUSTOMER_BUYGOODS_ONLINE = 'CustomerBuyGoodsOnline'

class StkPushSimulateRequest(BaseModel):
    BusinessShortCode: int
    Password: str | None
    Timestamp: str | None
    TransactionType: str
    Amount: int
    PartyA: str
    PartyB: str
    PhoneNumber: str
    CallBackURL: str
    AccountReference: str
    TransactionDesc: str
    Passkey: str | None
    model_config: Incomplete
    def __init__(self, **data) -> None: ...
    @classmethod
    def validate(cls, values): ...

class StkPushSimulateResponse(BaseModel):
    MerchantRequestID: str
    CheckoutRequestID: str
    ResponseCode: int
    ResponseDescription: str
    CustomerMessage: str
    model_config: Incomplete
    def is_successful(self) -> bool: ...

class StkPushSimulateCallbackMetadataItem(BaseModel):
    Name: str
    Value: str | int | float | None
    @classmethod
    def parse_value_conditionally(cls, data): ...

class StkPushSimulateCallbackMetadata(BaseModel):
    Item: list[StkPushSimulateCallbackMetadataItem]

class StkCallback(BaseModel):
    MerchantRequestID: str
    CheckoutRequestID: str
    ResultCode: int | str
    ResultDesc: str
    CallbackMetadata: StkPushSimulateCallbackMetadata | None

class StkPushSimulateCallbackBody(BaseModel):
    stkCallback: StkCallback

class StkPushSimulateCallback(BaseModel):
    Body: StkPushSimulateCallbackBody
    model_config: Incomplete
    def get_metadata_value(self, name: str) -> str | int | float | None: ...
    @property
    def amount(self) -> float | None: ...
    @property
    def mpesa_receipt_number(self) -> str | None: ...
    @property
    def balance(self) -> float | None: ...
    @property
    def transaction_date(self) -> str | None: ...
    @property
    def phone_number(self) -> str | None: ...
    @property
    def is_successful(self) -> bool: ...

class StkPushSimulateCallbackResponse(BaseModel):
    ResultCode: int | str
    ResultDesc: str
    model_config: Incomplete

class StkPushQueryRequest(BaseModel):
    BusinessShortCode: int
    Password: str | None
    Timestamp: str | None
    CheckoutRequestID: str
    Passkey: str | None
    model_config: Incomplete
    def __init__(self, **data) -> None: ...
    @classmethod
    def validate(cls, values): ...

class StkPushQueryResponse(BaseModel):
    MerchantRequestID: str
    CheckoutRequestID: str
    ResponseCode: int | str
    ResponseDescription: str
    ResultCode: int | str
    ResultDesc: str
    model_config: Incomplete
    def is_successful(self) -> bool: ...
