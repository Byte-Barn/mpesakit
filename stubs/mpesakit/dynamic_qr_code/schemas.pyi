from _typeshed import Incomplete
from enum import Enum
from mpesakit.utils.phone import normalize_phone_number as normalize_phone_number
from pydantic import BaseModel

class DynamicQRTransactionType(str, Enum):
    BUY_GOODS = 'BG'
    WITHDRAW_CASH = 'WA'
    PAYBILL = 'PB'
    SEND_MONEY = 'SM'
    SEND_TO_BUSINESS = 'SB'

class DynamicQRGenerateRequest(BaseModel):
    MerchantName: str
    RefNo: str
    Amount: int
    TrxCode: str
    CPI: str
    Size: str
    model_config: Incomplete
    def validate(cls, values): ...

class DynamicQRGenerateResponse(BaseModel):
    ResponseCode: str | int
    ResponseDescription: str
    QRCode: str
    model_config: Incomplete
    def is_successful(self) -> bool: ...
