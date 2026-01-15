from _typeshed import Incomplete
from enum import Enum
from pydantic import BaseModel

class C2BResponseType(str, Enum):
    COMPLETED = 'Completed'
    CANCELLED = 'Cancelled'

class C2BValidationResultCodeType(str, Enum):
    ACCEPTED = '0'
    INVALID_MSISDN = 'C2B00011'
    INVALID_ACCOUNT_NUMBER = 'C2B00012'
    INVALID_AMOUNT = 'C2B00013'
    INVALID_KYC_DETAILS = 'C2B00014'
    INVALID_SHORTCODE = 'C2B00015'
    OTHER_ERROR = 'C2B00016'

class C2BRegisterUrlRequest(BaseModel):
    ShortCode: int
    ResponseType: str
    ConfirmationURL: str
    ValidationURL: str
    model_config: Incomplete
    @classmethod
    def validate(cls, values): ...

class C2BRegisterUrlResponse(BaseModel):
    OriginatorConversationID: str | None
    ResponseCode: str | int
    ResponseDescription: str
    model_config: Incomplete
    def is_successful(self) -> bool: ...

class C2BValidationRequest(BaseModel):
    TransactionType: str
    TransID: str
    TransTime: str
    TransAmount: float
    BusinessShortCode: int
    BillRefNumber: str | None
    InvoiceNumber: str | None
    OrgAccountBalance: str | None
    ThirdPartyTransID: str | None
    MSISDN: int | str
    FirstName: str | None
    MiddleName: str | None
    LastName: str | None
    model_config: Incomplete

class C2BValidationResponse(BaseModel):
    ResultCode: int | str
    ResultDesc: str
    ThirdPartyTransID: str | None
    model_config: Incomplete
    @classmethod
    def validate(cls, values): ...

class C2BConfirmationResponse(BaseModel):
    ResultCode: int | str
    ResultDesc: str
    model_config: Incomplete
