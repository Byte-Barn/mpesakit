from _typeshed import Incomplete
from pydantic import BaseModel, EmailStr as EmailStr, HttpUrl as HttpUrl
from typing import Any

class BillManagerOptInRequest(BaseModel):
    shortcode: int
    email: EmailStr
    officialContact: str
    sendReminders: int
    logo: str | None
    callbackurl: HttpUrl
    model_config: Incomplete

class BillManagerOptInResponse(BaseModel):
    app_key: str
    resmsg: str
    rescode: str
    model_config: Incomplete
    def is_successful(self) -> bool: ...

class BillManagerUpdateOptInRequest(BaseModel):
    shortcode: int
    email: EmailStr
    officialContact: str
    sendReminders: int
    logo: str | None
    callbackurl: str | None
    model_config: Incomplete

class BillManagerUpdateOptInResponse(BaseModel):
    resmsg: str
    rescode: str
    model_config: Incomplete
    def is_successful(self) -> bool: ...

class InvoiceItem(BaseModel):
    itemName: str
    amount: int
    model_config: Incomplete

class BillManagerSingleInvoiceRequest(BaseModel):
    externalReference: str
    billedFullName: str
    billedPhoneNumber: str
    billedPeriod: str
    invoiceName: str
    dueDate: str
    accountReference: str
    amount: int
    invoiceItems: list[InvoiceItem] | None
    model_config: Incomplete
    @classmethod
    def validate(cls, values): ...

class BillManagerSingleInvoiceResponse(BaseModel):
    Status_Message: str
    resmsg: str
    rescode: str
    model_config: Incomplete
    def is_successful(self) -> bool: ...

class BillManagerBulkInvoiceRequest(BaseModel):
    invoices: list[BillManagerSingleInvoiceRequest]
    model_config: Incomplete

class BillManagerBulkInvoiceResponse(BaseModel):
    Status_Message: str
    resmsg: str
    rescode: str
    model_config: Incomplete
    def is_successful(self) -> bool: ...

class BillManagerCancelSingleInvoiceRequest(BaseModel):
    externalReference: str
    model_config: Incomplete

class BillManagerCancelBulkInvoiceRequest(BaseModel):
    invoices: list[BillManagerCancelSingleInvoiceRequest]
    model_config: Incomplete

class BillManagerCancelInvoiceResponse(BaseModel):
    Status_Message: str
    resmsg: str
    rescode: str
    errors: list[Any] | None
    model_config: Incomplete
    def is_successful(self) -> bool: ...

class BillManagerPaymentNotificationRequest(BaseModel):
    transactionId: str
    paidAmount: int
    msisdn: str
    dateCreated: str
    accountReference: str
    shortCode: int
    model_config: Incomplete

class BillManagerPaymentNotificationResponse(BaseModel):
    resmsg: str
    rescode: str
    model_config: Incomplete

class BillManagerPaymentAcknowledgmentRequest(BaseModel):
    paymentDate: str
    paidAmount: int
    accountReference: str
    transactionId: str
    phoneNumber: str
    fullName: str
    invoiceName: str
    externalReference: str
    model_config: Incomplete

class BillManagerPaymentAcknowledgmentResponse(BaseModel):
    resmsg: str
    rescode: str
    model_config: Incomplete
