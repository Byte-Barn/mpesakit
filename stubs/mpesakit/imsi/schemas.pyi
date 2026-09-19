from typing import Optional
from pydantic import BaseModel

class IMSIRequest(BaseModel):
    customerNumber: str
    @classmethod
    def validate_customer_number(cls,v: str) ->str:...

class StatusMixin:
    @property
    def is_successful(self) -> bool:...

class SwapCheckMixin:
    lastSwapDate: Optional[str] = None
    @property
    def is_recently_swapped(self) -> bool:...

class NetworkAgeMixin:
    msisdnRegistrationDate: Optional[str] = None
    @property
    def is_registration_default(self) -> bool:...

class BaseIMSISelfResponse(BaseModel,StatusMixin):
    requestRefID: str
    responseCode: str
    responseDesc: str
    customerNumber: Optional[str]

class IMSIV1Response(BaseIMSISelfResponse,SwapCheckMixin,NetworkAgeMixin):
    imsi: str
    lastSwapDate: str
    msisdnRegistrationDate: str

class IMSIV2Response(BaseIMSISelfResponse,NetworkAgeMixin):
    msisdnRegistrationDate: str

class IMSIV3Response(BaseIMSISelfResponse):
    imsi: str


