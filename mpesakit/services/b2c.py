"""Facade for M-Pesa B2C APIs (Business to Customer, Account TopUp)."""

from mpesakit.auth import TokenManager, AsyncTokenManager
from mpesakit.http_client import HttpClient, AsyncHttpClient
from mpesakit.b2c import B2C, AsyncB2C, B2CRequest, B2CResponse, B2CCommandIDType
from mpesakit.b2c_account_top_up import (
    B2CAccountTopUp,
    AsyncB2CAccountTopUp,
    B2CAccountTopUpRequest,
    B2CAccountTopUpResponse,
)


class B2CService:
    """Facade for all M-Pesa B2C APIs."""

    def __init__(self, http_client: HttpClient, token_manager: TokenManager) -> None:
        """Initialize the B2C service facade."""
        self.http_client = http_client
        self.token_manager = token_manager
        self.b2c = B2C(http_client=self.http_client, token_manager=self.token_manager)
        self._account_topup = B2CAccountTopUp(
            http_client=self.http_client, token_manager=self.token_manager
        )

    def send_payment(
        self,
        originator_conversation_id: str,
        initiator_name: str,
        security_credential: str,
        command_id: B2CCommandIDType,
        amount: int,
        party_a: str,
        party_b: str,
        remarks: str,
        queue_timeout_url: str,
        result_url: str,
        occasion: str = "",
        **kwargs,
    ) -> B2CResponse:
        """Initiate a B2C payment request.

        Args:
            originator_conversation_id: Unique ID for the transaction.
            initiator_name: The name of the initiator.
            security_credential: The encrypted security credential.
            command_id: The command ID for the transaction.
            amount: The amount to be sent.
            party_a: The business short code.
            party_b: The recipient's phone number.
            remarks: Remarks for the transaction.
            queue_timeout_url: URL for timeout notifications.
            result_url: URL for result notifications.
            occasion: Occasion for the transaction.
            kwargs: Additional fields for B2CRequest.

        Returns:
            B2CResponse: Response from M-Pesa API.
        """
        request = B2CRequest(
            OriginatorConversationID=originator_conversation_id,
            InitiatorName=initiator_name,
            SecurityCredential=security_credential,
            CommandID=command_id.value,
            Amount=amount,
            PartyA=party_a,
            PartyB=party_b,
            Remarks=remarks,
            QueueTimeOutURL=queue_timeout_url,
            ResultURL=result_url,
            Occasion=occasion,
            **{k: v for k, v in kwargs.items() if k in B2CRequest.model_fields},
        )
        return self.b2c.send_payment(request)

    def account_topup(
        self,
        initiator: str,
        security_credential: str,
        amount: int,
        party_a: str,
        party_b: str,
        account_reference: str,
        requester: str,
        remarks: str,
        queue_timeout_url: str,
        result_url: str,
        **kwargs,
    ) -> B2CAccountTopUpResponse:
        """Initiate a B2C Account TopUp transaction.

        Args:
            initiator: The name of the initiator.
            security_credential: The encrypted security credential.
            amount: The amount to be topped up.
            party_a: The party initiating the transaction.
            party_b: The party receiving the transaction.
            account_reference: Reference for the transaction.
            requester: Optional requester name.
            remarks: Remarks for the transaction.
            queue_timeout_url: URL for timeout notifications.
            result_url: URL for result notifications.
            kwargs: Additional fields for B2CAccountTopUpRequest.

        Returns:
            B2CAccountTopUpResponse: Response from M-Pesa API.
        """
        request = B2CAccountTopUpRequest(
            Initiator=initiator,
            SecurityCredential=security_credential,
            Amount=amount,
            PartyA=party_a,
            PartyB=party_b,
            AccountReference=account_reference,
            Requester=requester,
            Remarks=remarks,
            QueueTimeOutURL=queue_timeout_url,
            ResultURL=result_url,
            **{
                k: v
                for k, v in kwargs.items()
                if k in B2CAccountTopUpRequest.model_fields
            },
        )
        return self._account_topup.topup(request)


class AsyncB2CService:
    """Async facade for all M-Pesa B2C APIs."""

    def __init__(
        self, http_client: AsyncHttpClient, token_manager: AsyncTokenManager
    ) -> None:
        """Initialize the async B2C service facade."""
        self.http_client = http_client
        self.token_manager = token_manager
        self.b2c = AsyncB2C(
            http_client=self.http_client, token_manager=self.token_manager
        )
        self._account_topup = AsyncB2CAccountTopUp(
            http_client=self.http_client, token_manager=self.token_manager
        )

    async def send_payment(
        self,
        originator_conversation_id: str,
        initiator_name: str,
        security_credential: str,
        command_id: B2CCommandIDType,
        amount: int,
        party_a: str,
        party_b: str,
        remarks: str,
        queue_timeout_url: str,
        result_url: str,
        occasion: str = "",
        **kwargs,
    ) -> B2CResponse:
        """Initiate a B2C payment request asynchronously."""
        request = B2CRequest(
            OriginatorConversationID=originator_conversation_id,
            InitiatorName=initiator_name,
            SecurityCredential=security_credential,
            CommandID=command_id.value,
            Amount=amount,
            PartyA=party_a,
            PartyB=party_b,
            Remarks=remarks,
            QueueTimeOutURL=queue_timeout_url,
            ResultURL=result_url,
            Occasion=occasion,
            **{k: v for k, v in kwargs.items() if k in B2CRequest.model_fields},
        )
        return await self.b2c.send_payment(request)

    async def account_topup(
        self,
        initiator: str,
        security_credential: str,
        amount: int,
        party_a: str,
        party_b: str,
        account_reference: str,
        requester: str,
        remarks: str,
        queue_timeout_url: str,
        result_url: str,
        **kwargs,
    ) -> B2CAccountTopUpResponse:
        """Initiate a B2C Account TopUp transaction asynchronously."""
        request = B2CAccountTopUpRequest(
            Initiator=initiator,
            SecurityCredential=security_credential,
            Amount=amount,
            PartyA=party_a,
            PartyB=party_b,
            AccountReference=account_reference,
            Requester=requester,
            Remarks=remarks,
            QueueTimeOutURL=queue_timeout_url,
            ResultURL=result_url,
            **{
                k: v
                for k, v in kwargs.items()
                if k in B2CAccountTopUpRequest.model_fields
            },
        )
        return await self._account_topup.topup(request)
