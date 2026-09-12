"""Facade for M-Pesa B2Pochi (Business to Pochi La Biashara) APIs."""

from mpesakit.auth import TokenManager
from mpesakit.http_client import HttpClient
from mpesakit.b2p import B2P, B2PRequest, B2PResponse, B2PCommandIDType


class B2PService:
    """Facade for M-Pesa B2Pochi APIs."""

    def __init__(self, http_client: HttpClient, token_manager: TokenManager) -> None:
        """Initialize the B2Pochi service facade."""
        self.http_client = http_client
        self.token_manager = token_manager
        self.b2p = B2P(http_client=self.http_client, token_manager=self.token_manager)

    def send_payment(
        self,
        originator_conversation_id: str,
        initiator_name: str,
        security_credential: str,
        amount: int,
        party_a: str,
        party_b: str,
        remarks: str,
        queue_timeout_url: str,
        result_url: str,
        command_id: B2PCommandIDType = B2PCommandIDType.BusinessPayToPochi,
        occasion: str | None = None,
        **kwargs,
    ) -> B2PResponse:
        """Initiate a B2Pochi payment request.

        Args:
            originator_conversation_id: Unique ID for the transaction.
            initiator_name: The name of the initiator.
            security_credential: The encrypted security credential.
            amount: The amount to be sent.
            party_a: The business short code.
            party_b: The recipient's phone number.
            remarks: Remarks for the transaction (2–100 characters).
            queue_timeout_url: URL for timeout notifications.
            result_url: URL for result notifications.
            command_id: The command ID for the transaction.
            occasion: Optional occasion for the transaction.
            kwargs: Additional fields for B2PRequest.

        Returns:
            B2PResponse: Response from M-Pesa API.
        """
        command_id_value = (
            command_id.value if isinstance(command_id, B2PCommandIDType) else command_id
        )
        request = B2PRequest(
            OriginatorConversationID=originator_conversation_id,
            InitiatorName=initiator_name,
            SecurityCredential=security_credential,
            CommandID=command_id_value,
            Amount=amount,
            PartyA=party_a,
            PartyB=party_b,
            Remarks=remarks,
            QueueTimeOutURL=queue_timeout_url,
            ResultURL=result_url,
            Occassion=occasion,
            **{k: v for k, v in kwargs.items() if k in B2PRequest.model_fields},
        )
        return self.b2p.send_payment(request)
