# mpesakit

> ⚡ Effortless M-Pesa integration using Safaricom's Daraja API — built for developers, by developers.

[![Python 3.12+](https://img.shields.io/badge/python-3.12+-blue.svg)](https://www.python.org/downloads/)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-green.svg)](https://opensource.org/licenses/Apache-2.0)
[![PyPI version](https://img.shields.io/pypi/v/mpesakit.svg)](https://pypi.org/project/mpesakit)
[![Downloads](https://pepy.tech/badge/mpesakit)](https://pepy.tech/project/mpesakit)
[![Build Status](https://img.shields.io/badge/build-passing-brightgreen.svg)](https://github.com/Byte-Barn/mpesakit/actions)

---

Integrating Safaricom's Daraja API from scratch means wrestling with OAuth2 token rotation, security credential encryption, inconsistent sandbox vs. production endpoints, and documentation that rarely connects end-to-end.

**`mpesakit`** handles all of that. Add your credentials, call a method, move on. Every service is available as a **sync** client and a **fully async** client, so it fits whether you're scripting a one-off payment or wiring STK Push into a FastAPI backend.

---

## Installation

```bash
pip install mpesakit
```

---

## Quick Start

### 1. Set your credentials

```bash
export MPESA_CONSUMER_KEY="your_consumer_key"
export MPESA_CONSUMER_SECRET="your_consumer_secret"
export MPESA_SHORTCODE="your_shortcode"
export MPESA_PASSKEY="your_lipa_na_mpesa_passkey"
export MPESA_PHONE_NUMBER="254712345678"
```

### 2. Trigger an STK Push

```python
import os
from dotenv import load_dotenv
from mpesakit import MpesaClient
from mpesakit.mpesa_express import TransactionType

load_dotenv()

client = MpesaClient(
    consumer_key=os.getenv("MPESA_CONSUMER_KEY"),
    consumer_secret=os.getenv("MPESA_CONSUMER_SECRET"),
    environment="sandbox",  # Switch to "production" when ready
)

response = client.stk_push(
    business_short_code=int(os.getenv("MPESA_SHORTCODE")),
    passkey=os.getenv("MPESA_PASSKEY"),
    transaction_type=TransactionType.CUSTOMER_PAYBILL_ONLINE,
    amount=250,
    party_a=os.getenv("MPESA_PHONE_NUMBER"),
    party_b=os.getenv("MPESA_SHORTCODE"),
    phone_number=os.getenv("MPESA_PHONE_NUMBER"),
    callback_url="https://yourdomain.com/mpesa/callback",
    account_reference="Order-001",
    transaction_desc="Payment for order",
)

if response.is_successful:
    print("Request accepted:", response.CheckoutRequestID)
else:
    print("Error:", response.error_message())
```

Prefer async? Swap in `AsyncMpesaClient` and `await` the call — see [Async Support](#async-support) below.

### 3. Handle the payment callback

The client exposes `process_*` methods that validate and deserialize incoming Safaricom payloads into typed Pydantic objects — no manual dict parsing required.

```python
# Example: FastAPI callback handler
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

app = FastAPI()

@app.post("/mpesa/callback")
async def mpesa_callback(request: Request):
    payload = await request.json()

    # Validates the payload and returns a typed StkPushSimulateCallback object
    callback = client.process_stk_callback(payload)

    if callback.is_successful:
        metadata = callback.Body.stkCallback.CallbackMetadata
        print(f"Payment confirmed — Receipt: {metadata}")
    else:
        print(f"Payment failed: {callback.Body.stkCallback.ResultDesc}")

    return JSONResponse({"ResultCode": 0, "ResultDesc": "Accepted"})
```

All `process_*` methods follow the same pattern — pass in the raw JSON payload, get back a validated object. They're plain validation with no network I/O, so they're identical on `MpesaClient` and `AsyncMpesaClient` and are never `await`ed, even inside an async webhook handler.

| Method | Returns |
|--------|---------|
| `client.process_stk_callback(payload)` | `StkPushSimulateCallback` |
| `client.process_stk_query_callback(payload)` | `StkPushQueryResponse` |
| `client.process_b2c_callback(payload)` | `B2CResultCallback` |
| `client.process_account_balance_callback(payload)` | `AccountBalanceResultCallback` |
| `client.process_account_balance_timeout(payload)` | `AccountBalanceTimeoutCallback` |
| `client.process_transactions_callback(payload)` | `TransactionStatusResultCallback` |
| `client.process_reversal_callback(payload)` | `ReversalResultCallback` |
| `client.process_tax_remittance_callback(payload)` | `TaxRemittanceResultCallback` |
| `client.process_dynamic_qr_code_callback(payload)` | `DynamicQRGenerateResponse` |
| `client.process_b2b_callback(payload)` | `B2BExpressCheckoutCallback` |
| `client.process_bill_manager_callback(payload)` | `BillManagerPaymentNotificationRequest` |
| `client.process_ratiba_service_callback(payload)` | `StandingOrderCallback` |

### 4. Error handling

```python
from mpesakit.errors import MpesaApiException

try:
    response = client.stk_push(...)
except MpesaApiException as e:
    err = e.error
    print(f"Code: {err.error_code}")       # e.g. AUTH_INVALID_CREDENTIALS
    print(f"Message: {err.error_message}") # Human-readable description
    print(f"HTTP status: {err.status_code}")
    print(f"Request ID: {err.request_id}")
except Exception as exc:
    print(f"Unexpected error: {exc}")
```

The same `try/except` shape works whether you're calling `client.stk_push(...)` or `await client.stk_push(...)`.

---

## Async Support

Every client, service, and callback helper has a first-class async counterpart. Nothing about the API surface changes besides adding `Async` to the class name and `await` to the call site.

```python
import os
import asyncio
from dotenv import load_dotenv
from mpesakit import AsyncMpesaClient
from mpesakit.mpesa_express import TransactionType

load_dotenv()

async def main():
    # Use as an async context manager so the connection pool is closed for you
    async with AsyncMpesaClient(
        consumer_key=os.getenv("MPESA_CONSUMER_KEY"),
        consumer_secret=os.getenv("MPESA_CONSUMER_SECRET"),
        environment="sandbox",
    ) as client:
        response = await client.stk_push(
            business_short_code=int(os.getenv("MPESA_SHORTCODE")),
            passkey=os.getenv("MPESA_PASSKEY"),
            transaction_type=TransactionType.CUSTOMER_PAYBILL_ONLINE,
            amount=250,
            party_a=os.getenv("MPESA_PHONE_NUMBER"),
            party_b=os.getenv("MPESA_SHORTCODE"),
            phone_number=os.getenv("MPESA_PHONE_NUMBER"),
            callback_url="https://yourdomain.com/mpesa/callback",
            account_reference="Order-001",
            transaction_desc="Payment for order",
        )

        if response.is_successful:
            print("Request accepted:", response.CheckoutRequestID)
        else:
            print("Error:", response.error_message())

asyncio.run(main())
```

In a long-lived app (e.g. `AsyncMpesaClient` wired up as a FastAPI dependency), construct it once at startup and call `await client.aclose()` on shutdown instead of opening a new `async with` block per request:

```python
client = AsyncMpesaClient(
    consumer_key=os.getenv("MPESA_CONSUMER_KEY"),
    consumer_secret=os.getenv("MPESA_CONSUMER_SECRET"),
    environment="sandbox",
)

# ... use client.stk_push(...), client.b2c.send_payment(...), etc. across requests ...

# on shutdown
await client.aclose()
```

Because every call returns a coroutine, you can fan requests out concurrently with `asyncio.gather` instead of awaiting them one at a time — handy for payroll-style B2C runs, bulk QR generation, or reconciliation jobs that check many transaction statuses at once:

```python
responses = await asyncio.gather(*(
    client.transactions.query_status(
        initiator="api_user",
        security_credential="ENCRYPTED_CREDENTIAL",
        transaction_id=tid,
        party_a=int(os.getenv("MPESA_SHORTCODE")),
        identifier_type=4,  # short code
        result_url="https://yourdomain.com/mpesa/result",
        queue_timeout_url="https://yourdomain.com/mpesa/timeout",
        remarks="Nightly reconciliation",
    )
    for tid in pending_transaction_ids
))
```

Keep Safaricom's rate limits in mind — chunk large batches rather than firing hundreds of requests in one `gather`.

The direct/low-level API mirrors this same pattern: `Reversal` → `AsyncReversal`, `B2C` → `AsyncB2C`, `TokenManager` → `AsyncTokenManager`, `MpesaHttpClient` → `MpesaAsyncHttpClient`, and so on across every service.

---

## More Examples

### B2C — Send money to a customer

```python
from mpesakit.b2c import B2CCommandIDType

response = client.b2c.send_payment(
    originator_conversation_id="ocid-1234-5678",
    initiator_name="your_initiator_name",
    security_credential="your_encrypted_security_credential",
    command_id=B2CCommandIDType.BusinessPayment,
    amount=1500,
    party_a="600999",           # Your bulk disbursement shortcode
    party_b="254712345678",     # Recipient phone number (normalized by SDK)
    remarks="Refund for order 042",
    queue_timeout_url="https://yourdomain.com/mpesa/timeout",
    result_url="https://yourdomain.com/mpesa/result",
)

if response.is_successful:
    print("Payout sent:", response.ResponseDescription)
```

> **Note:** B2C in production requires a Bulk Disbursement Account from Safaricom — a standard PayBill or Till will not work. See the [B2C docs](https://mpesakit.dev/b2c) for details.

### STK Query — Check a push status

```python
response = client.stk_query(
    business_short_code=int(os.getenv("MPESA_SHORTCODE")),
    passkey=os.getenv("MPESA_PASSKEY"),
    checkout_request_id="ws_CO_191220191020363925",
)
```

### Switching to production

```python
client = MpesaClient(
    consumer_key="...",
    consumer_secret="...",
    environment="production",  # That's all it takes
)
```

---

## Supported APIs

Every API below ships with both a sync and an async client.

| API | Status | Description |
|-----|--------|-------------|
| **STK Push** | ✅ Ready | Prompt a customer to enter their M-Pesa PIN to pay |
| **STK Query** | ✅ Ready | Check the status of an STK Push request |
| **C2B Payments** | ✅ Ready | Receive payments from customers via paybill or till |
| **B2C Payments** | ✅ Ready | Send money to customers or staff |
| **B2C Account Top-up** | ✅ Ready | Top up B2C utility accounts |
| **Business Paybill** | ✅ Ready | Business-to-business paybill transfers |
| **Business BuyGoods** | ✅ Ready | Business-to-business till transfers |
| **Token Management** | ✅ Ready | Automatic OAuth2 token handling — no manual refresh needed |
| **Account Balance** | ✅ Ready | Query your M-Pesa account balance |
| **Transaction Status** | ✅ Ready | Look up the status of any past transaction |
| **Transaction Reversal** | ✅ Ready | Reverse erroneous transactions |
| **Dynamic QR** | ✅ Ready | Generate QR codes for M-Pesa payments |
| **Tax Remittance** | ✅ Ready | Submit tax remittances via M-Pesa |

---

## Security Best Practices

- **Never commit credentials** to version control — use environment variables or a secrets manager
- **Validate callbacks** using `is_mpesa_ip_allowed` to restrict requests to known Safaricom IP ranges
- **Use HTTPS** for all callback URLs — Safaricom will not deliver to plain HTTP in production
- **Log transaction IDs** (`OriginatorConversationID`, `ConversationID`) for reconciliation and dispute resolution
- **Persist callback payloads** before returning an acknowledgement, to protect against processing failures

---

## Full Documentation

API reference, webhook guides, and production checklist: **[mpesakit.dev](https://mpesakit.dev)**

---

## Contributing

```bash
git clone https://github.com/Byte-Barn/mpesakit.git
cd mpesakit

python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

pip install -e ".[dev]"
pytest tests/unit
```

Ways to contribute:
- **Report bugs** — [GitHub Issues](https://github.com/Byte-Barn/mpesakit/issues)
- **Suggest features** — [GitHub Discussions](https://github.com/Byte-Barn/mpesakit/discussions)
- **Submit a PR** — please include tests and update docs for any API changes
- **Join the community** — [Discord](https://discord.gg/hNxTew523E)

Please follow PEP 8 and include type hints in new code.

---

## Support

- 📖 Docs: [mpesakit.dev](https://mpesakit.dev)
- 🐛 Issues: [github.com/Byte-Barn/mpesakit/issues](https://github.com/Byte-Barn/mpesakit/issues)
- 💬 Discussions: [github.com/Byte-Barn/mpesakit/discussions](https://github.com/Byte-Barn/mpesakit/discussions)
- 📧 Email: johnmkagunda@gmail.com

---

## License

[Apache 2.0](LICENSE) — free for commercial and private use.

---

<div align="center">

**Made with ❤️ for the Kenyan developer community**

[⭐ Star this repo](https://github.com/Byte-Barn/mpesakit) · [🐛 Report a bug](https://github.com/Byte-Barn/mpesakit/issues) · [💡 Request a feature](https://github.com/Byte-Barn/mpesakit/issues/new)

*Built on the shoulders of [`Arlus/mpesa-py`](https://github.com/Arlus/mpesa-py)*

</div>