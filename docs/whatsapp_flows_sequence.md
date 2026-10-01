# WhatsApp Flows Message Sequence

WhatsApp Flows use two separate communication channels:

1. The normal WhatsApp Messages API launches the Flow.
2. The encrypted Flow data endpoint handles actions within the Flow.
3. The normal messages webhook reports terminal Flow completion.

## Sequence

```text
Application server              Meta / WhatsApp                    User
       │                               │                              │
       │  1. Interactive Flow message  │                              │
       │ ─────────────────────────────>│  Display Flow button         │
       │                               │─────────────────────────────>│
       │                               │                              │
       │                               │  Open Flow                   │
       │                               │<─────────────────────────────│
       │                               │                              │
       │  2. Encrypted INIT request    │                              │
       │<──────────────────────────────│                              │
       │                               │                              │
       │  3. Encrypted screen response │                              │
       │──────────────────────────────>│  Render first screen         │
       │                               │─────────────────────────────>│
       │                               │                              │
       │                               │  Submit current screen       │
       │                               │<─────────────────────────────│
       │                               │                              │
       │  4. Encrypted data_exchange   │                              │
       │<──────────────────────────────│                              │
       │                               │                              │
       │  5. Encrypted next screen     │                              │
       │──────────────────────────────>│  Render next screen          │
       │                               │─────────────────────────────>│
       │                               │                              │
       │                               │  Complete Flow               │
       │                               │<─────────────────────────────│
       │                               │                              │
       │  6. messages webhook:         │                              │
       │     interactive/nfm_reply     │                              │
       │<──────────────────────────────│                              │
```

A `data_exchange` and an `nfm_reply` can both occur near the end of one Flow,
but they represent different user actions:

1. The user submits a screen whose action is `data_exchange`.
2. The endpoint response tells Meta to render the next screen, which may be a
  terminal result screen.
3. The user presses a control whose action is `complete` on that terminal
  screen.
4. Meta sends the application an `interactive` message containing `nfm_reply`.

The `complete` action does not produce another data-endpoint request. Conversely,
if a screen completes the Flow directly without first using `data_exchange`, the
application receives the `nfm_reply` without a preceding exchange for that
button press.

## 1. Launch the Flow

The case handler creates an internal `ServerFlowMsg`:

```python
ServerFlowMsg(
    flow_id    = "123456",
    flow_token = "secret-token",
    flow_cta   = "Open",
    body       = "Start here",
    screen     = "START",
)
```

`write_payload()` converts it into a regular WhatsApp interactive message:

```json
{
  "type": "interactive",
  "interactive": {
    "type": "flow",
    "action": {
      "name": "flow",
      "parameters": {
        "flow_id": "123456",
        "flow_token": "secret-token",
        "flow_action": "navigate",
        "flow_action_payload": {
          "screen": "START"
        }
      }
    }
  }
}
```

The Flow token correlates later data-endpoint requests and the completion webhook
with the persisted Flow session. Stored message audits redact the token.

## 2. Receive an Encrypted Endpoint Request

When the user opens or advances the Flow, Meta posts an encrypted envelope:

```json
{
  "encrypted_flow_data": "<base64 ciphertext>",
  "encrypted_aes_key": "<base64 RSA-encrypted key>",
  "initial_vector": "<base64 IV>"
}
```

`WhatsApp_IB_Encrypted_FlowRequest` validates this outer transport object.
`decrypt_flow_request()` decrypts and validates its contents as a
`WhatsApp_IB_Decrypted_FlowRequest`:

```python
WhatsApp_IB_Decrypted_FlowRequest(
    version    = "3.0",
    action     = "data_exchange",
    flow_token = "secret-token",
    screen     = "CREDIT_OTP",
    data       = {
        "verification_code" : "123456",
    },
)
```

This model contains the client input for one endpoint call. Its `screen` is the
screen that initiated that call, and its `data` is the payload submitted by that
screen.

## 3. Build Handler Context and Produce a Response

After decrypting the request, `WhatsAppFlowEndpoint` uses its `flow_token` to load
the persisted session. It then constructs a `WhatsApp_Local_FlowContext` and
passes both objects to the application handler:

```text
WhatsApp_Local_FlowContext
        +
WhatsApp_IB_Decrypted_FlowRequest
        │
        ▼
CaseHandler.handle_flow_request(...)
        │
        ▼
WhatsApp_Local_FlowHandlerResult
```

### `WhatsAppFlowEndpoint` and `WhatsAppCaseHandler`

These classes sit on opposite sides of the application boundary:

```text
Meta / WhatsApp             WhatsAppFlowEndpoint             WhatsAppCaseHandler
       │                              │                               │
       │  Encrypted request           │                               │
       │─────────────────────────────>│                               │
       │                              │                               │
       │                              │  Load session and construct   │
       │                              │  WhatsApp_Local_FlowContext   │
       │                              │                               │
       │                              │  handle_flow_request(         │
       │                              │    context,                   │
       │                              │    request,                   │
       │                              │  )                            │
       │                              │──────────────────────────────>│
       │                              │                               │
       │                              │  Application business logic   │
       │                              │                               │
       │                              │  WhatsApp_Local_              │
       │                              │  FlowHandlerResult            │
       │                              │<──────────────────────────────│
       │                              │                               │
       │                              │  Persist private state and    │
       │                              │  encrypt OB_FlowResponse      │
       │                              │                               │
       │  Encrypted response          │                               │
       │<─────────────────────────────│                               │
```

For one data exchange, Meta calls the route owned by `WhatsAppAPIWorker`, which
delegates synchronously to `WhatsAppFlowEndpoint` without using the message queue.
`WhatsAppCaseHandler` never decrypts or encrypts transport payloads and never
responds to Meta directly. It receives already validated application models and
returns business-level response and state instructions to the endpoint.

```text
Meta / WhatsApp
      │
      │ encrypted HTTP request
      │ ( WhatsApp_IB_Encrypted_FlowRequest )
      │
      ▼
WhatsAppFlowEndpoint
  1. Decrypt and validate the request
  2. Load and validate the persisted Flow session
  3. Find the handler class registered by `handler_key`
  4. Build WhatsApp_Local_FlowContext
      │
      │ context + decrypted request
      │ ( WhatsApp_Local_FlowContext + WhatsApp_IB_Decrypted_FlowRequest )
      │
      ▼
WhatsAppCaseHandler.handle_flow_request()
  1. Apply application-specific business logic
  2. Select the next screen and its data
  3. Return the next private session state
      │
      │ WhatsApp_Local_FlowHandlerResult
      │ ( containing non-encrypted WhatsApp_OB_FlowResponse )
      │
      ▼
WhatsAppFlowEndpoint
  1. Persist the exchange and updated session state
  2. Encrypt the logical response
  3. Return the encrypted HTTP body to Meta
      │
      │ Base64-encoded AES-GCM ciphertext
      │ ( serialized WhatsApp_OB_FlowResponse )
      │
      ▼
Meta / WhatsApp
```

`WhatsAppFlowEndpoint` owns transport and infrastructure concerns. It knows how
to decrypt Meta's envelope, enforce session expiry, replay cached responses for
retries, persist exchanges, and encrypt the response. It does not know what a
TIA identification or verification code means.

`WhatsAppCaseHandler` and `AsyncWhatsAppCaseHandler` are the application-facing
base classes. A registered subclass implements `handle_flow_request()` to apply
customer-specific logic. For example, the TIA handler decides whether to show
the Club MÁS result screen or the Creditía OTP screen.

The endpoint invokes `handle_flow_request()` on the registered handler class; it
does not construct a contact-bound handler instance and does not call
`process_message()`. This is why the hook is a class method and receives an
explicit `WhatsApp_Local_FlowContext` rather than relying on instance fields.

The same handler subclass can therefore participate in both paths:

- A handler instance processes ordinary messages and sends the initial
`ServerFlowMsg` through the WhatsApp Messages API.
- The handler class processes encrypted Flow exchanges routed by
`WhatsAppFlowEndpoint`.



### Context Versus Request

The distinction is ownership and lifetime:


| Model                               | Owner              | Lifetime            | Purpose                                                      |
| ----------------------------------- | ------------------ | ------------------- | ------------------------------------------------------------ |
| `WhatsApp_Local_FlowContext`        | Application server | Entire Flow session | Routing identity and private state carried between exchanges |
| `WhatsApp_IB_Decrypted_FlowRequest` | Meta Flow client   | One endpoint call   | Current action, originating screen, and current user input   |


The interpretation that context applies across screens is therefore broadly
correct, with one qualification: context is not automatically visible to every
screen. It is private server-side information. A screen sees a value only when
the handler deliberately copies it into `WhatsApp_OB_FlowResponse.data`.

`WhatsApp_Local_FlowContext` contains two kinds of information:

- Stable session identity: WABA, Flow, business, contact, and session IDs.
- Mutable `state`: application data saved after one exchange and recovered for
the next exchange. This state is encrypted at rest.

`WhatsApp_IB_Decrypted_FlowRequest` contains only the current client event:

- `action`: `INIT`, `BACK`, `data_exchange`, or `ping`.
- `screen`: the screen that initiated this request.
- `data`: values submitted by that screen.
- `flow_token`: the opaque value used to find the server-side session.

For example, the Creditía sequence can avoid sending the identification back to
the client with the OTP screen:

```text
Exchange 1
  Context.state:  { "service": "credit" }
  Request.screen: "CREDIT_IDENTIFICATION"
  Request.data:   { "identification": "0123456789" }

  HandlerResult.state:
    { "service": "credit", "identification": "0123456789" }
  HandlerResult.response.screen: "CREDIT_OTP"

Exchange 2
  Context.state:
    { "service": "credit", "identification": "0123456789" }
  Request.screen: "CREDIT_OTP"
  Request.data:   { "verification_code": "123456" }
```

During the second exchange, the handler combines the identification from its
private persisted context with the verification code from the current request.

The handler returns a `WhatsApp_Local_FlowHandlerResult` containing:

- `response`: the logical outbound screen and screen data.
- `state`: the private state to encrypt and retain for the next exchange.
- `session_status`: the new server-side session status.

Its response might be:

```python
WhatsApp_Local_FlowHandlerResult(
    response = WhatsApp_OB_FlowResponse(
        screen = "CREDIT_RESULT",
        data   = {
            "result_text" : "Your card is active.",
        },
    ),
    state = {
        "service" : "credit",
    },
)
```

`encrypt_flow_response()` encrypts the logical `WhatsApp_OB_FlowResponse` with
the request's AES key and the bitwise-inverted request IV. The HTTP body is the
resulting Base64 ciphertext.

## 4. Repeat Data Exchanges

Each screen submission produces a new encrypted request and response. The
request is transient, while the context is reconstructed from the persisted
session before each handler call.

The data-endpoint cycle is therefore:

```text
Encrypted envelope
  → WhatsApp_IB_Encrypted_FlowRequest
  → decrypt_flow_request()
  → WhatsApp_IB_Decrypted_FlowRequest
  → load WhatsApp_Local_FlowContext
  → application handler
  → WhatsApp_Local_FlowHandlerResult
  → WhatsApp_OB_FlowResponse
  → encrypt_flow_response()
  → Base64 HTTP response
```



## 5. Receive the Completion Webhook

Completion does not arrive through the Flow data endpoint. It arrives through
the ordinary messages webhook:

```text
WhatsApp_IB_Payload
└── WhatsApp_IB_PayloadItem
    └── WhatsApp_IB_Change
        └── WhatsApp_IB_Value
            └── WhatsApp_IB_Message
                └── WhatsApp_IB_InteractiveReply
                    └── WhatsApp_IB_FlowReply
```

The relevant raw fragment is:

```json
{
  "type": "interactive",
  "interactive": {
    "type": "nfm_reply",
    "nfm_reply": {
      "name": "flow",
      "body": "Sent",
      "response_json": "{\"flow_token\":\"opaque\",\"service\":\"credit\",\"outcome\":\"success\"}"
    }
  }
}
```

`response_json` is itself a JSON-encoded string. The convenience property:

```python
message.interactive.nfm_reply.response
```

returns:

```python
{
    "flow_token" : "opaque",
    "service"    : "credit",
    "outcome"    : "success",
}
```



## How the Focused Tests Map to the Sequence

The tests in `.tests/test_flows.py` cover three independent slices rather than
one end-to-end exchange:

- `test_flow_launch_payload_and_persisted_token_redaction()` covers step 1.
- `test_flow_endpoint_encryption_round_trip()` covers steps 2 and 3.
- `test_nfm_reply_parses_safe_completion_payload()` covers step 5.
