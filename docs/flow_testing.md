# WhatsApp Flow Testing and Recovery

This document records the practical lessons from bringing an endpoint-backed
WhatsApp Flow from local code to a live, published Flow. For the request and
response sequence itself, see
[`whatsapp_flows_sequence.md`](whatsapp_flows_sequence.md).

## Persistent Configuration and Test State

Flow testing uses two categories of database records.

Persistent configuration:

- `wa_api_businesses`: configured WhatsApp businesses and phone numbers.
- `wa_case_handler_routes`: business-wide and contact-specific handler routing.
- `wa_api_flow_wabas`: the public key and encrypted private key used by a WABA.
- `wa_api_flows`: Meta Flow IDs, handler keys, versions, endpoint URIs, and status.

Contact test state:

- contacts and contact profiles;
- inbound and outbound messages, statuses, media metadata, and queue rows;
- case-handler manifests, messages, state histories, contexts, and leases;
- Flow sessions and their session-bound data exchanges.

Routine testing should reset only the contact test state. Do not use
`.scripts/danger_zone/clean_slate.sql` for this: that script intentionally
truncates both categories, including the WABA key pair and Flow definitions.
Use [`reset_contact.sql`](../.scripts/danger_zone/reset_contact.sql) instead.

Deleting one `wa_api_contacts` row is sufficient because the dependent records
use `ON DELETE CASCADE`. It preserves the business, its business-wide route,
WABA keys, Flow definitions, WABA-level health exchanges, and Flow webhook
events. A contact-specific route necessarily disappears with its contact.

The reset affects database state only. It does not delete messages from the
user's WhatsApp client or media bytes from external object storage.

## First-Time Setup Order

Use this order for a new endpoint-backed Flow:

1. Deploy the application and expose the Flow endpoint through the reverse
   proxy.
2. Provision the WABA key pair and phone number:

   ```bash
   python3 -m wa_agents.flows \
     --waba-id <WABA_ID> \
     --phone-number-id <PHONE_NUMBER_ID>
   ```

   Provisioning generates a key pair, stores the private key encrypted in
   `wa_api_flow_wabas`, uploads the public key to Meta, and verifies its
   signature status.
3. Create the Flow in Flow Builder and load the Flow JSON.
4. Configure the endpoint URI as:

   ```text
   https://<WHATSAPP_HOST>/<WEBHOOK_PATH>/flows/<WABA_ID>
   ```

5. Run Flow Builder's endpoint health check.
6. Publish the Flow and record the Meta Flow ID.
7. Register the published Flow in `wa_api_flows`, including its stable
   application `flow_key`, `handler_key`, versions, endpoint URI, and
   `PUBLISHED` status.
8. Confirm that the business or contact route selects the registered handler.
9. Send a normal message to exercise Flow launch through the case handler.

Provisioning and Flow registration are separate operations. A successful
health check proves that Meta and the endpoint share a working key pair; it does
not register the Flow for application launch. Likewise, a
`FLOW_STATUS_CHANGE` webhook is currently an audit event and does not create or
update `wa_api_flows` automatically.

## Flow Builder Validation Lessons

### Text inputs do not have a `sensitive` property

`TextInput` accepts supported properties such as `input-type`. Use
`"input-type": "password"` or `"passcode"` when the client should mask an
entry. The data exchanged with the endpoint is encrypted independently of the
input's display type.

### The routing model contains forward edges only

Declare the screens that may follow each screen. Do not add reverse edges for
the client's Back behavior. Returning the same screen with an error after a
`data_exchange` is also an endpoint response and does not require a self-edge.

For example:

```text
SERVICE_SELECTION
├── REWARDS_IDENTIFICATION → REWARDS_RESULT
└── CREDIT_IDENTIFICATION → CREDIT_OTP → CREDIT_RESULT
```

## Diagnosing Endpoint Failures by HTTP Response

The response format identifies which layer rejected the request:

- nginx HTML `403`: The request never reached FastAPI. Check the host nginx
  `location` rules and the installed/enabled configuration.
- nginx HTML `502`: nginx matched the route but could not reach the backend.
  Check container status, the published port, and the nginx upstream.
- Plain-text `404 Unknown WABA`: nginx and FastAPI routing work, but the WABA
  provisioning row is absent. Check `wa_api_flow_wabas` and reprovision it.
- Plain-text decryption error: A WABA row exists, but Meta's public key and the
  stored private key do not match, or the envelope is invalid. Reprovision the
  key pair and retry the health check.
- Encrypted HTTP `200`: The endpoint decrypted and processed the request.
  Confirm the recorded result in `wa_api_flow_data_exchanges`.

A wrong or missing FastAPI route normally produces FastAPI's JSON `404`; it
does not produce nginx's HTML `403`.

## Reverse-Proxy Deployment Lesson

An exact nginx location such as `location = /whatsapp` accepts only the normal
webhook path. It does not match `/whatsapp/flows/<WABA_ID>`. The Flow endpoint
needs its own narrowly scoped proxy rule.

The checked-in nginx file is not necessarily the active file. Verify the file
installed under `/etc/nginx/sites-available`, its symlink under
`/etc/nginx/sites-enabled`, and the output of `nginx -t`.

When a deployment changes the deployment script itself, pull that change before
starting the script or run the deployment again. A process that began from the
old script cannot be relied upon to execute newly added deployment steps.

## Why a Published Flow Can Still Fall Back to Buttons

The case handler launches a Flow only when `get_active_flow()` finds an active
definition for the WABA and application `flow_key`. A Flow can exist and be
published in Meta while that database row is absent. In that case, Flow launch
returns false and the chatbot deliberately uses its existing button-message
fallback.

Confirm all of the following:

- the WABA ID matches the receiving business;
- the Flow ID is the published Meta ID;
- the application `flow_key` matches the handler constant;
- `handler_key` resolves to the intended case handler;
- `flow_status` is `PUBLISHED`;
- `is_active` is true.

## Recovery After a Full Clean Slate

`clean_slate.sql` removes the WABA's private key and Flow definitions. Seeding
businesses and handler routes does not recreate either one. Recovery is:

1. Seed the WhatsApp businesses and business-wide routes.
2. Re-run WABA provisioning to generate, store, upload, and verify a new key
   pair.
3. Re-register every published Flow definition.
4. Run Flow Builder's health check again.
5. Confirm a `ping` exchange completed with HTTP 200 and no error.
6. Send a normal message and confirm that the case handler launches the Flow
   rather than its fallback.

The public-key indicator in Flow Builder may remain green after the database
private key has been deleted because Meta still has the old public key. Only a
successful health check proves that Meta's current public key matches the
private key available to the endpoint.

## Flow Lifecycle Webhooks

Webhook changes with `field: "flows"` are accepted as
`WhatsApp_IB_FlowEvent`. Known examples include `FLOW_STATUS_CHANGE` and
`ENDPOINT_AVAILABILITY`. Event-specific fields are preserved and the event is
stored in `wa_api_flow_events`.

These events are operational audit data. They currently do not provision keys,
register definitions, update Flow status, or repair endpoint availability.
