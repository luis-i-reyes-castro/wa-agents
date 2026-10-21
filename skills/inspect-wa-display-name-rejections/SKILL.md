---
name: inspect-wa-display-name-rejections
description: Inspect audited WhatsApp `phone_number_name_update` webhook payloads in the Supabase database selected by a consumer dotenv file, identify display-name decisions and rejection reasons, interpret known reason codes, and suggest evidence-backed replacement names. Use when a WhatsApp Business display name is rejected, pending, unexpectedly approved, or missing from application processing, especially for Sofia deployments backed by `wa_api_inbound_payloads`.
---

# Inspect WhatsApp Display Name Rejections

Keep the diagnosis read-only and distinguish Meta's documented values from codes observed in
production. Never print database URLs, access tokens, full raw payloads, WABA IDs, or phone
numbers.

## Establish Context

1. Read `../maintain-wa-agents/SKILL.md` for repository and payload-audit conventions.
2. For Sofia, read `~/sofia-server/skills/inspect-sofia-runtime/SKILL.md` and use its safe
   database query helper with `~/sofia-server/.env`.
3. Check `git status --short --branch` in the repositories being inspected. Do not modify
   application code or database state during diagnosis.
4. Read [references/rejection-reasons.md](references/rejection-reasons.md) before interpreting
   a code or recommending a replacement name.

## Inspect Stored Decisions

Run the bundled read-only query from the `wa-agents` repository root:

```bash
~/pe/bin/python \
  ~/sofia-server/skills/inspect-sofia-runtime/scripts/query_database.py \
  --env-file ~/sofia-server/.env \
  --max-rows 100 \
  --format json \
  < skills/inspect-wa-display-name-rejections/scripts/inspect_display_name_rejections.sql
```

The query returns only the payload audit ID, receipt time, requested display name, decision,
rejection reason, and validation state. It deliberately omits phone numbers, WABA IDs, hashes,
and complete payload/error bodies.

If the expected row is absent:

1. Confirm that the selected dotenv file contains the intended Supabase connection variable by
   key name only; never print its value.
2. Confirm that Meta sent a `phone_number_name_update` subscription event.
3. Inspect bounded, redacted webhook logs using the Sofia runtime skill.
4. Do not infer a rejection reason from `name_status = DECLINED`; the reason comes from the
   webhook payload.

## Interpret Results

- Treat `decision` as the outcome field. `PENDING` and `DEFERRED` are not rejections.
- Treat `rejection_reason` as an open string, not a closed enum. Meta can emit values absent from
  its public catalog.
- Report the exact code first, then its catalog meaning and provenance.
- Say that a code is undocumented when the catalog does not define it. Do not expand an enum
  name into a more precise policy allegation than the evidence supports.
- If `validated` is false, explain that the raw webhook was audited but the current payload model
  did not accept it. Inspect validation errors only when necessary and redact identifiers before
  reporting them.

## Recommend a Replacement Name

1. Review prior name-update rows for a name already approved for the same integration.
2. Verify the brand's exact spelling and capitalization on its official website or other public
   brand evidence.
3. Prefer the shortest exact public brand name. Add a department or service qualifier only when
   that complete phrase is visibly used by the business.
4. Avoid promotional language, generic-only names, URLs, phone numbers, emojis, unsupported
   third-party brands, and decorative capitalization.
5. Present the recommendation as a best next attempt, never a guarantee of Meta approval.

## Report

Include:

- payload audit ID and receipt time;
- requested display name, decision, and exact rejection code;
- whether the code is documented or only locally observed;
- whether model validation accepted the webhook;
- one recommended next name with a short evidence-based rationale;
- confirmation that inspection was read-only.

