# WhatsApp Display-Name Rejection Reasons

Meta sends these values in
`entry[].changes[].value.rejection_reason` when the change field is
`phone_number_name_update`. Treat the field as an open string because production can lead the
public documentation.

## Inspect stored decisions

For Sofia, run the bundled read-only query through the safe runtime helper:

```bash
~/pe/bin/python \
  ~/sofia-server/skills/inspect-sofia-runtime/scripts/query_database.py \
  --env-file ~/sofia-server/.env \
  --max-rows 100 \
  --format json \
  < ~/wa-agents/skills/inspect-wa-agents/scripts/inspect_display_name_rejections.sql
```

The query returns only the audit ID, receipt time, requested display name, decision,
rejection reason, and validation state. It omits phone numbers, WABA IDs, hashes, and
complete payload and error bodies.

If the expected row is absent:

1. Confirm the selected dotenv contains the intended database connection variable by
   key name only; never print its value.
2. Confirm Meta sent a `phone_number_name_update` subscription event.
3. Inspect bounded, redacted webhook logs with `$inspect-sofia-runtime`.
4. Do not infer a reason from `name_status = DECLINED`; use the webhook's
   `rejection_reason` value.

## Publicly documented values

| Value | Meaning | Suggested response |
| --- | --- | --- |
| `NAME_EMPLOYEE_ISSUE` | The display name includes a person's name or employee identifier. | Use the public business or brand name. |
| `NAME_INDIVIDUAL_ISSUE` | The display name includes a person's name or employee identifier. | Use the public business or brand name. |
| `NAME_ENDCLIENT_NOTRELATED` | The display name includes an unrelated business's name. | Remove the unrelated name or publish clear evidence of the relationship. |
| `NAME_FORMAT_UNACCEPTABLE` | The display-name format is unacceptable. | Remove unusual capitalization, punctuation, emojis, URLs, phone numbers, slogans, and promotional text. |
| `NAME_NOT_CONSISTENT` | The display name is inconsistent with the business's branding. | Match the spelling used on the official website and business records. |
| `UNKNOWN` | Meta did not provide a specific reason. | Check the guidelines and contact Meta support if the exact public brand name is rejected. |
| `null` | The submitted name was accepted. | No corrective action is required. |

Authoritative references:

- [Meta Developers: WhatsApp webhook components](https://developers.facebook.com/docs/whatsapp/cloud-api/webhooks/components)
- [Meta Business Help: WhatsApp display-name guidelines](https://www.facebook.com/business/help/757569725593362)

Meta pages may require a logged-in browser and can change without versioned notice. Recheck them
before treating this table as exhaustive.

## Values observed in the Sofia database

Observed through 2026-10-21:

| Value | Decision observed | Interpretation |
| --- | --- | --- |
| `NONE` | `APPROVED` | No rejection. Meta emitted a string sentinel instead of the publicly documented `null`. |
| `BIZ_COMMERCE_VIOLATION_OTHER` | `REJECTED` | Not defined in the public six-value catalog checked for this skill. Preserve and report the exact code. Its spelling suggests a broad commerce-policy bucket, but it does not identify the violated rule. |

Do not silently fold observed values into `UNKNOWN`: retaining the exact value makes future Meta
documentation changes and repeated production patterns discoverable.

## Decision values

| Value | Treatment |
| --- | --- |
| `APPROVED` | Terminal approval. A rejection reason should be `null` or a no-rejection sentinel such as the observed `NONE`. |
| `REJECTED` | Terminal rejection. Read and report `rejection_reason`. |
| `PENDING` | Review has not completed. Do not recommend a replacement yet. |
| `DEFERRED` | Meta deferred the decision. Recheck the live number status and await another outcome. |

## Recommendation heuristic

Review prior name-update rows for a name already approved for the same integration,
then verify spelling and capitalization against public brand evidence. Prefer the
shortest exact public brand name. Add a qualifier only when the complete phrase is
publicly used.

Avoid promotional language, generic-only names, URLs, phone numbers, emojis,
unsupported third-party brands, and decorative capitalization. A phrase such as
`Brand Customer Service` is weak unless the whole phrase appears as a public-facing
identity. Present a replacement as a best next attempt, never guaranteed approval.

## Interpretation and reporting

- Report the exact rejection code before its catalog meaning and provenance. Label
  codes absent from this catalog as undocumented; do not infer a more specific policy
  violation from an enum name.
- If `validated` is false, report that the raw webhook was audited but rejected by the
  current payload model. Inspect validation errors only when necessary and redact
  identifiers.
- Include the audit ID and receipt time, requested name, decision, exact code and
  provenance, validation result, and one evidence-based next name when appropriate.
- Confirm that inspection was read-only.
