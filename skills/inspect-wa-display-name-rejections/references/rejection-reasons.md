# WhatsApp Display-Name Rejection Reasons

Meta sends these values in
`entry[].changes[].value.rejection_reason` when the change field is
`phone_number_name_update`. Treat the field as an open string because production can lead the
public documentation.

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

The safest next attempt is normally the exact, shortest brand name visibly used on the official
website. A phrase such as `Brand Customer Service` is weaker unless the whole phrase appears as a
public-facing identity; a page navigation heading named “Customer service” does not necessarily
establish that compound phrase as a brand.
