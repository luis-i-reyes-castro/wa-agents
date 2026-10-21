WITH name_updates AS (
  SELECT
    payload.id,
    payload.received_at,
    payload.validated,
    ( payload.errors IS NOT NULL ) AS has_validation_errors,
    change.value->'value' AS update_value
  FROM
    public.wa_api_inbound_payloads AS payload
  CROSS JOIN LATERAL jsonb_path_query(
    payload.data_raw,
    '$.entry[*].changes[*] ? (@.field == "phone_number_name_update")'
  ) AS change(value)
)
SELECT
  id,
  received_at,
  update_value->>'requested_verified_name' AS requested_verified_name,
  update_value->>'decision' AS decision,
  update_value->>'rejection_reason' AS rejection_reason,
  validated,
  has_validation_errors
FROM
  name_updates
ORDER BY
  received_at DESC,
  id DESC
LIMIT 100;

