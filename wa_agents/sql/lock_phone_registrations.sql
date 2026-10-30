-- PARAMS:
  -- phone_number_id : str

-- Execute before activation, in the same transaction, to refresh its SQL snapshot.
SELECT
  num.id
FROM
  wa_agents.api_phone_numbers AS num
WHERE
  (
    num.display_phone_number = (
      SELECT
        target.display_phone_number
      FROM
        wa_agents.api_phone_numbers AS target
      WHERE
        ( target.phone_number_id = @phone_number_id )
    )
  )
ORDER BY
  num.id
FOR UPDATE;
