-- PARAMS:
  -- contact     : api_contacts.id
  -- owner_token : UUID

DELETE FROM
  wa_agents.case_handler_contact_leases
WHERE
  ( contact     = @contact     ) AND
  ( owner_token = @owner_token )
RETURNING
  contact;
