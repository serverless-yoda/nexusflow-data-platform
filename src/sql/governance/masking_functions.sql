CREATE OR REPLACE FUNCTION nff_catalog.governance.email_mask(email STRING)
RETURN CASE
  WHEN is_account_group_member('nff_compliance_officers') THEN email
  ELSE regexp_replace(email, '^.*@', '****@')
END;

-- Apply the mask to the Silver Layer
ALTER TABLE nff_catalog.silver.customers 
ALTER COLUMN email SET MASK nff_catalog.governance.email_mask;