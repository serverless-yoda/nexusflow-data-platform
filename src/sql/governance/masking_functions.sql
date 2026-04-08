-- src/sql/governance/masking_functions.sql

-- 1. Full Redaction Mask
-- Used for high-sensitivity fields like Customer Names or Addresses
CREATE OR REPLACE FUNCTION nff_catalog.main.redact_mask(col_value STRING)
RETURN CASE 
    WHEN is_account_group_member('nff_data_stewards') THEN col_value 
    ELSE '### REDACTED ###' 
END;

-- 2. Partial Mask (e.g., for Account Numbers or IDs)
-- Shows only the last 4 characters for context, masks the rest
CREATE OR REPLACE FUNCTION nff_catalog.main.partial_id_mask(col_value STRING)
RETURN CASE 
    WHEN is_account_group_member('nff_data_stewards') THEN col_value
    ELSE CONCAT('XXXX-', RIGHT(col_value, 4))
END;

-- 3. Dynamic Email Masking
-- Keeps the domain for analytics but hides the user identity
CREATE OR REPLACE FUNCTION nff_catalog.main.email_mask(email STRING)
RETURN CASE 
    WHEN is_account_group_member('nff_data_stewards') THEN email
    ELSE REGEXP_REPLACE(email, '^.*@', 'masked-user@')
END;