-- src/sql/governance/row_level_security.sql

-- 1. Define the Regional Security Function
-- This function returns TRUE if the user is allowed to see the row.
CREATE OR REPLACE FUNCTION nff_catalog.main.regional_filter(region_name STRING)
RETURN 
  -- Admins see everything (Auckland, Wellington, etc.)
  is_account_group_member('nff_admin') 
  OR 
  -- Regional staff only see rows where the column matches their group suffix
  -- e.g., Group 'nff_region_auckland' matches region 'Auckland'
  is_account_group_member(CONCAT('nff_region_', LOWER(region_name)));

-- 2. How it's applied (Executed via GovernanceManager)
-- ALTER TABLE nff_catalog.silver.transactions 
-- SET ROW FILTER nff_catalog.main.regional_filter ON (region);