-- sql/governance/row_level_security.sql

/* NEXUSFLOW REGIONAL DATA SOVEREIGNTY 
  Goal: Restrict row visibility based on the user's 'nz_region' attribute.
*/

CREATE OR REPLACE FUNCTION nff_catalog.governance.region_filter(region_name STRING)
RETURN 
  -- 1. Global access for the Compliance and Admin groups
  is_account_group_member('nff_compliance_officers') OR 
  is_account_group_member('nff_admins') OR
  
  -- 2. Regional access: User's session region must match the row's region
  -- Note: 'current_user()' returns the email/ID of the person running the query
  (region_name = 'Auckland' AND is_account_group_member('nff_region_auckland')) OR
  (region_name = 'Wellington' AND is_account_group_member('nff_region_wellington')) OR
  (region_name = 'Canterbury' AND is_account_group_member('nff_region_canterbury'));

-- 3. Apply the filter to the Silver Transactions table
-- From this moment on, 'SELECT *' returns different results for different users.
ALTER TABLE nff_catalog.silver.transactions 
SET ROW FILTER nff_catalog.governance.region_filter ON (region);

-- 4. Apply to Gold for consistent reporting
ALTER TABLE nff_catalog.gold.fct_transactions 
SET ROW FILTER nff_catalog.governance.region_filter ON (region);