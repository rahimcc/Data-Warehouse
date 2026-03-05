SELECT cst_firstname FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)


SELECT cst_lastname FROM silver.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname)


-- Quality check consistensy of values in low cardinality columns 

SELECT DISTINCT cst_gndr FROM silver.crm_cust_info