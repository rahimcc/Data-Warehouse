CREATE VIEW gold.dim_customers as 
SELECT 
		ROW_NUMBER() OVER (ORDER BY cst_id) as customer_key,
		ci.cst_id as customer_id, 
		ci.cst_key as customer_number, 
		ci.cst_firstname as first_name, 
		ci.cst_lastname as last_name,
		ci.cst_marital_status as marital_status,
		CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr
			ELSE COALESCE(gen,'n/a')
		END AS gender,
		ci.cst_create_date as create_date ,
		ca.bdate as birthdate,
		la.cntry as country
	FROM silver.crm_cust_info as ci
	LEFT JOIN silver.erp_cust_az12 as ca 
	ON  ci.cst_key = ca.cid
	LEFT JOIN silver.erp_loc_ac101 as la 
	ON  ci.cst_key = la.cid 


SELECT  
		CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr
		ELSE COALESCE(gen,'n/a')
		END AS NEW_GENDER,
		ci.cst_gndr,
		ca.gen
	FROM silver.crm_cust_info as ci
	LEFT JOIN silver.erp_cust_az12 as ca 
	ON  ci.cst_key = ca.cid
	LEFT JOIN silver.erp_loc_ac101 as la 
	ON  ci.cst_key = la.cid 
