INSERT INTO silver.erp_cust_az12 (cid,bdate,gen)

SELECT 
	CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LENGTH(cid))
	ELSE cid
 	END as cid, 
	
	CASE WHEN bdate > NOW() THEN NULL 
	ELSE bdate
	END as bdate,

	CASE WHEN UPPER(TRIM(gen)) IN  ('M','Male') THEN 'Male'
	     WHEN UPPER(TRIM(gen)) IN  ('F','Female') THEN 'Female'
	ELSE 'n/a'
	END as gen

FROM bronze.erp_cust_az12 




-- check cid ,
-- 	CID starts with NAS


--check bdate 
-- check for very old date 
SELECT bdate FROM silver.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > NOW()

--check gen , low cardinality 

SELECT DISTINCT gen
FROM silver.erp_cust_az12


