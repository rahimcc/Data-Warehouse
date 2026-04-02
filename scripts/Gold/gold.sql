CALL bronze.load_bronze()

CALL silver.load_silver()


SELECT * FROM silver.crm_cust_info;

	
		TRUNCATE TABLE silver.erp_cust_az12;
		
		INSERT INTO silver.erp_cust_az12 (cid,bdate,gen)
		SELECT 
			CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LENGTH(cid))
			ELSE cid
		 	END as cid, 
			
			CASE WHEN bdate > NOW() THEN NULL 
			ELSE bdate
			END as bdate,
		
			CASE WHEN UPPER(TRIM(gen)) IN  ('M','MALE') THEN 'Male'
			     WHEN UPPER(TRIM(gen)) IN  ('F','FEMALE') THEN 'Female'
			ELSE 'n/a'
			END as gen
		
		FROM bronze.erp_cust_az12 ;


SELECT COUNT(*),gen FROM silver.erp_cust_az12
GROUP BY gen


SELECT COUNT(*), gen FROM bronze.erp_cust_az12
GROUP BY gen