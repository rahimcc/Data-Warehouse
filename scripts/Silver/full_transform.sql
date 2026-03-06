CREATE OR REPLACE PROCEDURE silver.load_silver()
LANGUAGE plpgsql
AS $$
BEGIN 	

	DECLARE
		start_time  TIMESTAMP;
		end_time TIMESTAMP;
		batch_start_time TIMESTAMP; 
		batch_end_time TIMESTAMP; 
	

	BEGIN
		batch_start_time := clock_timestamp();

	
		RAISE NOTICE 'Truncating silver.crm_cust_info';
		TRUNCATE TABLE silver.crm_cust_info ;
		
		RAISE NOTICE 'Inserting into silver.crm_cust_info';
		INSERT INTO silver.crm_cust_info ( 
			cst_id, 
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_marital_status, 
			cst_gndr,
			cst_create_date
		)
		SELECT
			cst_id, 
			cst_key,
			TRIM(cst_firstname) as cst_firstname, 
			TRIM(cst_lastname) as cst_lastname,
		
			CASE 
				WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
				WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
				ELSE 'n/a'
			
			END cst_marital_status,
				CASE
				  WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
				  WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
				  ELSE 'n/a'
				END cst_gndr, 
			cst_create_date
			
		FROM 
			( SELECT * FROM 
					( SELECT *, ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date) as flag_last
					 FROM bronze.crm_cust_info) as foo 
			WHERE flag_last = 1 ) as dedup ; 
		
		
		RAISE NOTICE 'Truncating silver.crm_prd_info' ; 
		TRUNCATE TABLE silver.crm_prd_info ;
		RAISE NOTICE 'Inserting into silver.crm_prd_info' ;
		INSERT INTO silver.crm_prd_info  ( 
			prd_id,
			cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt
		) 
		SELECT 
			prd_id,
			REPLACE(SUBSTRING(prd_key,1,5),'-','_') as cat_id,
			SUBSTRING(prd_key,7,LENGTH(prd_key)) as prd_key,
			prd_nm,
			COALESCE(prd_cost,0) as prd_cost, 
		
			CASE 
				WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
				WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
				WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
				WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
				ELSE 'n/a'
			END prd_line,
			prd_start_dt,
			LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt )-1 as prd_end_dt
		
		FROM bronze.crm_prd_info ; 
		
		
		RAISE NOTICE 'Truncating silver.crm_sales_details' ;
		TRUNCATE TABLE silver.crm_sales_details ;
		RAISE NOTICE 'Inserting Data Into: silver.crm_sales_details' ;
		INSERT INTO silver.crm_sales_details ( 
			sls_ord_num, 
			sls_prd_key,
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			sls_sales,
			sls_quantity,
			sls_price 
			)	
		SELECT 
			sls_ord_num, 
			sls_prd_key,
			sls_cust_id,
			CASE 
				WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt::TEXT) != 8 THEN NULL
				ELSE CAST(CAST(sls_order_dt as VARCHAR(50)) as DATE) 
			END as sls_order_dt,
			CASE 
				WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt::TEXT) != 8 THEN NULL
				ELSE CAST(CAST(sls_ship_dt as VARCHAR(50)) as DATE) 
			END as sls_ship_dt, 
			CASE 
				WHEN sls_due_dt = 0 OR LENGTH(sls_due_dt::TEXT) != 8 THEN NULL
				ELSE CAST(CAST(sls_due_dt as VARCHAR(50)) as DATE) 
			END as sls_due_dt,
			CASE WHEN sls_sales <= 0 OR sls_sales IS NULL OR sls_sales != sls_quantity * ABS(sls_price)
			 THEN sls_quantity * ABS(sls_price)
			 ELSE sls_sales 
			END as sls_sales, 
		sls_quantity,
			CASE WHEN sls_price <=0 OR sls_price IS NULL 
				 THEN sls_sales / NULLIF(sls_quantity,0)
			 	ELSE sls_price
			END AS sls_price
			FROM bronze.crm_sales_details; 
		
		
		RAISE NOTICE 'Truncating erp_cust_az12 table'; 
		TRUNCATE TABLE silver.erp_cust_az12; 
		
		RAISE NOTICE 'Inserting table to erp_cust_az12';
		
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
		
		FROM bronze.erp_cust_az12 ;
		
		
		RAISE NOTICE 'Truncating silver.erp_loc_ac101'; 
		TRUNCATE TABLE silver.erp_loc_ac101;
		
		RAISE NOTICE 'Inserting data to table: silver.erp_loc_ac101'; 
		INSERT INTO silver.erp_loc_ac101
		(cid, cntry)
		SELECT
			REPLACE(cid,'-','') cid,
			CASE WHEN UPPER(TRIM(cntry))= 'DE' THEN 'Germany'
			WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
			WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
			ELSE cntry
			END as cntry 
			
		FROM bronze.erp_loc_ac101 ;
		
		
		RAISE NOTICE 'Truncating silver.erp_px_cat_g1ve'; 
		TRUNCATE TABLE silver.erp_px_cat_g1v2; 
		RAISE NOTICE 'Inserting into silver.erp_px_cat_g1v2'; 
		INSERT INTO silver.erp_px_cat_g1v2
		( id ,cat,subcat, maintenance)
		SELECT 
			id,
			cat,
			subcat,
			maintenance
		FROM bronze.erp_px_cat_g1v2 ;


		
		batch_end_time := clock_timestamp();
	    RAISE NOTICE 'Silver level duration: %s ', EXTRACT( EPOCH FROM batch_end_time - batch_start_time ) * 1000;
	 
	
		EXCEPTION
			 WHEN OTHERS THEN 
				 RAISE NOTICE '====================================';
				 RAISE NOTICE 'ERROR OCCURE %' , SQLERRM; 
			   	 RAISE NOTICE 'ERROR MESSAGE %' , SQLSTATE; 
		 
	END;


	
END;
$$;

