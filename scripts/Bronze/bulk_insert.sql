CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql 
as $$
BEGIN 

	DECLARE 
		start_time TIMESTAMP;
		end_time TIMESTAMP;
	

	BEGIN TRY 
		RAISE NOTICE '========================';
		RAISE NOTICE 'LOAD Bronze Layer';
		RAISE NOTICE '========================';
	
	
		RAISE NOTICE '-----------------------------------';
		RAISE NOTICE 'LOADING CRM TABLES'; 
		RAISE NOTICE '-----------------------------------';

		SET start_time := now()
		TRUNCATE TABLE bronze.crm_cust_info;
		COPY bronze.crm_cust_info
		FROM '/Users/rahimsharifov/Documents/Playground/Bara Analytics/sql-data-warehouse-project/datasets/source_crm/cust_info.csv'
		DELIMITER ',' CSV HEADER; 
		set end_time := now()
		
		TRUNCATE TABLE bronze.crm_prd_info;
		
		COPY bronze.crm_prd_info
		FROM '/Users/rahimsharifov/Documents/Playground/Bara Analytics/sql-data-warehouse-project/datasets/source_crm/prd_info.csv'
		DELIMITER ',' CSV HEADER; 
		
		
		TRUNCATE TABLE bronze.crm_sales_details;
		
		COPY bronze.crm_sales_details
		FROM '/Users/rahimsharifov/Documents/Playground/Bara Analytics/sql-data-warehouse-project/datasets/source_crm/sales_details.csv'
		DELIMITER ',' CSV HEADER; 
		
		
	
		RAISE NOTICE '-----------------------------------';
		RAISE NOTICE 'LOADING ERP TABLES'; 
		RAISE NOTICE '-----------------------------------';
		
		TRUNCATE TABLE bronze.erp_cust_az12;
		
		COPY bronze.erp_cust_az12
		FROM '/Users/rahimsharifov/Documents/Playground/Bara Analytics/sql-data-warehouse-project/datasets/source_erp/CUST_AZ12.csv'
		DELIMITER ',' CSV HEADER; 
		
		
		
		TRUNCATE TABLE bronze.erp_loc_ac101;
		
		COPY bronze.erp_loc_ac101
		FROM '/Users/rahimsharifov/Documents/Playground/Bara Analytics/sql-data-warehouse-project/datasets/source_erp/LOC_A101.csv'
		DELIMITER ',' CSV HEADER; 
		
		
		
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		
		COPY bronze.erp_px_cat_g1v2
		FROM '/Users/rahimsharifov/Documents/Playground/Bara Analytics/sql-data-warehouse-project/datasets/source_erp/PX_CAT_G1V2.csv'
		DELIMITER ',' CSV HEADER; 
	END TRY
	BEGIN CATCH 
		RAISE NOTICE "====================================";
		RAISE NOTICE "ERROR OCCURE" + ERROR_MESSAGE();
		RAISE NOTICE "ERROR MESSAGE";  
	END CATCH 
END; $$;


