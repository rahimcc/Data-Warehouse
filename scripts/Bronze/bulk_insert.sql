CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql 
as $$
BEGIN 

	DECLARE
		start_time  TIMESTAMP;
		end_time TIMESTAMP;
		batch_start_time TIMESTAMP; 
		batch_end_time TIMESTAMP; 

	
	BEGIN 
		batch_start_time := clock_timestamp();
		RAISE NOTICE '========================';
		RAISE NOTICE 'LOAD Bronze Layer';
		RAISE NOTICE '========================';

		RAISE NOTICE '-----------------------------------';
		RAISE NOTICE 'LOADING CRM TABLES'; 
		RAISE NOTICE '-----------------------------------';

		start_time := clock_timestamp();
		TRUNCATE TABLE bronze.crm_cust_info;
		COPY bronze.crm_cust_info
		FROM '/Users/rahimsharifov/Documents/Playground/Bara Analytics/sql-data-warehouse-project/datasets/source_crm/cust_info.csv'
		DELIMITER ',' CSV HEADER; 
		end_time := clock_timestamp();
		RAISE NOTICE 'CRM_cust_info load duration: %s ', EXTRACT(EPOCH FROM (end_time - start_time)) * 1000 ;


		start_time := clock_timestamp();
		TRUNCATE TABLE bronze.crm_prd_info;
		COPY bronze.crm_prd_info
		FROM '/Users/rahimsharifov/Documents/Playground/Bara Analytics/sql-data-warehouse-project/datasets/source_crm/prd_info.csv'
		DELIMITER ',' CSV HEADER; 
		end_time := clock_timestamp(); 
		RAISE NOTICE 'CRM PROD INFO load duration: %s ', EXTRACT(EPOCH FROM (end_time - start_time )) * 1000; 
		

		start_time := clock_timestamp(); 
		TRUNCATE TABLE bronze.crm_sales_details;
		COPY bronze.crm_sales_details
		FROM '/Users/rahimsharifov/Documents/Playground/Bara Analytics/sql-data-warehouse-project/datasets/source_crm/sales_details.csv'
		DELIMITER ',' CSV HEADER;
		end_time := clock_timestamp(); 
		RAISE NOTICE 'CRM sales load duration: %s ', EXTRACT(EPOCH FROM (end_time - start_time)) * 1000; 
		
		RAISE NOTICE '-----------------------------------';
		RAISE NOTICE 'LOADING ERP TABLES'; 
		RAISE NOTICE '-----------------------------------';

		start_time := clock_timestamp();
		TRUNCATE TABLE bronze.erp_cust_az12;
		COPY bronze.erp_cust_az12
		FROM '/Users/rahimsharifov/Documents/Playground/Bara Analytics/sql-data-warehouse-project/datasets/source_erp/CUST_AZ12.csv'
		DELIMITER ',' CSV HEADER;
		end_time := clock_timestamp(); 
		RAISE NOTICE 'ERP cust az  load duration: %s', EXTRACT(EPOCH FROM (end_time - start_time )) * 1000; 


		start_time := clock_timestamp();
		TRUNCATE TABLE bronze.erp_loc_ac101;
		COPY bronze.erp_loc_ac101
		FROM '/Users/rahimsharifov/Documents/Playground/Bara Analytics/sql-data-warehouse-project/datasets/source_erp/LOC_A101.csv'
		DELIMITER ',' CSV HEADER;
		end_time := clock_timestamp(); 
		RAISE NOTICE 'ERP loc ac load duration: %s', EXTRACT( EPOCH FROM (end_time - start_time )) * 1000 ; 
		
		
		start_time := clock_timestamp();
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		COPY bronze.erp_px_cat_g1v2
		FROM '/Users/rahimsharifov/Documents/Playground/Bara Analytics/sql-data-warehouse-project/datasets/source_erp/PX_CAT_G1V2.csv'
		DELIMITER ',' CSV HEADER;
		end_time := clock_timestamp(); 
		RAISE NOTICE 'ERP px cat g1v2 load duration: %s', EXTRACT( EPOCH FROM (end_time - start_time )) * 1000; 

		 batch_end_time := clock_timestamp();
	     RAISE NOTICE 'Bronze level duration: %s ', EXTRACT( EPOCH FROM batch_end_time - batch_start_time ) * 1000;
	 
	 EXCEPTION
		 WHEN OTHERS THEN 
			 RAISE NOTICE '====================================';
			 RAISE NOTICE 'ERROR OCCURE %' , SQLERRM; 
		   	 RAISE NOTICE 'ERROR MESSAGE %' , SQLSTATE; 
	 
	 END;
	 

	
END;
$$;


