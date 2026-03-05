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

FROM bronze.crm_prd_info



SELECT COUNT(*),prd_id FROM silver.crm_prd_info 
GROUP BY prd_id
HAVING COUNT(*) > 1 or prod_id N


-- prd_key , Product key hold two information


-- prd_nm check for extra space 


SELECT * FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)


--check prd_cost for NULL or Negative VALUES. 

SELECT * FROM silver.crm_prd_info
WHERE prd_cost < 0 or prd_cost IS null


-- check prd_line 

SELECT DISTINCT prd_line FROM bronze.crm_prd_info 


-- check prd_start_dt , prd_end_dt , check for invalid date orders 


SELECT prd_id, prd_key,prd_start_dt,prd_end_dt,LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt )-1 as prd_end_dt_test
FROM bronze.crm_prd_info
WHERE prd_key IN ('AC-HE-HL-U509-R','AC-HE-HL-U509')


SELECT * FROM silver.crm_prd_info 




