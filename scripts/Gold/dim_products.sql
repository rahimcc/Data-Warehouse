CREATE VIEW gold.dim_products AS 
SELECT
	ROW_NUMBER() OVER ( ORDER BY pn.prd_start_dt,pn.prd_key) as product_key,
	pn.prd_id as product_id,
	pn.prd_key as product_number,
	pn.prd_nm as product_name,
	
	pn.cat_id as category_id,
	pc.cat as category,
	pc.subcat as subcategoy  ,
	pc.maintenance as maintenance,
	pn.prd_cost as product_cost,
	pn.prd_line as product_line ,
	pn.prd_start_dt as start_date
	

FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc 
ON pn.cat_id = pc.id

WHERE prd_end_dt IS NULL 


SELECT * FROM gold.dim_products