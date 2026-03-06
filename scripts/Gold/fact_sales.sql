CREATE VIEW gold.fact_sales  as 
SELECT 
	sls_ord_num as order_number,
	pd.product_key ,
	cu.customer_key,
	sls_order_dt as order_date,
	sls_ship_dt as shipping_date,
	sls_due_dt as due_date,
	sls_sales as  sales_amount,
	sls_quantity as quantity,
	sls_price as price

FROM silver.crm_sales_details sd 
LEFT JOIN gold.dim_products pd 
ON sd.sls_prd_key = pd.product_number
LEFT JOIN gold.dim_customers cu 
ON CAST(sd.sls_cust_id AS INT) = cu.customer_id 




