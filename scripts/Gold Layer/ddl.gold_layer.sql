/***************************************************************************************
Script Name   : silver.create_tables
Schema        : silver
Author 		  : Oybek ALikulov


Script Purpose:
	This script creates views for the Gold layer in the data warehouse
	The Gold layer represents the final dimension and fact tables (Star Schema)

	Each view performs transformations and combines data from the Silver layer
	the produce a clean, enriched, and busines-ready dataset.

***************************************************************************************/


--**************************************************************
--CREATE DIMENSION: gold.dim_customers
--**************************************************************

IF OBJECT_ID ('gold.dim_customers','V') IS NOT NULL
	DROP VIEW gold.dim_customers;
GO

CREATE VIEW gold.dim_customers AS
SELECT 
	ROW_NUMBER() OVER(ORDER BY ci.cst_id) csutomer_key,--SUROGATE KEY
	ci.cst_id AS customer_id,
	ci.cst_key AS customer_number,
	ci.cst_firstname AS first_name,
	ci.cst_lastname AS last_name,
	cl.cntry AS country,
	ci.cst_marital_status AS marital_status,
	CASE	
		WHEN ci.cst_gndr!='n/a' THEN ci.cst_gndr
		ELSE COALESCE(ca.gen,'n/a')
	END gender,
	ca.bdate AS birth_date,
	ci.cst_create_date AS create_date
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_loc_a101 cl
ON ci.cst_key=cl.cid
LEFT JOIN silver.erp_cust_az12 ca
ON ci.cst_key=ca.cid
GO

--**************************************************************
--CREATE DIMENSION: gold.dim_products
--**************************************************************
IF OBJECT_ID('gold.dim_products','V')  IS NOT NULL 
	DROP VIEW gold.dim_products;
GO

CREATE VIEW gold.dim_products AS
SELECT 
	ROW_NUMBER() OVER(ORDER BY pm.prd_start_dt,pm.prd_key) AS product_key, --SUROGATE KEY
	pm.prd_id AS product_id,
	pm.prd_key AS product_number,
	pm.prd_nm AS product_name,
	pm.cat_id AS category_id,
	pc.cat AS category,
	pc.subcat AS subcategory,
	pc.maintenance,
	pm.prd_cost AS cost,
	pm.prd_line AS product_line,
	pm.prd_start_dt AS start_date
FROM silver.crm_prd_info pm
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON pm.cat_id=pc.id
WHERE pm.prd_end_dt IS NULL;
GO;

--**************************************************************
--CREATE FACT: gold.fact_sales
--**************************************************************
IF OBJECT_ID('gold.fact_sales','V') IS NOT NULL
	DROP VIEW gold.fact_sales;
GO

CREATE VIEW gold.fact_sales AS 
SELECT 
	sd.sls_ord_num AS order_number,
	dp.product_key ,
	dc.csutomer_key,
	sd.sls_order_dt AS order_date,
	sd.sls_ship_dt AS shipping_date,
	sd.sls_due_dt AS due_date,
	sd.sls_sales AS sales,
	sd.sls_quantity AS qauntity,
	sd.sls_price AS price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products dp
ON dp.product_number=sd.sls_prd_key
LEFT JOIN gold.dim_customers dc
ON dc.customer_id=sd.sls_cust_id
GO
