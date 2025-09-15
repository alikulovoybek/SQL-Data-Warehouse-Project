/***************************************************************************************
Script Name   : silver.load_silver
Schema        : silver
Author        : Oybek Alikulov

Script Purpose:
    This script loads and transforms data from the Bronze Layer into the Silver Layer.
    Data is cleaned, standardized, and enriched with business rules before being inserted
    into the corresponding silver tables.
    Each table is truncated and reloaded during execution (full refresh approach).

Transformations Applied:
    - Removed unwanted spaces in customer names
    - Normalized marital status and gender values
    - Derived category IDs from product keys
    - Standardized product line descriptions
    - Fixed invalid or inconsistent dates
    - Validated and recalculated sales amounts
    - Standardized gender, country codes, and customer IDs in ERP data

***************************************************************************************/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @batch_start_time DATETIME, @batch_end_time DATETIME,
			@start_time DATETIME, @end_time DATETIME
	BEGIN TRY
		PRINT '=================================================================='
		PRINT 'LOADING SILVER LAYER'
		PRINT ''

		SET @batch_start_time = GETDATE()
		SET @start_time = GETDATE()

		TRUNCATE TABLE silver.crm_cust_info;
		INSERT INTO silver.crm_cust_info (
			   cst_id,
			   cst_key,
			   cst_firstname,
			   cst_lastname,
			   cst_marital_status,
			   cst_gndr,
			   cst_create_date
			   )
		SELECT cst_id,
			   cst_key,
			   TRIM(cst_firstname) cst_firstname, --Remove unwanted spaces
			   TRIM(cst_lastname) cst_lastname,   --Remove unwanted spaces
			   CASE 
					WHEN UPPER(TRIM(cst_marital_status)) ='M' THEN 'Married'
					WHEN UPPER(TRIM(cst_marital_status)) ='S' THEN 'Single'
					ELSE 'n/a'
			   END cst_marital_status, --Normalize marital status values to readeble format
			   CASE 
					WHEN UPPER(TRIM(cst_gndr)) ='M' THEN 'Male'
					WHEN UPPER(TRIM(cst_gndr)) ='F' THEN 'Female'
					ELSE 'n/a'
			   END cst_gndr,  --Normalize Gender values to readeble format
			   cst_create_date
		FROM (
				SELECT *,	
					   ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC ) AS flag_number 
				FROM bronze.crm_cust_info
				WHERE cst_id IS NOT NULL) A
		WHERE flag_number=1
		SET @end_time=GETDATE()
		PRINT 'Inserted data -> silver.crm_cust_info table'
		PRINT 'LOAD DURATION IS '+ CAST(DATEDIFF(SECOND,@start_time,@end_time) AS VARCHAR)+' SECONDS'
		PRINT '------------------------------------------------------------------'

		TRUNCATE TABLE silver.crm_prd_info;
		SET @start_time = GETDATE()
		INSERT INTO silver.crm_prd_info (
			prd_id ,
			cat_id ,
			prd_key ,
			prd_nm ,
			prd_cost,
			prd_line ,
			prd_start_dt,
			prd_end_dt
			)
		SELECT 
			prd_id,
			REPLACE(SUBSTRING(TRIM(prd_key),1,5),'-','_') AS cat_id,
			SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,
			prd_nm,
			ISNULL(prd_cost,0) AS prd_cost,
			CASE UPPER(TRIM(prd_line))	
				WHEN 'M' THEN 'Mountain'
				WHEN 'R' THEN 'Road'
				WHEN 'S' THEN 'Other Sales'
				WHEN 'T' THEN 'Touring'
				ELSE 'n/a'
			END prd_line,
			prd_start_dt,
			DATEADD(DAY,-1,LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt)) AS prd_end_dt
		FROM bronze.crm_prd_info
		SET @end_time=GETDATE()
		PRINT 'Inserted data -> silver.crm_prd_info table'
		PRINT 'LOAD DURATION IS '+ CAST(DATEDIFF(SECOND,@start_time,@end_time) AS VARCHAR)+' SECONDS'
		PRINT '------------------------------------------------------------------'

		TRUNCATE TABLE silver.crm_sales_details;
		SET @start_time = GETDATE()
		INSERT INTO silver.crm_sales_details(
			sls_ord_num ,
			sls_prd_key,
			sls_cust_id ,
			sls_order_dt ,
			sls_ship_dt,
			sls_due_dt,
			sls_sales ,
			sls_quantity,
			sls_price
			)
		SELECT  sls_ord_num
			  ,sls_prd_key
			  ,sls_cust_id,
			  CASE 
					WHEN sls_order_dt<=0 OR LEN(sls_order_dt)!=8 THEN NULL
					ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
			  END sls_order_dt,
			  CASE 
					WHEN sls_ship_dt<=0 OR LEN(sls_ship_dt)!=8 THEN NULL
					ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
			  END sls_ship_dt,
			  CASE 
					WHEN sls_due_dt<=0 OR LEN(sls_due_dt)!=8 THEN NULL
					ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
			  END sls_due_dt,
			  CASE 
					WHEN sls_sales<=0 OR sls_sales IS NULL OR sls_sales != sls_quantity*ABS(sls_price)
					THEN sls_quantity*ABS(sls_price)
					ELSE sls_sales
			  END AS  sls_sales
			  ,sls_quantity,
			  CASE 
					WHEN sls_price<=0 OR sls_price IS NULL
					THEN sls_sales/NULLIF(sls_quantity,0)
					ELSE sls_price
			  END AS sls_price
		  FROM  bronze.crm_sales_details
		SET @end_time=GETDATE()
		PRINT 'Inserted data -> silver.crm_sales_details table'
		PRINT 'LOAD DURATION IS '+ CAST(DATEDIFF(SECOND,@start_time,@end_time) AS VARCHAR)+' SECONDS'
		PRINT '------------------------------------------------------------------'


		TRUNCATE TABLE silver.erp_cust_az12;
		SET @start_time = GETDATE()
		INSERT INTO silver.erp_cust_az12(
			cid,
			bdate,
			gen
			)
		SELECT 
			CASE	
				WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
				ELSE cid
			END AS cid,
			CASE
				WHEN bdate>GETDATE() THEN NULL
				ELSE bdate 
			END AS bdate,
			CASE 
				WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
				WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
				ELSE 'N/A'
			END AS gen
		FROM bronze.erp_cust_az12
		SET @end_time=GETDATE()
		PRINT 'Inserted data -> silver.erp_cust_az12 table'
		PRINT 'LOAD DURATION IS '+ CAST(DATEDIFF(SECOND,@start_time,@end_time) AS VARCHAR)+' SECONDS'
		PRINT '------------------------------------------------------------------'


		TRUNCATE TABLE silver.erp_loc_a101;
		SET @start_time = GETDATE()
		INSERT INTO silver.erp_loc_a101(
			cid,cntry)
		SELECT 
			REPLACE(cid,'-','') cid,
			CASE 
				WHEN TRIM(cntry) ='DE' THEN 'Germany'
				WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
				WHEN cntry IS NULL OR cntry='' THEN 'n/a'
				ELSE cntry
			END AS cntry	
		FROM bronze.erp_loc_a101
		SET @end_time=GETDATE()
		PRINT 'Inserted data -> silver.erp_loc_a101 table'
		PRINT 'LOAD DURATION IS '+ CAST(DATEDIFF(SECOND,@start_time,@end_time) AS VARCHAR)+' SECONDS'
		PRINT '------------------------------------------------------------------'


		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		SET @start_time = GETDATE()
		INSERT INTO silver.erp_px_cat_g1v2(
			id,
			cat,
			subcat,
			maintenance)
		SELECT 
			id,
			cat,
			subcat,
			maintenance
		FROM bronze.erp_px_cat_g1v2
		SET @end_time=GETDATE()
		PRINT 'Inserted data -> silver.erp_px_cat_g1v2 table'
		PRINT 'LOAD DURATION IS '+ CAST(DATEDIFF(SECOND,@start_time,@end_time) AS VARCHAR)+' SECONDS'
		PRINT '------------------------------------------------------------------'
		SET @batch_end_time =GETDATE()
		PRINT ''
		PRINT 'LOADING SILVER LAYER IS COMPLETED'
		PRINT 'TOTAL LOADING TIME IS ' + CAST(DATEDIFF(SECOND,@batch_start_time,@batch_end_time) AS VARCHAR)+' SECONDS'
		PRINT '=================================================================='
	END TRY
	BEGIN CATCH
		PRINT '=================================================================='
		PRINT 'ERROR OCCURED WHILE INSERTING SILVER LAYER'
		PRINT 'ERROR MESSAGE IS ' + ERROR_MESSAGE()
		PRINT 'ERROR NUMBER IS '+ CAST(ERROR_NUMBER() AS VARCHAR)
		PRINT '=================================================================='
	END CATCH
END
