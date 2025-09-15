/***************************************************************************************
Script Name   : silver.create_tables
Schema        : silver
Author 		  : Oybek ALikulov


Script Purpose:
	This script creates a new CRM and ERP tables for Silver Layer.
	Existing tables are DROPPED and RECREATED each time when the procedure is executed.
	(full refresh approach).

Note		  :
	Added `dwh_create_date` column to all tables (differentiating from the bronze layer)

***************************************************************************************/
CREATE OR ALTER PROCEDURE silver.create_tables AS 
BEGIN
	PRINT '====================================================================='
	PRINT 'CREATE TABLES'
	PRINT '====================================================================='

	IF OBJECT_ID('silver.crm_cust_info', 'U') IS NOT NULL
		DROP TABLE silver.crm_cust_info;
	CREATE TABLE silver.crm_cust_info (
			cst_id INT,
			cst_key NVARCHAR(50),
			cst_firstname NVARCHAR(50),
			cst_lastname NVARCHAR(50),
			cst_marital_status NVARCHAR(50),
			cst_gndr NVARCHAR(50),
			cst_create_date DATE,
			dwh_create_date DATETIME2 DEFAULT GETDATE()
			);

	IF OBJECT_ID('silver.crm_prd_info', 'U') IS NOT NULL
		DROP TABLE silver.crm_prd_info;
	CREATE TABLE silver.crm_prd_info (
			prd_id INT,
			prd_key NVARCHAR(50),
			prd_nm NVARCHAR(50),
			prd_cost DECIMAL(10,2),
			prd_line NVARCHAR(50),
			prd_start_dt DATETIME,
			prd_end_dt DATETIME,
			dwh_create_date DATETIME2 DEFAULT GETDATE()
			);

	IF OBJECT_ID('silver.crm_sales_details','U') IS NOT NULL
		DROP TABLE silver.crm_sales_details;
	CREATE TABLE silver.crm_sales_details (
			sls_ord_num NVARCHAR(50),
			sls_prd_key NVARCHAR(50),
			sls_cust_id INT,
			sls_order_dt INT,
			sls_ship_dt INT,
			sls_due_dt INT,
			sls_sales INT,
			sls_quantity INT,
			sls_price INT,
			dwh_create_date DATETIME2 DEFAULT GETDATE()
			);

	IF OBJECT_ID('silver.erp_CUST_AZ12','U') IS NOT NULL
		DROP TABLE silver.erp_CUST_AZ12;
	CREATE TABLE silver.erp_CUST_AZ12 (
			cid	NVARCHAR(50),
			bdate DATE,
			gen NVARCHAR(10),
			dwh_create_date DATETIME2 DEFAULT GETDATE()
			);

	IF OBJECT_ID('silver.erp_LOC_A101', 'U') IS NOT NULL
		DROP TABLE silver.erp_LOC_A101;
	CREATE TABLE silver.erp_LOC_A101 (
			cid NVARCHAR(50),
			cntry NVARCHAR(50),
			dwh_create_date DATETIME2 DEFAULT GETDATE()
			);

	IF OBJECT_ID('silver.erp_PX_CAT_G1V2','U') IS NOT NULL
		DROP TABLE silver.erp_PX_CAT_G1V2;
	CREATE TABLE silver.erp_PX_CAT_G1V2 (
			id NVARCHAR(50),
			cat NVARCHAR(50),
			subcat NVARCHAR(50),
			maintenance NVARCHAR(50),
			dwh_create_date DATETIME2 DEFAULT GETDATE()
			);
END
