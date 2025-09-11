/******************************************************************************************************
  Script Name  : bronze.load_data
  Schema       : bronze
  Object Type  : Stored Porcedure
  Author       : Oybek Alikulov

  Purpose      
    This stored procedure automates the loading of CRM and ERP source data 
    from flat files (CSV) into the Bronze layer tables of the data warehouse. 
    It ensures fresh loads by truncating existing tables and bulk inserting 
    new data from source files. 
    Execution time is logged for each table as well as for the entire load.

  Parameters   :
    None (currently hard-coded file paths for bulk insert).
    Future enhancement: add parameters for dynamic file paths or logging table.

  Error Handling:
    TRY...CATCH block captures any SQL errors.
    Prints error message, number, and state to console.


*****************************************************************************************************/
  
CREATE OR ALTER PROCEDURE bronze.load_data AS 
BEGIN
	DECLARE @start_time  DATETIME, @end_time DATETIME, @start_time_loading DATETIME, @end_time_loading DATETIME;
	BEGIN TRY
		SET @start_time_loading = GETDATE();

		PRINT '==========================================================='
		PRINT 'LOADING BRONZE LAYER'
		PRINT '==========================================================='
		PRINT ''

		PRINT '-----------------------------------------------------------'
		PRINT 'LOAD CRM TABLES'
		PRINT '-----------------------------------------------------------'

		SET @start_time=GETDATE();
		TRUNCATE TABLE bronze.crm_cust_info;
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\Oybek\Documents\My projects\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
			);
		SET @end_time = GETDATE();
		PRINT '>> LOADING DURATION: '+ CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + ' SECONDS'
		PRINT ''

		SET @start_time=GETDATE();
		TRUNCATE TABLE bronze.crm_prd_info;
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\Oybek\Documents\My projects\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
			);
		SET @end_time = GETDATE();
		PRINT '>> LOADING DURATION: '+ CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + ' SECONDS'
		PRINT ''

		SET @start_time=GETDATE();
		TRUNCATE TABLE bronze.crm_sales_details;
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\Oybek\Documents\My projects\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
			);
		SET @end_time = GETDATE();
		PRINT '>> LOADING DURATION: '+ CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + ' SECONDS'
		
		PRINT ''
		PRINT '-----------------------------------------------------------'
		PRINT 'LOAD ERP TABLES'
		PRINT '-----------------------------------------------------------'

		SET @start_time=GETDATE();
		TRUNCATE TABLE bronze.erp_cust_az12;
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\Oybek\Documents\My projects\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
			);
		SET @end_time = GETDATE();
		PRINT '>> LOADING DURATION: '+ CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + ' SECONDS'
		PRINT ''

		SET @start_time=GETDATE();
		TRUNCATE TABLE bronze.erp_loc_a101;
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\Oybek\Documents\My projects\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
			);
		SET @end_time = GETDATE();
		PRINT '>> LOADING DURATION: '+ CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + ' SECONDS'
		PRINT ''

		SET @start_time=GETDATE();
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\Oybek\Documents\My projects\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
			);
		SET @end_time = GETDATE();
		PRINT '>> LOADING DURATION: '+ CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + ' SECONDS'
		PRINT ''
		SET @end_time_loading = GETDATE();
		
		PRINT '==========================================================='
		PRINT 'LOADING BRONZE LAYER IS COMPLETED'
		PRINT '>> TOTAL LOAD DURATION: ' + CAST(DATEDIFF(SECOND,@start_time_loading,@end_time_loading) AS NVARCHAR) + ' SECONDS'
		PRINT '==========================================================='

	END TRY
	BEGIN CATCH
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'ERROR MESSAGE'+ ERROR_MESSAGE()
		PRINT 'ERROR NUMBER' + CAST(ERROR_NUMBER() AS NVARCHAR)
		PRINT 'ERROR STATE' + CAST(ERROR_STATE() AS NVARCHAR)
	END CATCH
END;
