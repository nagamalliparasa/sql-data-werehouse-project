/*
=================================================
Stored Procedure: Load Bronze Layer (from Source -> Bronze)
=================================================

Script Purpose: 
  This stored procedure loads data into bronze schema from external csv files. 
  1. Truncates the bronze tables before loading data. 
  2. Uses BULK INSERT command to load data from csv files 

================================================

*/


CREATE OR ALTER PROCEDURE bronze.load_bronze AS 

BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;

	SET @batch_start_time=GETDATE();
	BEGIN TRY
		PRINT '==============================================';
		PRINT 'Loading Bronze Layer ';
		PRINT  '==============================================';

		PRINT '-----------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '-----------------------------------------------';


		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_customer_info';
		TRUNCATE TABLE bronze.crm_customer_info;

		PRINT '>> Inserting Data Into: bronze.crm_customer_info';
		BULK INSERT bronze.crm_customer_info 
		FROM 'C:\Users\nparasa\Work\DataAnalysis\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();

		PRINT '>> Duration of Load: '+CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+'seconds';


		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_customer_info';
		TRUNCATE TABLE bronze.crm_product_info;
		PRINT '>> Inserting Data Into: bronze.crm_customer_info';
		BULK INSERT bronze.crm_product_info
		FROM 'C:\Users\nparasa\Work\DataAnalysis\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();

		PRINT '>> Duration of Load: '+CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+'seconds';


		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;
		PRINT '>> Inserting Data Into: bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\nparasa\Work\DataAnalysis\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		SET @end_time = GETDATE();

		PRINT '>> Duration of Load: '+CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+'seconds';


		PRINT '-----------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '-----------------------------------------------';


		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;
		PRINT '>> Inserting Data Into: bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\nparasa\Work\DataAnalysis\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();

		PRINT '>> Duration of Load: '+CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+'seconds';


		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;
		PRINT '>> Inserting Data Into: bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\nparasa\Work\DataAnalysis\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();

		PRINT '>> Duration of Load: '+CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+'seconds';


		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		PRINT '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\nparasa\Work\DataAnalysis\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();

		PRINT '>> Duration of Load: '+CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+'seconds';
		
		SET @batch_end_time=GETDATE();
		PRINT '=======================================';
		PRINT 'Loading Bronze layer is completed ';
		PRINT '>> Total Duration of Load: '+CAST(DATEDIFF(second,@batch_start_time,@batch_end_time) AS NVARCHAR)+'seconds';
		PRINT '=======================================';
		
	END TRY
	BEGIN CATCH 
		PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Number' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error State' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='
	END CATCH

	

END


EXEC bronze.load_bronze;
