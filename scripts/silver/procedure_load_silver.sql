/* 
==============================================================
Stored Procedure: Load Silver layer (Bronze -> Silver) 
==============================================================

Script Purpose: 
  This stored procedure performs the ETL process to populate the 'silver' schema tables from 'bronze' schema.

  1. Truncates silver tables. 
  2. Inserts cleaned and transformed data from bronze layer to silver layer. 

==============================================================

*/

CREATE OR ALTER PROCEDURE silver.load_silver AS 

BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;

	SET @batch_start_time=GETDATE();
	BEGIN TRY
		PRINT '==============================================';
		PRINT 'Loading silver Layer ';
		PRINT  '==============================================';

		PRINT '-----------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '-----------------------------------------------';


		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_customer_info';
		TRUNCATE TABLE silver.crm_customer_info;

		PRINT '>> Inserting Data Into: silver.crm_customer_info';
		INSERT INTO silver.crm_customer_info(
			customer_id,
			customer_key,
			customer_firstname, 
			customer_lastname,
			customer_marital_status,
			customer_gender,
			customer_create_date
		)
		SELECT 
			customer_id,
			customer_key,
			TRIM(customer_firstname) AS customer_firstname,
			TRIM(customer_lastname) AS customer_lastname,
			CASE 
				WHEN UPPER(TRIM(customer_marital_status)) = 'S' THEN 'Single'
				WHEN UPPER(TRIM(customer_marital_status)) = 'M' THEN 'Married'
			END AS customer_marital_status,
			CASE 
				WHEN UPPER(TRIM(customer_gender)) ='F' THEN 'Female'
				WHEN UPPER(TRIM(customer_gender)) = 'M' THEN 'Male'
				ELSE 'n/a'
			END AS customer_gender,
			customer_create_date 
			FROM(
				SELECT *, 
					ROW_NUMBER() OVER(PARTITION BY customer_id ORDER BY customer_create_date DESC) AS update_flag
				FROM bronze.crm_customer_info
				WHERE customer_id IS NOT NULL
			) t
			WHERE update_flag=1 --selecting most recent record per customer
			
		SET @end_time = GETDATE();

		PRINT '>> Duration of Load: '+CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+'seconds';
		PRINT '>> ------------------';



		--loading 
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_product_info';
		TRUNCATE TABLE silver.crm_product_info;
		PRINT '>> Inserting Data Into: silver.crm_product_info';
		INSERT INTO silver.crm_product_info (
			product_id,
			category_id,
			product_key,
			product_name,
			product_cost,
			product_line,
			product_start_date,
			product_end_date
		)
		SELECT 
			product_id,
			REPLACE(SUBSTRING(product_key,1,5),'-','_') AS category_id,
			SUBSTRING(product_key,7,LEN(product_key)) AS product_key,
			product_name,
			ISNULL(product_cost,0) AS product_cost,
			CASE
				WHEN UPPER(TRIM(product_line)) = 'M' THEN 'Mountain'
				WHEN UPPER(TRIM(product_line)) = 'R' THEN 'Road'
				WHEN UPPER(TRIM(product_line)) = 'S' THEN 'Other Sales'
				WHEN UPPER(TRIM(product_line)) = 'T' THEN 'Touring'
				ELSE 'n/a'
			END AS product_line, 
			CAST(product_start_date AS DATE) AS product_start_date,
			CAST(
				LEAD(product_start_date) OVER (PARTITION BY product_key ORDER BY product_start_date) -1 
				AS DATE
			) AS proudct_end_date
		FROM bronze.crm_product_info;

		SET @end_time = GETDATE();

		PRINT '>> Duration of Load: '+CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+'seconds';



		--loading crm sales details
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details;
		PRINT '>> Inserting Data Into: silver.crm_sales_details';
		INSERT INTO silver.crm_sales_details (
			sales_order_num,
			sales_product_key,
			sales_customer_id,
			sales_order_date,
			sales_ship_date,
			sales_due_dt,
			sales_sales,
			sales_quantity,
			sales_price
		)
		SELECT 
			sales_order_num,
			sales_product_key,
			sales_customer_id,
			CASE 
				WHEN sales_order_date = 0 OR LEN(sales_order_date) != 8 THEN NULL 
				ELSE CAST(CAST(sales_order_date AS VARCHAR) AS DATE)
			END AS sales_order_date,
			CASE 
				WHEN sales_ship_date = 0 OR LEN(sales_ship_date) != 8 THEN NULL 
				ELSE CAST(CAST(sales_ship_date AS VARCHAR) AS DATE) 
			END AS sales_ship_date,
			CASE 
				WHEN sales_due_dt = 0 OR LEN(sales_due_dt) != 8 THEN NULL 
				ELSE CAST(CAST(sales_due_dt AS VARCHAR) AS DATE) 
			END AS sales_due_dt,
			CASE 
				WHEN sales_sales IS NULL OR sales_sales<=0 OR sales_sales != sales_quantity * ABS(sales_price)
					THEN sales_quantity*ABS(sales_price)
				ELSE sales_sales
			END AS sales_sales,
			sales_quantity,
			CASE 
				WHEN sales_price IS NULL OR sales_price <=0
					THEN sales_sales/NULLIF(sales_quantity,0)
				ELSE sales_price
			END AS sales_price
		FROM bronze.crm_sales_details;


		SET @end_time = GETDATE();

		PRINT '>> Duration of Load: '+CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+'seconds';


		PRINT '-----------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '-----------------------------------------------';


		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_loc_a101';
		TRUNCATE TABLE silver.erp_loc_a101;
		PRINT '>> Inserting Data Into: silver.erp_loc_a101';
		INSERT INTO silver.erp_loc_a101 (
			cid,
			country
		)
		SELECT 
			REPLACE(cid,'-','') AS cid,
			CASE 
				WHEN TRIM(country) = 'DE' THEN 'Germany'
				WHEN TRIM(country) IN ('US','USA') THEN 'United States'
				WHEN TRIM(country) ='' OR country IS NULL THEN 'n/a'
				ELSE TRIM(country)
			END AS country
		FROM bronze.erp_loc_a101;

		SET @end_time = GETDATE();

		PRINT '>> Duration of Load: '+CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+'seconds';


		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_cust_az12';
		TRUNCATE TABLE silver.erp_cust_az12;
		PRINT '>> Inserting Data Into: silver.erp_cust_az12';
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
				WHEN bdate > GETDATE() THEN NULL 
				ELSE bdate
			END AS bdate,
			CASE 
				WHEN UPPER(TRIM(gen)) IN ('F','Female') THEN 'Female'
				WHEN UPPER(TRIM(gen)) IN ('M','Male') THEN 'Male'
				ELSE 'n/a'
			END AS gen
		FROM bronze.erp_cust_az12;

		SET @end_time = GETDATE();

		PRINT '>> Duration of Load: '+CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+'seconds';


		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_px_cat_g1v2';
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		PRINT '>> Inserting Data Into: silver.erp_px_cat_g1v2';
		INSERT INTO silver.erp_px_cat_g1v2 (
			id,
			cat,
			subcat,
			maintenance
		)
		SELECT 
			id,
			cat,
			subcat,
			maintenance
		FROM bronze.erp_px_cat_g1v2;

		SET @end_time = GETDATE();

		PRINT '>> Duration of Load: '+CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+'seconds';
		
		SET @batch_end_time=GETDATE();
		PRINT '=======================================';
		PRINT 'Loading silver layer is completed ';
		PRINT '>> Total Duration of Load: '+CAST(DATEDIFF(second,@batch_start_time,@batch_end_time) AS NVARCHAR)+'seconds';
		PRINT '=======================================';
		
	END TRY
	BEGIN CATCH 
		PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING silver LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Number' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error State' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='
	END CATCH

END


EXEC silver.load_silver;

