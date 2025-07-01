/*

==========================================================
DDL Script: Create silver tables
==========================================================
Script Purpose: 
  This scirpt is to creates tables in the 'silver' schema, if any tables present with the name then it will drop those tables and creates a new table. 

==========================================================
*/

IF OBJECT_ID('silver.crm_customer_info','U') IS NOT NULL 
	DROP TABLE silver.crm_customer_info;
GO
CREATE TABLE silver.crm_customer_info(
	customer_id INT,
	customer_key NVARCHAR(50),
	customer_firstname NVARCHAR(50),
	customer_lastname NVARCHAR(50),
	customer_marital_status NVARCHAR(50),
	customer_gender NVARCHAR(50), 
	customer_create_date DATE, 
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID('silver.crm_product_info','U') IS NOT NULL 
	DROP TABLE silver.crm_product_info;
GO
CREATE TABLE silver.crm_product_info (
    product_id       INT,
	category_id NVARCHAR(50),
    product_key      NVARCHAR(50),
    product_name       NVARCHAR(50),
	product_cost    INT,
    product_line     NVARCHAR(50),
    product_start_date DATETIME,
    product_end_date   DATETIME, 
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO





IF OBJECT_ID('silver.crm_sales_details','U') IS NOT NULL 
	DROP TABLE silver.crm_sales_details;
GO
CREATE TABLE silver.crm_sales_details (
    sales_order_num  NVARCHAR(50),
    sales_product_key  NVARCHAR(50),
    sales_customer_id  INT,
    sales_order_date DATETIME,
    sales_ship_date DATETIME,
    sales_due_dt   DATETIME,
    sales_sales    INT,
    sales_quantity INT,
    sales_price    INT, 
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO


IF OBJECT_ID('silver.erp_loc_a101','U') IS NOT NULL 
	DROP TABLE silver.erp_loc_a101;
GO
CREATE TABLE silver.erp_loc_a101 (
    cid    NVARCHAR(50),
    country NVARCHAR(50), 
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO


IF OBJECT_ID('silver.erp_cust_az12','U') IS NOT NULL 
	DROP TABLE silver.erp_cust_az12;
GO
CREATE TABLE silver.erp_cust_az12 (
    cid    NVARCHAR(50),
    bdate  DATE,
    gen    NVARCHAR(50), 
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO


IF OBJECT_ID('silver.erp_px_cat_g1v2','U') IS NOT NULL 
	DROP TABLE silver.erp_px_cat_g1v2;
GO
CREATE TABLE silver.erp_px_cat_g1v2 (
    id           NVARCHAR(50),
    cat          NVARCHAR(50),
    subcat       NVARCHAR(50),
    maintenance  NVARCHAR(50), 
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO
