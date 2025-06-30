/*
===========================================
Create Databases and Schemas
===========================================

Script Purpose: 
  This script create a new database named DataWerehouse after checking that if any database existed with the same name. 
If database exists then delete that database and create a new database with respective schemas. 

*/


USE master;
GO

--Drop and recreate the DataWerehouse database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name='DataWerehouse')
BEGIN
	ALTER DATABASE DataWerehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWerehouse;
END 
GO

--CREATE the DataWerehouse database
CREATE DATABASE DataWerehouse;
GO

USE DataWerehouse;
GO

--CREATE schemas
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
