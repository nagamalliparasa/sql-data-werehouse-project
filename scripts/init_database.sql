/*
===========================================
Create Databases and Schemas
===========================================

Script Purpose: 
  This script create a new database named DataWerehouse after checking that if any database existed with the same name. 
If database exists then we don't delete that database and use the existing one.

*/

USE master;
GO

--Drop and recreate the DataWerehouse database
/*IF EXISTS (SELECT 1 FROM sys.databases WHERE name='Werehouse')
BEGIN
	ALTER DATABASE Werehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE Werehouse;
END 
GO*/

--CREATE the DataWerehouse database
IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name='Werehouse')
BEGIN
    CREATE DATABASE Werehouse;
    PRINT 'Database Werehouse created successfully.';
END
ELSE
BEGIN
    PRINT 'Database Werehouse already exists.';
END;
GO


IF EXISTS (SELECT 1 FROM sys.databases WHERE name='Werehouse')
BEGIN
	PRINT 'Accessing Werehouse Database';
	USE Werehouse;
END 
ELSE
	PRINT 'No Database found';
GO

--CREATE schemas
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
