/*
==============================================================================
Create Database and Schemas
==============================================================================
Script Purpose:
        The script creates new database named 'Datawarehouse' after checking if it exists. If the database exists, it is dropped and recreated. Additionally, the scripts creates three schemas within the database: 'bronze', 'silver' and 'gold'.
*/

USE master;
GO

-- Drop and recreate the 'Datawarehouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'Datawarehouse')
BEGIN
      ALTER DATABASE Datawarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
      DROP DATABASE Datawarehouse;
END;
GO

--Create the 'Datawarehouse' database
CREATE DATABASE Datawarehouse;
GO

USE Datawarehouse;
GO

--Create Schemas
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
