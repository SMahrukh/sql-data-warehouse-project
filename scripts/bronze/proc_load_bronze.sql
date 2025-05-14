/*
==================================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
==================================================================================
Script Purpose:
		This stored procedure loads data into the 'bronze' schema from external csv files.
		It performs the following actions:
		- Turncates the bronze tables before loading the data.
		- uses the 'BULK INTSERT' command to load data from the csv files to bronze tables
Parameters: None
This stored procedure does not accept any parameters or return any values.
Usage Example:
EXEC bronze.load_bronze
==================================================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS 
BEGIN 
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '=================================================';
		PRINT 'Loading Bronze Layer';
		PRINT '=================================================';

		PRINT '-------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '-------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT 'Truncating Table: bronze.crm_cust_info';
		truncate table bronze.crm_cust_info;

		PRINT 'Inserting Data Into: bronze.crm_cust_info'
		BULK INSERT bronze.crm_cust_info FROM 'C:\Mahrukh\Data Analytics\SQL\SQL Data Warehouse Project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
			WITH ( FORMAT = 'CSV',
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				ROWTERMINATOR = '\n',
				TABLOCK
			 );
		SELECT COUNT(*) FROM bronze.crm_cust_info;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration:' +	CAST(DATEDIFF(second, @start_time,@end_time) AS VARCHAR)+ 'seconds';
		PRINT '--------------';

		SET @start_time = GETDATE();
		PRINT 'Truncating Table: bronze.crm_prd_info';
		truncate table bronze.crm_prd_info;
				
		PRINT 'Inserting Data Into: bronze.crm_prd_info';
		bulk insert bronze.crm_prd_info FROM 'C:\Mahrukh\Data Analytics\SQL\SQL Data Warehouse Project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (FORMAT = 'csv',
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '\n',
			TABLOCK
			);
		SELECT COUNT(*) FROM bronze.crm_prd_info;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration:' +	CAST(DATEDIFF(second, @start_time,@end_time) AS VARCHAR) + 'seconds';
		PRINT '--------------';

		SET @start_time = GETDATE();
		PRINT 'Truncating Table: bronze.crm_sales_details';
		truncate table bronze.crm_sales_details;

		PRINT 'Inserting Data Into: bronze.crm_sales_details';
		bulk insert bronze.crm_sales_details FROM 'C:\Mahrukh\Data Analytics\SQL\SQL Data Warehouse Project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (FORMAT = 'csv',
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				ROWTERMINATOR = '\n',
				TABLOCK
			);
		SELECT COUNT(*) FROM bronze.crm_sales_details;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration:' +	CAST(DATEDIFF(second, @start_time,@end_time) AS VARCHAR)+ 'seconds';
		PRINT '-------------------------------------------------';

		PRINT '-------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '-------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT 'Truncating Table: bronze.erp_cust_az12';
		truncate table bronze.erp_cust_az12;

		PRINT 'Inserting Data Into: bronze.erp_cust_az12';
		bulk insert bronze.erp_cust_az12 FROM 'C:\Mahrukh\Data Analytics\SQL\SQL Data Warehouse Project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (FORMAT = 'csv',
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '\n',
			TABLOCK
			);
		SELECT COUNT(*) FROM bronze.erp_cust_az12;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration:' +	CAST(DATEDIFF(second, @start_time,@end_time) AS VARCHAR)+ 'seconds';
		PRINT '-------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT 'Truncating Table: bronze.erp_loc_a101';
		truncate table bronze.erp_loc_a101;

		PRINT 'Inserting Data Into: bronze.erp_loc_a101';
		bulk insert bronze.erp_loc_a101 FROM 'C:\Mahrukh\Data Analytics\SQL\SQL Data Warehouse Project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (FORMAT = 'csv',
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '\n',
			TABLOCK
			);
		SELECT COUNT(*) FROM bronze.erp_loc_a101;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration:' +	CAST(DATEDIFF(second, @start_time,@end_time) AS VARCHAR)+ 'seconds';
		PRINT '-------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT 'Truncating Table: erp_px_cat_g1v2';
		truncate table bronze.erp_px_cat_g1v2;

		PRINT 'Inserting Data Into: bronze.erp_px_cat_g1v2';
		bulk insert bronze.erp_px_cat_g1v2 FROM 'C:\Mahrukh\Data Analytics\SQL\SQL Data Warehouse Project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (FORMAT = 'csv',
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '\n',
			TABLOCK
			);
		SELECT COUNT(*) FROM bronze.erp_px_cat_g1v2;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration:' +	CAST(DATEDIFF(second, @start_time,@end_time) AS VARCHAR)+ 'seconds';
		PRINT '-------------------------------------------------';
	
	SET @batch_end_time = GETDATE();
	PRINT '>> Batch Load Duration:' +	CAST(DATEDIFF(second, @batch_start_time,@batch_end_time) AS VARCHAR)+ 'seconds';

	END TRY
	BEGIN CATCH
		PRINT '=================================================================';
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '================================================================='
	END CATCH
END;
