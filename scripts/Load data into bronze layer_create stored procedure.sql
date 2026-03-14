/*
---------------------------------------------------------
Load data into Bronze layer
---------------------------------------------------------
Script purpose :
- Load data into Bronze layer from CRM and ERP data sources.
1. Layered truncate table which enable re-run the script without error.
2. Then used bulk insert to load data from csv files into the bronze tables.
   The file path is hard coded in the script, so please make sure to update the file path before running the script.
3. Created the stored procedure named 'load_bronze' which can be easily executed to load data into the bronze tables.
4. Use BEGIN TRY to handle errors and print error message.
----------------------------------------------------------
Author: Yan
*/


CREATE OR ALTER PROCEDURE bronze.load_bronze AS

BEGIN
	BEGIN TRY
		PRINT '---------------------';
		PRINT 'Loading CRM Tables';
		PRINT '---------------------';

		PRINT '>> Truncating Table : bronze.crm_cust_info';
		PRINT '>> Inserting data into Table : bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info; -- Clear the table before loading new data

		BULK INSERT bronze.crm_cust_info
		FROM 'E:\Projects\DWH project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
		);

		PRINT '---------------------';
		PRINT '>> Truncating Table : bronze.crm_prd_info';
		PRINT '>> Inserting data into Table : bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;

		BULK INSERT bronze.crm_prd_info
		FROM 'E:\Projects\DWH project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH(
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
		);

		PRINT '---------------------';
		PRINT '>> Truncating Table : bronze.crm_sales_details';
		PRINT '>> Inserting data into Table : bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;

		BULK INSERT bronze.crm_sales_details
		FROM 'E:\Projects\DWH project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH(
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
		);

		PRINT '---------------------';
		PRINT 'Loading ERP Tables';
		PRINT '---------------------';

	
		PRINT '>> Truncating Table : bronze.erp_cust_az12';
		PRINT '>> Inserting data into Table : bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;

		BULK INSERT bronze.erp_cust_az12
		FROM 'E:\Projects\DWH project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH(
		FIRSTROW = 2,
		FIELDTERMINATOR = ',' ,
		TABLOCK
		);


		PRINT '---------------------';
		PRINT '>> Truncating Table : bronze.erp_loc_a101';
		PRINT '>> Inserting data into Table : bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101

		BULK INSERT bronze.erp_loc_a101
		FROM 'E:\Projects\DWH project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH(
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
		);


		PRINT '---------------------';
		PRINT '>> Truncating Table : bronze.erp_px_cat_g1v2';
		PRINT '>> Inserting data into Table : bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2

		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'E:\Projects\DWH project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH(
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
		);
	END TRY
	BEGIN CATCH
		PRINT ' ---------------------';
		PRINT 'Error occurred while loading data into Bronze layer: ' + ERROR_MESSAGE();
		PRINT '---------------------';
	END CATCH
END

