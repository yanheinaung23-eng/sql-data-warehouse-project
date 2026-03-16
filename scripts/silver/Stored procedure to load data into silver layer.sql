/*
-------------------------------------------------------------------
Stored Procedure : Load Silver Layer ( Bronze to Silver )
-------------------------------------------------------------------
Script purpose :
   This stored procedure performs the ETL ( Extract, Transform, Load ) process to
   populate the 'silver' schema tables from the 'bronze' schema.

   1. Truncates Silver tables
   2. Insert transformed and cleaned data from Bronze into Silver tables
   
Usage : EXEC silver.load_silver;

-------------------------------------------------------------------
Author: Yan
*/


CREATE OR ALTER PROCEDURE silver.load_silver AS

BEGIN

BEGIN TRY

PRINT '---------------------';
PRINT 'Loading CRM Tables';
PRINT '---------------------';

PRINT '>> Truncating table : silver.crm_cust_info';
TRUNCATE TABLE silver.crm_cust_info;
PRINT '>> Truncating Done';
PRINT '>> Inserting Data into: silver.crm_cust_info';
INSERT INTO silver.crm_cust_info(
cst_id,
cst_key,
cst_firstname,
cst_lastname,
cst_martial_status,
cst_gndr,
cst_create_date )

SELECT 
cst_id,
cst_key,
TRIM(cst_firstname) AS cst_firstname,  -- Clean the white spaces for names
TRIM(cst_lastname) AS cst_lastname,
CASE 
WHEN UPPER(TRIM(cst_martial_status)) = 'S' THEN 'Single'  -- Normalize martial status values to readable format
WHEN UPPER(TRIM(cst_martial_status)) = 'M' THEN 'Married'
ELSE 'n/a'
END AS cst_martial_status,
CASE 
WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'  -- Normalized gender status values to readable format
WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
ELSE 'n/a'
END AS cst_gndr,
cst_create_date
FROM
(
SELECT 
cst_id,
cst_key,
cst_firstname,
cst_lastname,
cst_martial_status,
cst_gndr,
cst_create_date
FROM(
SELECT 
cst_id,
cst_key,
cst_firstname,
cst_lastname,
cst_martial_status,
cst_gndr,
cst_create_date,
ROW_NUMBER() OVER ( PARTITION BY cst_id ORDER BY cst_create_date DESC ) AS flag_last  -- Remove the duplicate rows 
FROM bronze.crm_cust_info
WHERE cst_id IS NOT NULL
) t1
WHERE flag_last = 1 ) t2;  --  Select the most recent record for each customer

PRINT '---------------------';
------------------------------------------------------------------------------------------------------------
PRINT '>> Truncating table : silver.crm_prd_info';
TRUNCATE TABLE silver.crm_prd_info;
PRINT '>> Truncating Done';
PRINT '>> Inserting Data into: silver.crm_prd_info';


INSERT INTO silver.crm_prd_info (
prd_id,
cat_id,
prd_key,
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
)

SELECT 
prd_id,
REPLACE(SUBSTRING( prd_key , 1, 5 ),'-','_') AS cat_id, -- Extract the category ID
SUBSTRING( prd_key , 7 , LEN(prd_key) ) AS prd_key, -- Extract the product key
prd_nm,
ISNULL(prd_cost,0) AS prd_cost, -- Replace the null values with zero
CASE UPPER(TRIM(prd_line))
WHEN  'M' THEN 'Moutain'
WHEN  'R' THEN 'Road'
WHEN  'S' THEN 'Other Sales'
WHEN  'T' THEN 'Touring'
ELSE 'n/a'
END AS prd_line, -- Map product line codes to descriptive values
CAST(prd_start_dt AS DATE ) AS prd_start_dt,
CAST(LEAD(prd_start_dt) OVER ( PARTITION BY prd_key ORDER BY prd_start_dt )-1 AS DATE) AS prd_end_dt -- Correct the start and the end date of products
FROM bronze.crm_prd_info;

PRINT '---------------------';
-------------------------------------------------------------------------------------------------------------
PRINT '>> Truncating table : silver.crm_sales_details';
TRUNCATE TABLE silver.crm_sales_details;
PRINT '>> Truncating Done';
PRINT '>> Inserting Data into: silver.crm_sales_details';


INSERT INTO silver.crm_sales_details( 
sls_order_num,
sls_prd_key,
sls_cust_id,
sls_order_dt,
sls_ship_dt,
sls_due_dt,
sls_sales,
sls_quantity,
sls_price
)

SELECT 
sls_order_num,
sls_prd_key,
sls_cust_id,
CASE
	WHEN sls_order_dt <= 0 OR LEN(sls_order_dt) != 8 THEN NULL -- Handle invalid date values by setting them to NULL 
	ELSE CAST(CAST(sls_order_dt AS VARCHAR(50)) AS DATE )      -- Convert the integer date to a proper date format
END AS sls_order_dt,
CASE
	WHEN sls_ship_dt <= 0 OR LEN(sls_ship_dt) != 8 THEN NULL
	ELSE CAST(CAST(sls_ship_dt AS VARCHAR(50)) AS DATE )
END AS sls_ship_dt,
CASE
	WHEN sls_due_dt <= 0 OR LEN(sls_due_dt) != 8 THEN NULL
	ELSE CAST(CAST(sls_due_dt AS VARCHAR(50)) AS DATE )
END AS sls_due_dt,
CASE 
	WHEN sls_sales IS NULL OR sls_sales < 0 OR sls_sales != sls_quantity * ABS(sls_price) THEN -- Handle invalid sales by calculating or replacing negative values with positive
	sls_quantity * ABS(sls_price)                                                                             
	ELSE sls_sales
END AS sls_sales,
sls_quantity,
CASE
WHEN
sls_price IS NULL OR sls_price < 0 THEN sls_sales / NULLIF(sls_quantity,0) -- -- Handle invalid prices by calculating or replacing negative values with positive
ELSE sls_price
END AS sls_price
FROM bronze.crm_sales_details
WHERE sls_cust_id IN (SELECT cst_id FROM silver.crm_cust_info);

PRINT '---------------------';
--------------------------------------------------------------------------------------------------------------
PRINT '---------------------';
PRINT 'Loading ERP Tables';
PRINT '---------------------';

PRINT '>> Truncating table : silver.erp_cust_az12';
TRUNCATE TABLE silver.erp_cust_az12;
PRINT '>> Truncating Done';
PRINT '>> Inserting Data into: silver.erp_cust_az12';

INSERT INTO silver.erp_cust_az12(
cid,
bdate,
gen
)

SELECT
CASE 
	WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) -- Remove the 'NAS' prefix from cid values
	ELSE cid
	END AS cid,
CASE 
	WHEN bdate < '1926-01-01' OR bdate > GETDATE() THEN NULL -- Handle invalid date values by setting them to NULL
	ELSE bdate
	END AS bdate,
CASE 
WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female' -- Normalize the gender values to readable format
WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
ELSE 'n/a'
END AS gen
FROM bronze.erp_cust_az12;

PRINT '---------------------';
--------------------------------------------------------------------------------------------------------------
PRINT '>> Truncating table : silver.erp_loc_a101';
TRUNCATE TABLE silver.erp_loc_a101;
PRINT '>> Truncating Done';
PRINT '>> Inserting Data into: silver.erp_loc_a101';

INSERT INTO silver.erp_loc_a101(
cid,
cntry
)

SELECT
REPLACE(cid, '-','') AS cid, -- Standardized the cid by removing '-'
CASE
WHEN UPPER(TRIM(cntry)) = 'DE' THEN 'Germany'
WHEN UPPER(TRIM(cntry)) IN ('USA','US') THEN 'United States'
WHEN TRIM(cntry) = '' OR TRIM(cntry) IS NULL THEN 'n/a'
ELSE TRIM(cntry)
END AS cntry -- Normalize and handle missing or blank country
FROM bronze.erp_loc_a101;

PRINT '---------------------';
--------------------------------------------------------------------------------------------------------------
PRINT '>> Truncating table : silver.erp_px_cat_g1v2';
TRUNCATE TABLE silver.erp_px_cat_g1v2;
PRINT '>> Truncating Done';
PRINT '>> Inserting Data into: silver.erp_px_cat_g1v2';

INSERT INTO silver.erp_px_cat_g1v2(
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

PRINT '---------------------';

--------------------------------------------------------------------------------------------------------------
END TRY
BEGIN CATCH
		PRINT ' ---------------------';
		PRINT 'Error occurred while loading data into Silver layer: ' + ERROR_MESSAGE();
		PRINT '---------------------';
	END CATCH
END
 
