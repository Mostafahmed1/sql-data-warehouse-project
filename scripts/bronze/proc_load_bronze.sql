/* 
==================================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
==================================================================================
Script purpose:  
	This Procedure is created to truncate and load data tables as a bulk insert process automatically
	to the bronze schema.
Parameters:
	None.
How to call it:
	exec Bronze.load_bronze

With separated lines and calculated time for each inserted table
===================================================================================
*/
create or alter PROCEDURE Bronze.load_bronze as
Begin
	
	declare @start_time DATETIME, @end_time DATETIME,@batch_start_time DATETIME, @batch_end_time DATETIME;
	set @batch_start_time = GETDATE();
	PRINT '================================='
	PRINT 'Loading Bronze Layer'
	PRINT '================================='
	-- Load CRM csv tables to the existing tables 
	PRINT '---------------------------------'
	Print 'LOADING CRM Tables'
	PRINT '---------------------------------'


	set @start_time = GETDATE();
	Truncate table Bronze.crm_cust_info;

	bulk insert Bronze.crm_cust_info
	from 'D:\Data Analysis\Data Warehouse\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
	with (
		firstrow=2,
		fieldterminator=',',
		tablock
	);
	set @end_time = GETDATE();
	PRINT '>> Load Duration: ' + CAST( DATEDiff(second,@start_time,@end_time) as nvarchar) + 'seconds' ; -- CAST(... AS nvarchar): Converts the numeric difference to a string so it can be concatenated.
	
	
	PRINT '----------------------------------------------'
	set @start_time = GETDATE();
	Truncate table Bronze.crm_prd_info;

	bulk insert Bronze.crm_prd_info
	from 'D:\Data Analysis\Data Warehouse\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
	with (
		firstrow=2,
		fieldterminator=',',
		tablock
	);
	set @end_time = GETDATE();
	PRINT '>> Load Duration: ' + CAST( DATEDiff(second,@start_time,@end_time) as nvarchar) + 'seconds' ;


	set @start_time = GETDATE();
	Truncate table Bronze.crm_sales_details;

	bulk insert Bronze.crm_sales_details
	from 'D:\Data Analysis\Data Warehouse\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
	with (
		firstrow=2,
		fieldterminator=',',
		tablock
	);

	set @end_time = GETDATE();
	PRINT '>> Load Duration: ' + CAST( DATEDiff(second,@start_time,@end_time) as nvarchar) + 'seconds' ;


	PRINT '---------------------------------';
	Print 'LOADING ERP Tables';
	PRINT '---------------------------------';
	--Load ERP csv tables

	set @start_time = GETDATE();
	Truncate table Bronze.erp_cust_az12;

	bulk insert Bronze.erp_cust_az12
	from 'D:\Data Analysis\Data Warehouse\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
	with (
		firstrow=2,
		fieldterminator=',',
		tablock
	);
	set @end_time = GETDATE();
	PRINT '>> Load Duration: ' + CAST( DATEDiff(second,@start_time,@end_time) as nvarchar) + 'seconds' ;


	PRINT '=======================================';
	set @start_time = GETDATE();

	Truncate table Bronze.erp_loc_a101;

	bulk insert Bronze.erp_loc_a101
	from 'D:\Data Analysis\Data Warehouse\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
	with (
		firstrow=2,
		fieldterminator=',',
		tablock
	);
	set @end_time = GETDATE();
	PRINT '>> Load Duration: ' + CAST( DATEDiff(second,@start_time,@end_time) as nvarchar) + 'seconds' ;



	PRINT '==========================================';

	set @start_time = GETDATE();
	Truncate table Bronze.erp_px_cat_g1v2;

	bulk insert Bronze.erp_px_cat_g1v2
	from 'D:\Data Analysis\Data Warehouse\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
	with (
		firstrow=2,
		fieldterminator=',',
		tablock
	); 

	set @end_time = GETDATE();
	PRINT '>> Load Duration: ' + CAST( DATEDiff(second,@start_time,@end_time) as nvarchar) + 'seconds' ;

	PRINT '===================================='
	set @batch_end_time = GETDATE();
	PRINT '**********************************************'
	PRINT 'Loading Bronze Layer is Completed'
	print '--Total Layer duration ' + cast(datediff(second,@batch_start_time,@batch_end_time) as nvarchar) + 'seconds' ;
	PRINT '**********************************************'
END
