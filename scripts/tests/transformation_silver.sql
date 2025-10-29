-- Transformation Phase in Silver Layer

-- Remove dublicates and nulls of a table
-- Subquery idea is dealing with column alias as a column name
-- Remove extra spaces by trim

exec Silver.load_silver 


create or alter procedure Silver.load_silver as
Begin

	PRINT '================================='
	PRINT 'Loading Silver Layer'
	PRINT '================================='
	-- Load CRM csv tables to the existing tables 
	PRINT '---------------------------------'
	Print 'LOADING ERP Tables'
	PRINT '---------------------------------'

	print '>>> Truncating Table: Silver.crm_cust_info';
	Truncate Table Silver.crm_cust_info;
	print '>>> Inserting Data into: Silver.crm_cust_info';
	insert into Silver.crm_cust_info (
		cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_marital_status,
		cst_gndr,
		cst_create_date)

	Select
	cst_id,
	cst_key,
	trim(cst_firstname) as cst_firstname,
	trim(cst_lastname) as cst_lastname,

	case When Upper(trim(cst_marital_status)) ='S' THEN 'Single'
		 When Upper(trim(cst_marital_status)) ='M' THEN 'Married'
		 else 'n/a' 
	end cst_marital_status,

	case When Upper(trim(cst_gndr)) ='M' THEN 'Male'
		 When Upper(trim(cst_gndr)) ='F' THEN 'Female'
		 else 'n/a' 
	end cst_gndr,
	cst_create_date

	from (
	select *, 
	row_number() over( 
			partition by cst_id  order by cst_create_date desc ) as ranking 
			-- order the partition based on cst_create_date

	from Bronze.crm_cust_info )t -- t is just an alias
	where ranking = 1 and cst_id is not NULL -- the second term is exist to remove the first NULL row that didn't removed in the Ranking method

	--**********************************************************************************************************
	-- crm_prd_info Table

	print '>>> Truncating Table: Silver.crm_prd_info';
	Truncate Table Silver.crm_prd_info;
	print '>>> Inserting Data into: Silver.crm_prd_info';

	insert into Silver.crm_prd_info(
		prd_id,
		cat_id,
		prd_key,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt)

	select 
		prd_id,
		Replace(substring(prd_key,1,5),'-','_') As cat_id, -- in the erp data the category like AB_CD not AB-CD
		substring(prd_key,7,len(prd_key)) as prd_key, -- get the other part 
		prd_nm,
		isnull(prd_cost,0) as prd_cost,

		-- mapping the characters known from the business experts 
		case when Upper(trim(prd_line)) = 'R' then 'Road'
			 when Upper(trim(prd_line)) = 'M' then 'Mountain'
			 when Upper(trim(prd_line)) = 'S' then 'Other Sales'
			 when Upper(trim(prd_line)) = 'T' then 'Touring'
			 else 'n/a'
		end as prd_line,

		cast (prd_start_dt as date) as prd_start_dt,
		cast (lead(prd_start_dt) over (partition by prd_key order by prd_start_dt)-1 as date)as prd_end_dt

	from Bronze.crm_prd_info

	--**********************************************************************************************************
	-- Sales Details Table 

	print '>>> Truncating Table: Silver.crm_sales_details';
	Truncate Table Silver.crm_sales_details;
	print '>>> Inserting Data into: Silver.crm_sales_details';

	insert into Silver.crm_sales_details(
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
	)

	select 
	sls_ord_num,
	sls_prd_key	,
	sls_cust_id	,
	--sls_order_dt
	case when sls_order_dt =0 or len(sls_order_dt)!= 8 then NULL
			 else cast (cast(sls_order_dt as varchar) as Date)
	end as sls_order_dt,
	--sls_ship_dt
	case when sls_ship_dt =0 or len(sls_ship_dt)!= 8 then NULL
			 else cast (cast(sls_ship_dt as varchar) as Date)
	end as sls_ship_dt,
	--sls_due_dt
	case when sls_due_dt =0 or len(sls_due_dt)!= 8 then NULL
			 else cast (cast(sls_due_dt as varchar) as Date)
	end as sls_due_dt,
	-- Sales
	case when sls_sales is NULL or sls_sales<=0 or sls_sales != sls_price*sls_quantity 
		 then sls_price *abs(sls_price)   
		 else sls_sales
	end as sls_sales,
	-- Price
	case when sls_price <= 0 or sls_price is NULL then sls_sales/sls_quantity
		 else sls_price
	end as sls_price
	,sls_quantity
	from Bronze.crm_sales_details
/* Old Idea
select t.* , (t.sls_price*t.sls_quantity) as sls_sales
from
(select 
sls_ord_num,
sls_prd_key	,
sls_cust_id	,
--sls_order_dt
case when sls_order_dt =0 or len(sls_order_dt)!= 8 then NULL
		 else cast (cast(sls_order_dt as varchar) as Date)
end as sls_order_dt,
--sls_ship_dt
case when sls_ship_dt =0 or len(sls_ship_dt)!= 8 then NULL
		 else cast (cast(sls_ship_dt as varchar) as Date)
end as sls_ship_dt,
--sls_due_dt
case when sls_due_dt =0 or len(sls_due_dt)!= 8 then NULL
		 else cast (cast(sls_due_dt as varchar) as Date)
end as sls_due_dt,
-- Price
case when sls_price < 0 then abs(sls_price)
	 when sls_price is NULL then sls_sales/sls_quantity
	 else sls_price
	end as sls_price

-- Sales (Sales need to be in outer query becasue it's depened on price
,sls_quantity

from Bronze.crm_sales_details
) t 
*/

	-- ***********************************************************************************************
	-- erp_cust_az12
	print '>>> Truncating Table: Silver.erp_cust_az12';
	Truncate Table Silver.erp_cust_az12;
	print '>>> Inserting Data into: Silver.erp_cust_az12';

	insert into Silver.erp_cust_az12 (cid,bdate,gen)
	select 
	case when cid like '%NASA%' then substring(cid,4,len(cid))
		else cid 
	end cid
	,
	case when bdate > getdate() then NULL
		else bdate
	end as bdate 
	,
	-- Gender values [F,M,Female,Male, ,NULL]
	case when upper(trim(gen)) in ('F','FEMALE') then 'Female'
		 when upper(trim(gen)) in ('M','MALE') then 'Male'
		 else 'n/a'
	end as gen
	from Bronze.erp_cust_az12
	order by bdate

	-- ***********************************************************************************************************
	-- erp_loc_a101 table
	print '>>> Truncating Table: Silver.erp_loc_a101';
	Truncate Table Silver.erp_loc_a101;
	print '>>> Inserting Data into: Silver.erp_loc_a101';
	insert into silver.erp_loc_a101 (cid,cntry)
	select Replace(cid,'-','') as cid
	,
	case when Upper(trim(cntry)) in ('US','USA','UNITED STATES') THEN 'United States'
		 when Upper(trim(cntry)) in ('GERMANY','DE') THEN 'Germany'
		 when trim(cntry) is NULL or trim(cntry)= '' THEN 'n/a'
		 ELSE trim(cntry)
	end as cntry
	from Bronze.erp_loc_a101
	-- ******************************************************************************************************
	-- erp_px_cat_g1v2 table
	print '>>> Truncating Table: Silver.erp_px_cat_g1v2';
	Truncate Table Silver.erp_px_cat_g1v2;
	print '>>> Inserting Data into: Silver.erp_px_cat_g1v2';
	insert into Silver.erp_px_cat_g1v2 (id,cat,	subcat,	maintenance)
	select *
	from Bronze.erp_px_cat_g1v2 
end





--$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$

-- Extra check
-- Ensure that joins will sucess
/*select distinct cst_key  
from Silver.crm_cust_info
where cst_key not in
(select CONCAT(substring(cid,1,2),substring(cid,4,len(cid))) as cid2
from Bronze.erp_loc_a101
)*/

--