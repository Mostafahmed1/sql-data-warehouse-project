/*
===========================================================
Quality Checks
===========================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy,
    and standardization across the 'silver' schemas. It includes checks for: 
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    -Data consistency between related fields.

Usage Notes:
    - Run these checks after data loading Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.
==========================================================
*/
select *
from Bronze.crm_sales_details
where sls_ord_num is Null  or  sls_prd_key is NULL or sls_cust_id is NULL

-- Convert Zero dates to Null
select NULLif(sls_order_dt,0) sls_prd_key -- equals "as sls_prd_key"
from Bronze.crm_sales_details
where sls_order_dt <=0

-- Checks
-- for Extra spaces
select *
from Bronze.crm_sales_details
where sls_prd_key != trim(sls_prd_key)

-- Change Date from 20101229 (string) to date type
-- with date should't < 8 digits
-- with date in specific Range for example: [2050/01/01] , [1900/01/01] (depends on the Business)

select Nullif(sls_ship_dt,0) as sls_ship_dt  
from Bronze.crm_sales_details
where sls_ship_dt >20500101 
or sls_ship_dt <19000101 
or len(sls_ship_dt ) < 8

--select clause show the 0s as NULLs
--where clause still treat with them as zero they for sure less than 19000101

select Nullif(sls_due_dt,0) as sls_due_dt
from Bronze.crm_sales_details
where sls_due_dt <=0
or len(sls_due_dt)!=8
or sls_due_dt <19000101
or sls_due_dt >20500101

--Check for Invalid Date orders as Order date must be the earliest
select *
from Bronze.crm_sales_details
where sls_order_dt > sls_ship_dt or sls_order_dt > sls_due_dt


-- Check sales 
-- Sales = Qty * Price
-- All of them shouldn't be Neg or Zeros or NULLs
select * 
from Bronze.crm_sales_details
where  sls_quantity <=0 or sls_price <=0 or  sls_quantity is null or sls_price is null--sls_sales <0 or

select *
from Bronze.crm_sales_details
where sls_sales != sls_quantity * sls_price 
or sls_price is NUll or sls_sales is NULL or sls_quantity is NULL 


select t.* , (t.sls_price2 * sls_quantity) as sls_sales2 
from (
	select *,
	case when sls_price < 0 then abs(sls_price)
		 when sls_price is NULL then sls_sales/sls_quantity
		 else sls_price
	end as sls_price2
	from Bronze.crm_sales_details) t



select * 
from Silver.crm_sales_details

