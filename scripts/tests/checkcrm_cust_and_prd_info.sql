--         Checks

/*Check crm_cust_info table*/

-- Check for duplicate rows
-- Expectation: No result
select cst_id, count(*) as 'appearance'
from Silver.crm_cust_info
group by cst_id
having count(*)> 1 or cst_id is NULL -- second condition is exsit in case there is only one NULL row

-- Check for unwanted spaces
-- Expectation: No result
select cst_firstname
from Silver.crm_cust_info
where cst_firstname != trim(cst_firstname)

-- Check Standardization & Consistency
-- Expectation: No result
Select Distinct cst_marital_status 
from Silver.crm_cust_info

Select Distinct cst_gndr
from Silver.crm_cust_info

Select * from Bronze.crm_cust_info
Select * from Silver.crm_cust_info

/*Check crm_cust_info table*/
-- check duplicates
select prd_id,count(*)
from Bronze.crm_prd_info
group by prd_id
having count(*) >1 or prd_id is NULL;

--check numeric errors in the numbers fields
select prd_cost
from Bronze.crm_prd_info
where prd_cost <0 or prd_cost is NULL
-- there are 2 nulls values

select distinct prd_line
from Bronze.crm_prd_info

-- end date is earlier than the start date that doesn't make sense
select * 
from Silver.crm_prd_info 
where  prd_end_dt < prd_start_dt


select *
from Silver.crm_prd_info


