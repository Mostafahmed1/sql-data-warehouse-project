Create View Gold.dim_customers as 
select 
	ROW_NUMBER() over ( order by ci.cst_id) as customer_key ,
	ci.cst_id as customer_id	,
	ci.cst_key	as customer_number,
	ci.cst_firstname as first_name,
	ci.cst_lastname	AS last_name,
	la.cntry as country,
	ci.cst_marital_status	as marital_status,
	case 
		when ci.cst_gndr != 'n/a' then ci.cst_gndr 
	else coalesce( ca.gen,'n/a')
	end as gender,
	ca.bdate as birthdate,
	ci.cst_create_date	as create_date
from Silver.crm_cust_info ci left join Silver.erp_cust_az12 ca
on ci.cst_key = ca.cid
left join Silver.erp_loc_a101 la
on ci.cst_key = la.cid




