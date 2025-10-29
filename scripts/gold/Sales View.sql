create view Gold.fact_sales as 
select 
  -- Dims Keys Columns

	-- sa.sls_prd_key,
	prd.product_key ,

	-- sa.sls_cust_id,
	cust.customer_key,
	sa.sls_ord_num as order_number,

  -- Dates 
	sa.sls_order_dt as order_date,
	sa.sls_ship_dt	as ship_date,
	sa.sls_due_dt	as due_date,

  -- Measures
	sa.sls_sales	as sales_amount,
	sa.sls_quantity	as quantity,
	sa.sls_price as price

from Silver.crm_sales_details sa
left join Gold.dim_products prd 
on sa.sls_prd_key =  prd.product_number
left join Gold.dim_customers cust
on sa.sls_cust_id = cust.customer_id 


select *
from Gold.dim_customers 



