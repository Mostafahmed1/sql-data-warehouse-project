create view Gold.dim_products as
select  
	ROW_NUMBER() over (order by crm.prd_start_dt,crm.prd_key) as product_key,
	crm.prd_id as product_id,
	
	crm.prd_key	as product_number,
	crm.prd_nm	As product_name,
	crm.cat_id as category_id,
	erp.subcat	as subcategory,
	erp.cat	as category,
	erp.maintenance	 as maintenance,
	crm.prd_cost as cost,
	crm.prd_line	as product_line,
	crm.prd_start_dt As startdate

from Silver.crm_prd_info crm left join Silver.erp_px_cat_g1v2 erp 
on crm.cat_id = erp.id
where prd_end_dt is NULL





