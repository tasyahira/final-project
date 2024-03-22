def etl_stg_to_dwh_fact(): """
	insert into fact_province_daily 
	select 
		row_number() over(order by cd.id) as id,
		dp.province_id ,
		cd.id as case_id,
		cd.case_date as case_date,
		count(cd.*) as total
	from dim_province dp 
	left join case_details cd on dp.province_id = cd.province_id::int
	group by 2,3,4;

	insert into fact_province_monthly 
	select 
		row_number() over(order by cd.id) as id,
		dp.province_id ,
		cd.id as case_id,
		TO_CHAR(cd.case_date, 'YYYY-MM') AS case_month,
		count(cd.*) as total
	from dim_province dp 
	left join case_details cd on dp.province_id = cd.province_id::int
	group by 2,3,4;


	insert into fact_province_yearly 
	select 
		row_number() over(order by cd.id) as id,
		dp.province_id ,
		cd.id as case_id,
		TO_CHAR(cd.case_date, 'YYYY') AS case_year,
		count(cd.*) as total
	from dim_province dp 
	left join case_details cd on dp.province_id = cd.province_id::int
	group by 2,3,4;

	insert into fact_district_monthly 
	select 
		row_number() over(order by cd.id) as id,
		dd.district_id ,
		cd.id as case_id,
		TO_CHAR(cd.case_date, 'YYYY-MM') AS case_month,
		count(cd.*) as total
	from dim_district dd  
	left join case_details cd on dd.district_id = cd.district_id::int
	group by 2,3,4;

	insert into fact_district_yearly 
	select
		row_number() over(order by cd.id) as id,
		dd.district_id ,
		cd.id as case_id,
		TO_CHAR(cd.case_date, 'YYYY') AS case_year,
		count(cd.*) as total
	from dim_district dd  
	left join case_details cd on dd.district_id = cd.district_id::int
	group by 2,3,4;
"""