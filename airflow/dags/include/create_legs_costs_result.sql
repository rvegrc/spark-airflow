drop table if exists dev_transerv_cont.legs_costs_result;

create table dev_transerv_cont.legs_costs_result
engine = MergeTree()
order by tuple() as
-- add message about import or transit for  route_service + p_route_edge_conver create table route_result
with 
legs_costs as (
	select 
		l.fk_transp_stages l_fk_transp_stages
		,l.fk_modality l_fk_modality
		,l.distance l_distance
		,l.day l_day
		,l.day_nko_dep l_day_nko_dep
		,l.day_nko_dest l_day_nko_dest
		,ltc.basic ltc_basic
		,ltc.quantity ltc_quantity
		,ltc.active ltc_active
		,ltc.fk_vat_rates ltc_fk_vat_rates
		,pr1.code pr_code_dep
		,pr1.fk_point_types pr_fk_point_types_dep
		,pr1.lon pr_lon_dep
		,pr1.lat pr_lat_dep
		,pr2.code pr_code_dest
		,pr2.fk_point_types pr_fk_point_types_dest
		,pr2.lon pr_lon_dest
		,pr2.lat pr_lat_dest
		,ts.fk_margins ts_fk_margins
		-- ,ts.date_start ts_date_start
		-- ,ts.date_finish ts_date_finish
		,extract(year from ts.date_start) ts_date_start_year
		,extract(month from ts.date_start) ts_date_start_month
		,extract(day from ts.date_start) ts_date_start_day
		,extract(year from ts.date_finish) ts_date_finish_year
		,extract(month from ts.date_finish) ts_date_finish_month
		,extract(day from ts.date_finish) ts_date_finish_day
		,ts.rent_day_cont ts_rent_day_cont
		,ts.rent_day_wag ts_rent_day_wag
		,ts.own_cont ts_own_cont
		,ts.train_cont ts_train_cont
		,ts.cost_weight ts_cost_weight
		,ts.kkv ts_kkv
		,ts.auto ts_auto
		-- ,ts.fk_coefficient ts_fk_coefficient
		,ts.quantity_cont ts_quantity_cont
		-- ,ts.code_char_fk_currencies ts_code_char_fk_currencies
		,ts.return_wag ts_return_wag
		-- for find anomaly after model prediction
		,ts.id ts_id 
		,cr.fk_services_rail cr_fk_services_rail
		,cr.fk_measure_units cr_fk_measure_units
		,cr.fk_currencies cr_fk_currencies
		,ce.fk_counterparty ce_fk_counterparty
		,ce.cost ce_cost -- target cost
	from dev_transerv_cont.legs l
	join dev_transerv_cont.legs_total_costs ltc
	on ltc.fk_legs = l.id
	join dev_transerv_cont.points_rail pr1
	on pr1.id = l.dep_fk_points_rail
	join dev_transerv_cont.points_rail pr2
	on pr2.id = l.dest_fk_points_rail
	join dev_transerv_cont.transport_solution ts 
	on ts.id = l.fk_transport_solution
	join dev_transerv_cont.costs_rail cr
	on cr.id = ltc.fk_costs_rail
	join dev_transerv_cont.costs_expend ce
	on cr.id = ce.fk_costs_rail
	where 
		(ts.id > 3102)
		and (isNaN(pr1.lat) != 1)
		and (isNaN(pr1.lon) != 1)
		and (isNaN(pr2.lat) != 1)
		and (isNaN(pr2.lon) != 1)
)
,cargo_guarded as (
	select
		cgt.weight cgt_weight
		,cgt.fk_cont_type cgt_fk_cont_type
		,cgt.quantity_cont cgt_quantity_cont 
		,cgt.quantity_wag cgt_quantity_wag
		,cgt.fk_transport_solution cgt_fk_transport_solution
		-- ,etn_pid fed_etn_pid
		,ef.code_fk_gng_freights  ef_code_fk_gng_freights
		,if(dpf.rt_id_fk_etsng_freights = ef.rt_id, 1, 0) cargo_guarded
	FROM dev_transerv_cont.cargo_types cgt
	left join dev_transerv_cont.etsng_freights ef 
	on cgt.fk_etsng_freights = ef.id
	left join dev_transerv_cont.dopog_freights dpf
	on dpf.rt_id_fk_etsng_freights = ef.rt_id
)
select 
	lc.*
	,cgd.cgt_weight
	,cgd.cgt_quantity_cont
	,cgd.cgt_quantity_wag
	,cgd.ef_code_fk_gng_freights
	,cgd.cargo_guarded
from legs_costs lc
left join cargo_guarded cgd
on lc.ts_id = cgd.cgt_fk_transport_solution
where 
	(lc.ce_cost > 0) or (lc.ce_cost = 0);


