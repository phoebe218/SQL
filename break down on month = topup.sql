--usage status by month--
with a as (
select shopid, grass_date
, rank() OVER (PARTITION BY shopid order by grass_date desc) AS rank_1
, rank() OVER (PARTITION BY shopid order by grass_date desc) -1 AS rank_2
from (select distinct shopid, grass_date from paid_ads_mart__fact_credit_tab)
)

, return as (
select a.shopid, a.grass_date org_date, b.grass_date
, case when date_diff('day', b.grass_date, a.grass_date) > 30 then 1 else 0 end is_return
from a 
left join a b on a.rank_1 = b.rank_2 and a.shopid = b.shopid
-- where a.shopid = 94265211 
)

, final as (
select shopid, date_trunc('month', org_date) month, sum(is_return) is_return
from return
group by 1,2
)

, first_user as (
select shopid, date_trunc('month', first_user) month_first_user
from (
    select shopid, min(grass_date) first_user
    from paid_ads_mart__fact_credit_tab
    group by 1
)
-- where shopid = 94265211
group by 1,2
)

select pa.month
, case
    when fu.month_first_user = pa.month then 'New User'
    when fu.month_first_user < pa.month and final.is_return = 1 then 'Returned User' else 'Active User' end seller_type
, count(distinct pa.shopid) total_user
from (select distinct shopid, date_trunc('month', grass_date) month from paid_ads_mart__fact_credit_tab) pa
left join first_user fu on fu.shopid = pa.shopid 
left join final on final.shopid = pa.shopid and pa.month = final.month
where pa.month between current_date - interval '9' month and current_date 
group by 1,2
order by 1 desc