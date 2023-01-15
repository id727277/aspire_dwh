/*
 DAILY STATS REPORT

 */



drop view if exists reports_daily_pivot_v;
create view if not exists reports_daily_pivot_v as
with recursive dates(date, month) as (values ('2022-01-01', '2022-01')
                                    union all
                                    select date(date, '+1 day'), substr(date(date, '+1 day'), 1, 7)
                                    from dates
                                    where date < date(datetime(), 'start of month', '+1 month', '-1 day')),

rso_total as (select substr(fr.request_number, 1, 3) as client
                        , substr(fr.added_dt, 0, 11)      as dt
                        , count(fr.spr_rso_id)            as rso
                   from fct_request fr
                   where fr.spr_rso_id is not null
                   group by substr(fr.request_number, 1, 3)
                          , substr(fr.added_dt, 0, 11)),

     rso_util as (select substr(fr.request_number, 1, 3) as client
                       , substr(fr.added_dt, 0, 11)      as dt
                       , count(fr.spr_rso_id)            as util_rso
                  from fct_request fr
                           join spr_client sc on substr(fr.request_number, 1, 3) = sc.client
                  where fr.spr_rso_id is not null
                    and sc.is_util = 1
                  group by substr(fr.request_number, 1, 3)
                         , substr(fr.added_dt, 0, 11)),

     act as (select substr(fr.request_number, 1, 3) as client
                  , substr(fr.added_dt, 0, 11)      as dt
                  , count(fr.spr_act_id)            as act
             from fct_request fr
             where fr.spr_act_id = 2 -- 'Yes'
             group by substr(fr.request_number, 1, 3)
                    , substr(fr.added_dt, 0, 11)),

     dp as (select substr(fr.request_number, 1, 3) as client
                 , substr(fr.added_dt, 0, 11)      as dt
                 , count(fr.spr_act_id)            as dp
            from fct_request fr
            where fr.spr_act_id = 3 -- 'Free Dragon Pass'
            group by substr(fr.request_number, 1, 3)
                   , substr(fr.added_dt, 0, 11)),

     cm as (select substr(fr.request_number, 1, 3) as client
                 , substr(fr.added_dt, 0, 11)      as dt
                 , count(id)                       as cm
            from fct_request fr
            where (fr.spr_act_id is null
                or fr.spr_act_id not in (2, 3))
              and fr.spr_rso_id is null
            group by substr(fr.request_number, 1, 3)
                   , substr(fr.added_dt, 0, 11)),

    combined as (select d.date
                      , cm.client
                      , cm.cm
                      , case
                            when rt.rso is null then 0
                            else rt.rso end      as rso
                      , case
                            when ru.util_rso is null then 0
                            else ru.util_rso end as util_rso
                      , case
                            when act.act is null then 0
                            else act.act end     as act
                      , case
                            when dp.dp is null then 0
                            else dp.dp end       as dp

                 from dates d
                          left join cm on d.date = cm.dt
                          left join rso_total rt on d.date = rt.dt and cm.client = rt.client
                          left join rso_util ru on d.date = ru.dt and cm.client = ru.client
                          left join act on d.date = act.dt and cm.client = act.client
                          left join dp on d.date = dp.dt and cm.client = dp.client
                 where cm.client is not null),

    target as (select dd.date,
                      dd.month,
                      srp.client,
                      srp.target as target_monthly
               from dates dd
                        cross join stg_rso_plan srp
               where dd.date >= effective_from
                 and dd.date < effective_to),

    base as (select  d.date,
                     d.month,
                     c.client,
                     c.cm,
                     c.rso,
                     c.util_rso,
                     c.act,
                     c.dp,
                     c.cm+c.rso+c.act as cm_rso_act,
                     t.target_monthly
              from dates d
                       left join combined c on d.date = c.date
                       left join target t on c.date = t.date and c.client = t.client
              where c.client is not null
              order by d.date, c.client),
     total as (select b.date,
                     b.month,
                     b.client,
                     b.cm,
                     b.rso,
                     b.util_rso,
                     b.act,
                     b.dp,
                     b.cm+b.rso+b.act as cm_rso_act,
                     b.target_monthly
                    , sum(b.cm + b.rso + b.act) over w as total
                    , sum(b.cm + b.rso + b.act + b.dp) over w as total_with_dp
               from base b
                   window w as (
                       partition by b.client
                           , b.month
                       rows between unbounded preceding and unbounded following
                       )),
     prev as (select b.date,
                     b.month,
                     b.client,
                     b.cm,
                     b.rso,
                     b.util_rso,
                     b.act,
                     b.dp,
                     b.cm_rso_act,
                     b.target_monthly,
                     b.total,
                     b.total_with_dp,
                     first_value(total) over w                                                         as prev_month_total,
                     first_value(total_with_dp) over w                                                         as prev_month_total_with_dp,
                     julianday(date, 'start of month') -
                     julianday(date, 'start of month', '-1 month')                                     as days_in_prev_month,
                     julianday(date, 'start of month', '+1 month') -
                     julianday(date, 'start of month')                                                 as days_in_curr_month
              from total b
                  window w as (
                      partition by client
                      order by month groups between 1 preceding and current row
    )),
     calcs as (select b.date,
                     b.month,
                     b.client,
                     b.cm,
                     b.rso,
                     b.util_rso,
                     b.act,
                     b.dp,
                     b.cm_rso_act,
                     b.target_monthly,
                     b.total,
                     b.total_with_dp,
                     b.prev_month_total,
                     b.prev_month_total_with_dp,
                     b.days_in_prev_month,
                     b.days_in_curr_month,
                      b.prev_month_total * 1.0 / b.days_in_prev_month * 30 as forecast_30_days
               from prev b)
select c.date,
                     c.month,
                     c.client,
                     c.cm,
                     c.rso,
                     c.util_rso,
                     c.act,
                     c.dp,
                     c.cm_rso_act,
                     c.target_monthly,
                     c.total,
                     c.total_with_dp,
                     c.prev_month_total,
                     c.prev_month_total_with_dp,
                     c.days_in_prev_month,
                     c.days_in_curr_month,
                     c.forecast_30_days,
       target_monthly - forecast_30_days as plan_rso,
       (target_monthly - forecast_30_days) / target_monthly as percentage,
        ((target_monthly - forecast_30_days) / target_monthly) / days_in_curr_month as plan_rso_daily,
        substr(month,6,2) as month_num
from calcs c
;



/* Таблица  СВОД*/
select
    v.client,
    sum(v.cm) as cm,
    sum(v.rso) as rso,
    sum(v.act) as act,
    max(v.total) as total,
    sum(v.dp) as dp,
--     max(v.total_with_dp)as total_with_dp,
    round((max(total) * 1.0 / cast(substr(date('now', '-1 day'), 9,2) as integer)) * v.days_in_curr_month) as forecast_without_dp,
    v.target_monthly
from reports_daily_pivot_v v
where date between '2022-07-01' and '2022-08-01'
-- where date between date('now', '-1 day', 'start of month') and date('now', '-1 day', 'start of month', '+1 month', '-1 day')
group by v.client, v.target_monthly
;


/******  Таблица DAILY STAT ****** */

select
    v.date,
    sum(v.cm) + sum(v.rso) + sum(v.act) + sum(v.dp) as total_with_dp,
    sum(v.cm) as cm,
    sum(v.rso) as rso,
    sum(v.act) as act,
    sum(v.dp) as dp,
    sum(v.rso * 1.0/ v.total_with_dp) as "%RSO from total",
    sum(v.act * 1.0/ v.total_with_dp) as "%ACT from total",
    sum(v.dp * 1.0/ v.total_with_dp) as "%DP from total"
from reports_daily_pivot_v v
where v.date between '2022-07-01' and '2022-08-01'
-- where date between date('now', '-1 day', 'start of month') and date('now', '-1 day', 'start of month', '+1 month', '-1 day')
group by v.date
;



/********* ПФ утилизация + ПФ RSO *********/

with base as (select v.client,
                     v.target_monthly            as plan,
                     sum(v.cm_rso_act)           as fact,
                     sum(v.rso+v.act)            as fact_rso_act,
                     round((max(total) * 1.0 / cast(substr(date('now', '-1 day'), 9, 2) as integer)) *
                           v.days_in_curr_month) as forecast_without_dp
              from reports_daily_pivot_v v
                       join spr_client sc on v.client = sc.client and sc.is_util = 1
              where v.date between '2022-07-01' and '2022-08-01'
--               where date between date('now', '-1 day', 'start of month') and date('now', '-1 day', 'start of month', '+1 month', '-1 day')

              group by v.client, v.target_monthly)
select b.client,
       b.plan,
       b.fact,
       b.forecast_without_dp,
       b.fact * 1.0 / b.plan - 1                                                                   as pf_diff,
       case
           when b.plan - b.fact < 0 then 0
           else (b.plan - b.fact) * 1.0 / cast(substr(date('now', '-1 day'), 9, 2) as integer) end as need_cases_per_day,
        b.plan - b.forecast_without_dp as plan_rso,
        b.fact_rso_act as fact_rso,
        b.fact_rso_act * 1.0 / (b.plan - b.forecast_without_dp) as pf_rso_diff,
        case
           when (b.plan - b.forecast_without_dp) - b.fact_rso_act < 0 then 0
           else ((b.plan - b.forecast_without_dp) - b.fact_rso_act) * 1.0 / cast(substr(date('now', '-1 day'), 9, 2) as integer) end as need_rso_per_day
from base b
;
