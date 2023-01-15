/*

loading SPR and FACT tables of the DWH

 */

-- spr_client
with cte as (select substr(oc.global_case_number, 4, 3)                                                               as client,
                    case
                        when oc.client is null then substr(oc.global_case_number, 4, 3)
                        else oc.client end                                                                            as client_name,
                    case
                        when substr(oc.global_case_number, 4, 3) in
                             ('BLB', 'CIP', 'HBC', 'HPB', 'KKB', 'PSP', 'RBA', 'RPB', 'SBN', 'SBP', 'SDB', 'SGS', 'UBK',
                              'VSM', 'YPO') then 1
                        else 0 end                                                                                    as is_util,
                    case
                        when substr(oc.global_case_number, 4, 3) in ('SDB', 'ICC', 'CBM') or
                             lower(oc.client) like '%inactive%' then 0
                        else 1 end                                                                                    as is_active,
                    row_number() over (partition by substr(oc.global_case_number, 4, 3) order by effective_from desc) as rn
             from ods_case oc)

insert
into spr_client
select null as id,
       client,
       client_name,
       is_util,
       is_active
from cte
where rn = 1
on conflict (client) do update
    set client_name = excluded.client_name,
        is_util     = excluded.is_util,
        is_active   = excluded.is_active
;


-- spr_contact_method
with cte as (select oc.contact_method,
                    row_number() over (partition by contact_method order by effective_from desc) as rn
             from ods_case oc)
insert
into spr_contact_method
select null           as id,
       contact_method as method
from cte
where rn = 1
on conflict (method) do nothing;


-- spr_contact_source
with cte as (select oc.contact_source,
                    row_number() over (partition by contact_source order by effective_from desc) as rn
             from ods_case oc)
insert
into spr_contact_source
select null           as id,
       contact_source as source
from cte
where rn = 1
  and source is not null
on conflict (source) do nothing;


-- spr_agent
with gather as (select orc.initial_agent as account
                from ods_case orc
                union
                select orc.responsible_agent as account
                from ods_case orc
                union
                select orr.assigned_to_agent as account
                from ods_request orr
                union
                select orr.issue_added_by_user_code as account
                from ods_request orr
                union
                select ora.added_by as account
                from ods_address ora
                union
                select ora.changed_by as account
                from ods_address ora),
     get_names as (select distinct account
                                 , substr(g.account, 1, instr(g.account, '.') - 1)                 as first_name
                                 , substr(g.account, instr(g.account, '.') + 1, length(g.account)) as last_name
                   from gather g),
     get_full as (select account
                       , upper(substr(gn.first_name, 1, 1)) ||
                         substr(gn.first_name, 2, length(gn.first_name) - 1) as first_name
                       , upper(substr(gn.last_name, 1, 1)) ||
                         substr(gn.last_name, 2, length(gn.last_name) - 1)   as last_name
                  from get_names gn),
     base as (select account                                     as username,
                     case
                         when first_name = '' then last_name
                         else first_name || ' ' || last_name end as fullname
              from get_full)
insert
into spr_agent
select null as id,
       b.username,
       b.fullname
from base b
where true
on conflict (username) do update
    set fullname = excluded.fullname
;


-- spr_customer_type
insert into spr_customer_type
select null         as id,
       address_type as type
from ods_address
where true
on conflict (type) do nothing
;


-- spr_customer


with gather as (select oa.address_id           as address_id,
                       oa.surname              as last_name,
                       oa.first_name           as first_name,
                       oa.middle_name          as middle_name,
                       oa.preferred_name       as preferred_name,
                       oa.dob                  as dob,
                       oa.address_date_added   as added_dt,
                       oa.address_date_changed as changed_dt,
                       oa.last_contact_dt      as last_contacted_dt,
                       oa.address_type,
                       oa.client_id,
                       oa.added_by,
                       oa.changed_by,
                       oa.client_address_id
                from ods_address oa
                where oa.is_active = 1),
     base as (select g.address_id,
                     g.last_name,
                     g.first_name,
                     g.middle_name,
                     g.preferred_name,
                     g.dob,
                     g.added_dt,
                     g.changed_dt,
                     g.last_contacted_dt,
                     sc.id   as spr_client_id,
                     sct.id  as spr_customer_type_id,
                     sco.id  as added_by_id,
                     sco2.id as changed_by_id,
                     g.client_address_id
              from gather g
                       join spr_client sc on g.client_id = sc.client
                       join spr_customer_type sct on g.address_type = sct.type
                       join spr_agent sco on g.added_by = sco.username
                       join spr_agent sco2 on g.changed_by = sco2.username)
insert
into spr_customer
select null as id,
       b.address_id,
       b.spr_client_id,
       b.spr_customer_type_id,
       b.last_name,
       b.first_name,
       b.middle_name,
       b.preferred_name,
       b.dob,
       b.added_by_id,
       b.changed_by_id,
       b.added_dt,
       b.changed_dt,
       b.last_contacted_dt
from base b
where true
on conflict (spr_client_id, address_id) do update
    set spr_customer_type_id = excluded.spr_customer_type_id,
        last_name            = excluded.last_name,
        first_name           = excluded.first_name,
        middle_name          = excluded.middle_name,
        preferred_name       = excluded.preferred_name,
        dob                  = excluded.dob,
        changed_by_id        = excluded.changed_by_id,
        added_dt             = excluded.added_dt,
        changed_dt           = excluded.changed_dt,
        last_contacted_dt    = excluded.last_contacted_dt
;


-- spr_phone
with customer_address as (select sc.id as spr_customer_id,
                                 sc.address_id,
                                 sc2.client
                          from spr_customer sc
                                   join spr_client sc2 on sc.spr_client_id = sc2.id),
     base as (select distinct case
                                  when oa.phone_type is null then 'Mobile'
                                  else oa.phone_type end as type,
                              oa.phone                   as phone,
                              ca.spr_customer_id
              from ods_address oa
                       join customer_address ca on oa.address_id = ca.address_id and oa.client_id = ca.client)
insert
into spr_phone
select null as id,
       type,
       phone,
       spr_customer_id
from base
where phone is not null
  and phone != 'tba'
on conflict (type, phone, spr_customer_id) do nothing;


-- spr_email
with emails as (select oa.address_id,
                       oa.client_id,
                       'Personal'    as type,
                       oa.pers_email as email
                from ods_address oa
                union all
                select oa.address_id,
                       oa.client_id,
                       'Working'     as type,
                       oa.work_email as email
                from ods_address oa),
     customer_address as (select sc.id as spr_customer_id,
                                 sc.address_id,
                                 sc2.client
                          from spr_customer sc
                                   join spr_client sc2 on sc.spr_client_id = sc2.id),
     base as (select distinct ca.spr_customer_id,
                              e.type,
                              e.email
              from customer_address ca
                       join emails e on ca.address_id = e.address_id and ca.client = e.client_id
              where email is not null
                and email != 'tba')

insert
into spr_email
select null as id,
       spr_customer_id,
       type,
       email
from base
where true
on conflict (type, email, spr_customer_id) do nothing
;


-- spr_status
insert
into spr_status
select distinct null as id,
                oc.case_status
from ods_case oc
where case_status is not null
on conflict (status) do nothing;


-- spr_service_type
insert into spr_service_type
select distinct null           as id,
                r.service_type as type
from ods_request r
where true
on conflict (type) do nothing;


-- spr_request_type
insert into spr_request_type
select distinct null           as id,
                r.request_type as type
from ods_request r
where true
on conflict (type) do nothing;


-- spr_request_subtype
insert into spr_request_subtype
select distinct null              as id,
                r.request_subtype as subtype
from ods_request r
where request_subtype is not null
on conflict (subtype) do nothing;


-- spr_act
insert into spr_act
select distinct null  as id,
                r.act as act_type
from ods_request r
where r.act is not null
on conflict (act_type) do nothing;


-- spr_request_active
insert into spr_request_active
select distinct null                           as id,
                r.request_active_or_not_active as active_type
from ods_request r
where r.request_active_or_not_active is not null
on conflict (active_type) do nothing
;


-- spr_popular_provider
with gather as (select r.popular_service_provider as provider
                from ods_request r

                union all

                select r2.popular_booking_channel as provider
                from ods_request r2)
insert
into spr_popular_provider
select distinct null as id,
                g.provider
from gather g
where g.provider is not null
on conflict (provider) do nothing;


-- spr_fulfillment_outcome
insert into spr_fulfillment_outcome
select distinct null                  as id,
                r.fulfillment_outcome as outcome
from ods_request r
where r.fulfillment_outcome is not null
on conflict (outcome) do nothing;


-- spr_fulfillment_method
insert into spr_fulfillment_method
select distinct null                 as id,
                r.fulfillment_method as method
from ods_request r
where r.fulfillment_method is not null
on conflict (method) do nothing;


-- spr_currency

with gather as (select sc.code,
                       sc.name_en as eng_name,
                       sc.name_ru as rus_name
                from stg_currency sc
                union
                select 'BYR'               as code,
                       'Belarusian ruble'  as eng_name,
                       'Белорусский рубль' as rus_name)
insert
into spr_currency
select null as id,
       g.code,
       g.eng_name,
       g.rus_name
from gather g
where true
on conflict (code) do nothing;


with gather as (select r.commission_currency as code
                from ods_request r

                union

                select r.reporting_currency as code
                from ods_request r)
insert
into spr_currency
select distinct null as id,
                g.code,
                null as eng_name,
                null as rus_name
from gather g
where code is not null
on conflict (code) do nothing;


-- spr_card_brand
insert into spr_card_brand
select distinct null                as id,
                r.credit_card_brand as card_type
from ods_request r
where r.credit_card_brand is not null
on conflict (card_type) do nothing;


-- spr_iata
insert into spr_iata
select distinct null            as id,
                r.iata_provided as provided
from ods_request r
where r.iata_provided is not null
on conflict (provided) do nothing;


-- spr_country


insert into spr_country
select distinct null            as id,
                r.event_country as code,
                null            as name_eng,
                null            as name_rus
from ods_request r
where r.event_country is not null
on conflict (code) do nothing;


with manual as (select 'OTH' as code,
                       null  as name_eng,
                       null  as name_rus)
insert
into spr_country
select null as id,
       m.code,
       m.name_eng,
       m.name_rus
from manual m
union
select null                       as id,
       substr(r.event_city, 1, 3) as code,
       null                       as name_eng,
       null                       as name_rus
from ods_request r
where r.event_city is not null
on conflict (code) do nothing;


-- spr_city
with cities as (select distinct case
                                    when r.event_city = 'OTHER' then 'OTH'
                                    else substr(r.event_city, 1, 3) end                    as country,
                                case
                                    when r.event_city = 'OTHER' then 'OTHER'
                                    else substr(r.event_city, 5, length(r.event_city)) end as city
                from ods_request r
                where r.event_city is not null)
insert
into spr_city
select null   as id,
       c.city as name_eng,
       null   as name_rus,
       sc.id  as spr_country_id
from cities c
         join spr_country sc on c.country = sc.code
where true
on conflict (name_eng, spr_country_id) do nothing;


/*
FACT tables
*/

-- fct_request
with base as (select r.issue_date_added,
                     r.booking_channel,
                     r.issue_date_changed,
                     r.commission_currency,
                     r.commission_charged,
                     r.confirmation_num,
                     r.closed_issue_date,
                     r.event_city_other,
                     r.event_city,
                     r.event_start_date_time,
                     r.event_end_date_time,
                     r.follow_up_method,
                     r.hotel_chain,
                     r.nights,
                     r.customer_promise_date,
                     r.reporting_amount,
                     r.actual_response_date,
                     r.request_details,
                     r.global_request_number,
                     r.service_provider,
                     r.situation,
                     r.act,
                     r.request_active_or_not_active,
                     r.issue_added_by_user_code,
                     r.assigned_to_agent,
                     r.credit_card_brand,
                     r.commission_currency,
                     r.reporting_currency,
                     r.event_city,
                     r.iata_provided,
                     r.fulfillment_method,
                     r.fulfillment_outcome,
                     r.popular_booking_channel,
                     r.popular_service_provider,
                     r.request_type,
                     r.service_type,
                     r.request_subtype,
                     r.rso_uptake,
                     r.issue_status,
                     r.stars,
                     r.vendor_contact
              from ods_request r
              where r.is_active = 1
                and r.request_type not like 'O %')
insert
into fct_request
select null                                                                as id,
       b.issue_date_added                                                  as added_dt,
       b.booking_channel,
       b.issue_date_changed                                                as changed_dt,
       b.commission_charged,
       b.confirmation_num                                                  as confirmation_number,
       b.closed_issue_date                                                 as closed_dt,
       b.event_city_other,
       b.event_end_date_time                                               as event_end_dt,
       b.event_start_date_time                                             as event_start_dt,
       b.follow_up_method                                                  as followup_method,
       b.hotel_chain,
       b.nights,
       b.customer_promise_date                                             as promised_dt,
       b.reporting_amount,
       b.actual_response_date                                              as response_dt,
       b.request_details,
       substr(b.global_request_number, 4, length(b.global_request_number)) as request_number,
       b.service_provider,
       b.situation,
       a.id                                                                as spr_act_id,
       ra.id                                                               as spr_active_id,
       c.id                                                                as spr_added_by_id,
       c2.id                                                               as spr_assigned_to_id,
       cb.id                                                               as spr_card_brand_id,
       sc.id                                                               as spr_commission_currency_id,
       sc2.id                                                              as spr_currency_id,
       ci.id                                                               as spr_event_city_id,
       i.id                                                                as spr_iata_provided_id,
       fo.id                                                               as spr_fulfillment_outcome_id,
       fm.id                                                               as spr_fulfillment_method_id,
       pp.id                                                               as spr_popular_provider_id,
       pp2.id                                                              as spr_popular_channel_id,
       rt.id                                                               as spr_request_type_id,
       rt2.id                                                              as spr_rso_id,
       st.id                                                               as spr_service_type_id,
       rs.id                                                               as spr_subtype_id,
       ss.id                                                               as spr_status_id,
       b.stars,
       b.vendor_contact
from base b
         left join spr_act a on b.act = a.act_type
         left join spr_request_active ra on b.request_active_or_not_active = ra.active_type
         left join spr_agent c on b.issue_added_by_user_code = c.username
         left join spr_agent c2 on b.assigned_to_agent = c2.username
         left join spr_card_brand cb on b.credit_card_brand = cb.card_type
         left join spr_currency sc on b.commission_currency = sc.code
         left join spr_currency sc2 on b.reporting_currency = sc2.code
         left join spr_city ci on b.event_city = ci.name_eng
         left join spr_iata i on b.iata_provided = i.provided
         left join spr_fulfillment_outcome fo on b.fulfillment_outcome = fo.outcome
         left join spr_fulfillment_method fm on b.fulfillment_method = fm.method
         left join spr_popular_provider pp on b.popular_service_provider = pp.provider
         left join spr_popular_provider pp2 on b.popular_booking_channel = pp2.provider
         left join spr_request_type rt on b.request_type = rt.type
         left join spr_request_type rt2 on b.rso_uptake = rt2.type
         left join spr_service_type st on b.service_type = st.type
         left join spr_request_subtype rs on b.request_subtype = rs.subtype
         left join spr_status ss on b.issue_status = ss.status
where true
on conflict (request_number) do update
    set added_dt                   = excluded.added_dt,
        booking_channel            = excluded.booking_channel,
        changed_dt                 = excluded.changed_dt,
        commission_charged         = excluded.commission_charged,
        confirmation_number        = excluded.confirmation_number,
        closed_dt                  = excluded.closed_dt,
        event_city_other           = excluded.event_city_other,
        event_end_dt               = excluded.event_end_dt,
        event_start_dt             = excluded.event_start_dt,
        followup_method            = excluded.followup_method,
        hotel_chain                = excluded.hotel_chain,
        nights                     = excluded.nights,
        promised_dt                = excluded.promised_dt,
        reporting_amount           = excluded.reporting_amount,
        response_dt                = excluded.response_dt,
        request_details            = excluded.request_details,
        service_provider           = excluded.service_provider,
        situation                  = excluded.situation,
        spr_act_id                 = excluded.spr_act_id,
        spr_active_id              = excluded.spr_active_id,
        spr_added_by_id            = excluded.spr_added_by_id,
        spr_assigned_to_id         = excluded.spr_assigned_to_id,
        spr_card_brand_id          = excluded.spr_card_brand_id,
        spr_commission_currency_id = excluded.spr_commission_currency_id,
        spr_currency_id            = excluded.spr_currency_id,
        spr_event_city_id          = excluded.spr_event_city_id,
        spr_iata_provided_id       = excluded.spr_iata_provided_id,
        spr_fulfillment_outcome_id = excluded.spr_fulfillment_outcome_id,
        spr_fulfillment_method_id  = excluded.spr_fulfillment_method_id,
        spr_popular_provider_id    = excluded.spr_popular_provider_id,
        spr_popular_channel_id     = excluded.spr_popular_channel_id,
        spr_request_type_id        = excluded.spr_request_type_id,
        spr_rso_id                 = excluded.spr_rso_id,
        spr_service_type_id        = excluded.spr_service_type_id,
        spr_subtype_id             = excluded.spr_subtype_id,
        spr_status_id              = excluded.spr_status_id,
        stars                      = excluded.stars,
        vendor_contact             = excluded.vendor_contact
;

update fct_request
set spr_rso_id = null
where spr_rso_id in (select rt.id
                     from spr_request_type rt
                     where rt.type in ('N\A', 'I agent PROGRAM', 'I AIRPORT SERVICES')
                        or rt.type like ('O %'))
;


-- fct_case

with cases as (select substr(c.global_case_number, 7, length(c.global_case_number)) as case_number,
                      substr(c.global_case_number, 4, 3)                            as client,
                      c.case_address_id,
                      c.received_date                                               as received_dt,
                      c.case_date_added                                             as added_dt,
                      c.case_date_changed                                           as changed_dt,
                      c.closed_case_date                                            as closed_dt,
                      c.case_status,
                      c.initial_agent,
                      c.responsible_agent,
                      c.contact_method,
                      c.contact_source,
                      c.current_location,
                      c.current_phone
               from ods_case c
               where c.is_active = 1
                 and substr(c.global_case_number, 4, length(c.global_case_number)) in
                     (select substr(r.request_number, 1, instr(r.request_number, '.') - 1) as case_num
                      from fct_request r))
insert
into fct_case
select null   as id,
       c.added_dt,
       c.case_number,
       c.changed_dt,
       c.closed_dt,
       c.current_location,
       c.current_phone,
       c.received_dt,
       sc.id  as spr_client_id,
       sq.spr_customer_id,
       con.id as spr_init_conc_id,
       scm.id as spr_method_id,
       cn.id  as spr_resp_conc_id,
       scm.id as spr_source_id,
       ss.id  as spr_status_id
from cases c
         left join spr_client sc on c.client = sc.client
         left join (select c2.client || cast(sc2.address_id as string) as client_address_id,
                           sc2.id                                      as spr_customer_id
                    from spr_customer sc2
                             join spr_client c2 on sc2.spr_client_id = c2.id) sq
                   on sq.client_address_id = sc.client || cast(c.case_address_id as string)
         left join spr_status ss on c.case_status = ss.status
         left join spr_agent con on c.initial_agent = con.username
         left join spr_agent cn on c.responsible_agent = cn.username
         left join spr_contact_method scm on c.contact_method = scm.method
         left join spr_contact_source scs on c.contact_source = scs.source
where true
on conflict (case_number, spr_client_id) do update
    set spr_customer_id  = excluded.spr_customer_id,
        received_dt      = excluded.received_dt,
        added_dt         = excluded.added_dt,
        changed_dt       = excluded.changed_dt,
        closed_dt        = excluded.closed_dt,
        spr_status_id    = excluded.spr_status_id,
        spr_init_conc_id = excluded.spr_init_conc_id,
        spr_resp_conc_id = excluded.spr_resp_conc_id,
        current_location = excluded.current_location,
        current_phone    = excluded.current_phone,
        spr_method_id    = excluded.spr_method_id,
        spr_source_id    = excluded.spr_source_id
;

