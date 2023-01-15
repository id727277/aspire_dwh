/*

uploading data from STG layer to ODS layer, with cleaning stg tables.

basically this is a procedure, unfortunately SQLite3 does not support stored procedures. 
 */


-- заливаем свежие данные
insert into ods_case
select null       as id,
       sc.case_date_added,
       sc.case_address_id,
       sc.case_date_changed,
       sc.case_status,
       sc.client,
       sc.closed_case_date,
       sc.contact_method,
       sc.contact_source,
       sc.current_location,
       sc.current_phone,
       sc.department,
       sc.global_case_number,
       sc.initial_agent,
       sc.received_date,
       sc.responsible_agent,
       datetime() as effective_from,
       0          as is_active
from stg_case sc
where case_date_added is not null
  and case_address_id is not null
  and case_date_changed is not null
  and case_status is not null
  and global_case_number is not null
  and initial_agent is not null
  and received_date is not null
  and responsible_agent is not null
;

-- делаем временную таблицу с только уникальными строками
drop table if exists tmp_ods_case;
create temp table if not exists tmp_ods_case as
select case_date_added,
       case_address_id,
       case_date_changed,
       case_status,
       client,
       closed_case_date,
       contact_method,
       contact_source,
       current_location,
       current_phone,
       department,
       global_case_number,
       initial_agent,
       received_date,
       responsible_agent,
       effective_from
from (select case_date_added,
             case_address_id,
             case_date_changed,
             case_status,
             client,
             closed_case_date,
             contact_method,
             contact_source,
             current_location,
             current_phone,
             department,
             global_case_number,
             initial_agent,
             received_date,
             responsible_agent,
             effective_from,
             row_number() over (partition by global_case_number, case_date_added, case_date_changed, case_status, client, closed_case_date, contact_method, contact_source, current_location, current_phone, department, initial_agent,received_date, responsible_agent order by effective_from desc) as rn
      from ods_case)
where rn = 1
order by case_date_added, effective_from
;

-- очищаем таблицу ods
delete
from ods_case;

-- заливаем данные из временной таблицы, только уникальные строки
insert into ods_case
select null as id,
       case_date_added,
       case_address_id,
       case_date_changed,
       case_status,
       client,
       closed_case_date,
       contact_method,
       contact_source,
       current_location,
       current_phone,
       department,
       global_case_number,
       initial_agent,
       received_date,
       responsible_agent,
       effective_from,
       0    as is_active
from tmp_ods_case;

-- обновляем флаг активности
update ods_case
set is_active = 1
where id in (select sq.id
             from (select oc.id,
                          row_number() over (partition by oc.global_case_number order by oc.effective_from desc) as rn
                   from ods_case oc) sq
             where sq.rn = 1)
;

-- удаляем временную таблицу
drop table if exists tmp_ods_case;


/*
 РЕКВЕСТЫ
 */
-- заливаем свежие данные из stg
insert into ods_request
select null       as id,
       sr.act,
       sr.actual_response_date,
       sr.assigned_to_agent,
       sr.booking_channel,
       sr.closed_issue_date,
       sr.commission_charged,
       sr.commission_currency,
       sr.confirmation_num,
       sr.credit_card_brand,
       sr.customer_promise_date,
       sr.event_city,
       sr.event_city_other,
       sr.event_country,
       sr.event_end_date_time,
       sr.event_start_date_time,
       sr.follow_up_method,
       sr.fulfillment_method,
       sr.fulfillment_outcome,
       sr.global_request_number,
       sr.hotel_chain,
       sr.iata_provided,
       sr.issue_added_by_user_code,
       sr.issue_date_added,
       sr.issue_date_changed,
       sr.issue_status,
       sr.nights,
       sr.number_of_adults,
       sr.number_of_children,
       sr.popular_booking_channel,
       sr.popular_service_provider,
       sr.reporting_amount,
       sr.reporting_currency,
       sr.request_active_or_not_active,
       sr.request_details,
       sr.request_subtype,
       sr.request_type,
       sr.rso_uptake,
       sr.service_provider,
       sr.service_type,
       sr.situation,
       sr.stars,
       sr.vendor_contact,
       datetime() as effective_from,
       0          as is_active
from stg_request sr
where assigned_to_agent is not null
  and global_request_number is not null
  and issue_added_by_user_code is not null
  and issue_date_added is not null
  and issue_date_changed is not null
  and issue_status is not null
  and request_type is not null
  and service_type is not null
;


-- делаем временную таблицу с только уникальными строками
drop table if exists tmp_ods_request;
create temp table if not exists tmp_ods_request as
select sr.act,
       sr.actual_response_date,
       sr.assigned_to_agent,
       sr.booking_channel,
       sr.closed_issue_date,
       sr.commission_charged,
       sr.commission_currency,
       sr.confirmation_num,
       sr.credit_card_brand,
       sr.customer_promise_date,
       sr.event_city,
       sr.event_city_other,
       sr.event_country,
       sr.event_end_date_time,
       sr.event_start_date_time,
       sr.follow_up_method,
       sr.fulfillment_method,
       sr.fulfillment_outcome,
       sr.global_request_number,
       sr.hotel_chain,
       sr.iata_provided,
       sr.issue_added_by_user_code,
       sr.issue_date_added,
       sr.issue_date_changed,
       sr.issue_status,
       sr.nights,
       sr.number_of_adults,
       sr.number_of_children,
       sr.popular_booking_channel,
       sr.popular_service_provider,
       sr.reporting_amount,
       sr.reporting_currency,
       sr.request_active_or_not_active,
       sr.request_details,
       sr.request_subtype,
       sr.request_type,
       sr.rso_uptake,
       sr.service_provider,
       sr.service_type,
       sr.situation,
       sr.stars,
       sr.vendor_contact,
       sr.effective_from
from (select oc.act,
             oc.actual_response_date,
             oc.assigned_to_agent,
             oc.booking_channel,
             oc.closed_issue_date,
             oc.commission_charged,
             oc.commission_currency,
             oc.confirmation_num,
             oc.credit_card_brand,
             oc.customer_promise_date,
             oc.event_city,
             oc.event_city_other,
             oc.event_country,
             oc.event_end_date_time,
             oc.event_start_date_time,
             oc.follow_up_method,
             oc.fulfillment_method,
             oc.fulfillment_outcome,
             oc.global_request_number,
             oc.hotel_chain,
             oc.iata_provided,
             oc.issue_added_by_user_code,
             oc.issue_date_added,
             oc.issue_date_changed,
             oc.issue_status,
             oc.nights,
             oc.number_of_adults,
             oc.number_of_children,
             oc.popular_booking_channel,
             oc.popular_service_provider,
             oc.reporting_amount,
             oc.reporting_currency,
             oc.request_active_or_not_active,
             oc.request_details,
             oc.request_subtype,
             oc.request_type,
             oc.rso_uptake,
             oc.service_provider,
             oc.service_type,
             oc.situation,
             oc.stars,
             oc.vendor_contact,
             oc.effective_from,
             row_number() over (partition by
                 oc.act,
                 oc.actual_response_date,
                 oc.assigned_to_agent,
                 oc.booking_channel,
                 oc.closed_issue_date,
                 oc.commission_charged,
                 oc.commission_currency,
                 oc.confirmation_num,
                 oc.credit_card_brand,
                 oc.customer_promise_date,
                 oc.event_city,
                 oc.event_city_other,
                 oc.event_country,
                 oc.event_end_date_time,
                 oc.event_start_date_time,
                 oc.follow_up_method,
                 oc.fulfillment_method,
                 oc.fulfillment_outcome,
                 oc.global_request_number,
                 oc.hotel_chain,
                 oc.iata_provided,
                 oc.issue_added_by_user_code,
                 oc.issue_date_added,
                 oc.issue_date_changed,
                 oc.issue_status,
                 oc.nights,
                 oc.number_of_adults,
                 oc.number_of_children,
                 oc.popular_booking_channel,
                 oc.popular_service_provider,
                 oc.reporting_amount,
                 oc.reporting_currency,
                 oc.request_active_or_not_active,
                 oc.request_details,
                 oc.request_subtype,
                 oc.request_type,
                 oc.rso_uptake,
                 oc.service_provider,
                 oc.service_type,
                 oc.situation,
                 oc.stars,
                 oc.vendor_contact
                 order by effective_from desc) as rn
      from ods_request oc) sr
where rn = 1
order by issue_date_added, effective_from
;

-- очищаем таблицу ods
delete
from ods_request;

-- заливаем данные из временной таблицы, только уникальные строки
insert into ods_request
select null as id,
       oc.act,
       oc.actual_response_date,
       oc.assigned_to_agent,
       oc.booking_channel,
       oc.closed_issue_date,
       oc.commission_charged,
       oc.commission_currency,
       oc.confirmation_num,
       oc.credit_card_brand,
       oc.customer_promise_date,
       oc.event_city,
       oc.event_city_other,
       oc.event_country,
       oc.event_end_date_time,
       oc.event_start_date_time,
       oc.follow_up_method,
       oc.fulfillment_method,
       oc.fulfillment_outcome,
       oc.global_request_number,
       oc.hotel_chain,
       oc.iata_provided,
       oc.issue_added_by_user_code,
       oc.issue_date_added,
       oc.issue_date_changed,
       oc.issue_status,
       oc.nights,
       oc.number_of_adults,
       oc.number_of_children,
       oc.popular_booking_channel,
       oc.popular_service_provider,
       oc.reporting_amount,
       oc.reporting_currency,
       oc.request_active_or_not_active,
       oc.request_details,
       oc.request_subtype,
       oc.request_type,
       oc.rso_uptake,
       oc.service_provider,
       oc.service_type,
       oc.situation,
       oc.stars,
       oc.vendor_contact,
       oc.effective_from,
       0    as is_active
from tmp_ods_request oc;

-- обновляем флаг активности
update ods_request
set is_active = 1
where id in (select sq.id
             from (select oc.id,
                          row_number() over (partition by oc.global_request_number order by oc.effective_from desc) as rn
                   from ods_request oc) sq
             where sq.rn = 1);

-- удаляем временную таблицу
drop table if exists tmp_ods_request;


/*
 ПРОФИЛИ КЛИЕНТОВ
 */

-- заливаем свежие данные из stg
insert into ods_address
select null                                          as id,
       sa.added_by,
       sa.address_date_added,
       sa.address_date_changed,
       sa.address_id,
       sa.address_type,
       sa.changed_by,
       sa.client_id,
       sa.dob,
       sa.first_name,
       sa.last_contact_dt,
       sa.member_ref_1,
       sa.member_ref_2,
       sa.member_ref_3,
       sa.member_ref_4,
       sa.member_ref_5,
       sa.member_ref_6,
       sa.middle_name,
       sa.pers_email,
       sa.phone_type,
       sa.phone,
       sa.preferred_name,
       sa.surname,
       sa.work_email,
       sa.client_id || cast(sa.address_id as string) as client_address_id,
       datetime()                                    as effective_from,
       0                                             as is_active
from stg_address sa
where added_by is not null
  and address_date_added is not null
  and address_date_changed is not null
  and address_id is not null
  and address_type is not null
  and changed_by is not null
  and client_id is not null
  and last_contact_dt is not null
;


-- делаем временную таблицу с только уникальными строками
drop table if exists tmp_ods_address;
create temp table if not exists tmp_ods_address as
select sa2.added_by,
       sa2.address_date_added,
       sa2.address_date_changed,
       sa2.address_id,
       sa2.address_type,
       sa2.changed_by,
       sa2.client_id,
       sa2.dob,
       sa2.first_name,
       sa2.last_contact_dt,
       sa2.member_ref_1,
       sa2.member_ref_2,
       sa2.member_ref_3,
       sa2.member_ref_4,
       sa2.member_ref_5,
       sa2.member_ref_6,
       sa2.middle_name,
       sa2.pers_email,
       sa2.phone_type,
       sa2.phone,
       sa2.preferred_name,
       sa2.surname,
       sa2.work_email,
       sa2.effective_from
from (select sa.added_by,
             sa.address_date_added,
             sa.address_date_changed,
             sa.address_id,
             sa.address_type,
             sa.changed_by,
             sa.client_id,
             sa.dob,
             sa.first_name,
             sa.last_contact_dt,
             sa.member_ref_1,
             sa.member_ref_2,
             sa.member_ref_3,
             sa.member_ref_4,
             sa.member_ref_5,
             sa.member_ref_6,
             sa.middle_name,
             sa.pers_email,
             sa.phone_type,
             sa.phone,
             sa.preferred_name,
             sa.surname,
             sa.work_email,
             sa.effective_from,
             row_number() over (partition by
                 sa.added_by,
                 sa.address_date_added,
                 sa.address_date_changed,
                 sa.address_id,
                 sa.address_type,
                 sa.changed_by,
                 sa.client_id,
                 sa.dob,
                 sa.first_name,
                 sa.last_contact_dt,
                 sa.member_ref_1,
                 sa.member_ref_2,
                 sa.member_ref_3,
                 sa.member_ref_4,
                 sa.member_ref_5,
                 sa.member_ref_6,
                 sa.middle_name,
                 sa.pers_email,
                 sa.phone_type,
                 sa.phone,
                 sa.preferred_name,
                 sa.surname,
                 sa.work_email
                 order by effective_from desc) as rn
      from ods_address sa) sa2
where rn = 1
order by address_date_added, effective_from
;

-- очищаем таблицу ods
delete
from ods_address;

-- заливаем данные из временной таблицы, только уникальные строки
insert into ods_address
select null                                          as id,
       sa.added_by,
       sa.address_date_added,
       sa.address_date_changed,
       sa.address_id,
       sa.address_type,
       sa.changed_by,
       sa.client_id,
       sa.dob,
       sa.first_name,
       sa.last_contact_dt,
       sa.member_ref_1,
       sa.member_ref_2,
       sa.member_ref_3,
       sa.member_ref_4,
       sa.member_ref_5,
       sa.member_ref_6,
       sa.middle_name,
       sa.pers_email,
       sa.phone_type,
       sa.phone,
       sa.preferred_name,
       sa.surname,
       sa.work_email,
       sa.client_id || cast(sa.address_id as string) as client_address_id,
       effective_from,
       0                                             as is_active
from tmp_ods_address sa;

-- обновляем флаг активности

update ods_address
set is_active = 1
where id in (select sq.id
             from (select oa.id,
                          row_number() over (partition by oa.client_address_id order by oa.effective_from desc) as rn
                   from ods_address oa) sq
             where sq.rn = 1);

-- удаляем временную таблицу
drop table if exists tmp_ods_address;

/*
rejected строки пишем в отдельную таблицу
 */
insert into rejected_stg_case
select *
from stg_case sc
where case_date_added is null
   or case_address_id is null
   or case_date_changed is null
   or case_status is null
   or global_case_number is null
   or initial_agent is null
   or received_date is null
   or responsible_agent is null
;

insert into rejected_stg_request
select *
from stg_request sr
where assigned_to_agent is null
   or global_request_number is null
   or issue_added_by_user_code is null
   or issue_date_added is null
   or issue_date_changed is null
   or issue_status is null
   or request_type is null
   or service_type is null
;

insert into rejected_stg_address
select *
from stg_address
where added_by is null
   or address_date_added is null
   or address_date_changed is null
   or address_id is null
   or address_type is null
   or changed_by is null
   or client_id is null
   or last_contact_dt is null
;


/*
 очищаем таблицы stg
 */
--
delete
from stg_case
;

delete
from stg_request
;

delete
from stg_address
;

