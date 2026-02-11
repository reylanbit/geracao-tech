create function app.fn_events_rls(@customer_id bigint)
returns table
with schemabinding
as
return select 1 as allow
where @customer_id = cast(session_context(n'app.current_customer_id') as bigint);

create security policy app.sp_events_rls
add filter predicate app.fn_events_rls(customer_id) on app.events
with (state = on);

exec sys.sp_set_session_context @key = N'app.current_customer_id', @value = N'1';
select id, happened_at, payload from app.events;
