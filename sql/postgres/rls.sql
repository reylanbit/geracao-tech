alter table app.events enable row level security;
create role app_reader noinherit;
grant select on app.events to app_reader;
create policy events_per_customer on app.events
  for select
  to app_reader
  using (customer_id = current_setting('app.current_customer_id')::bigint);

select set_config('app.current_customer_id', '1', false);

set role app_reader;
select id, happened_at, payload from app.events;
reset role;
