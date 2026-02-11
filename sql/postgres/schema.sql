create schema if not exists app;

create table if not exists app.customers (
  id bigint primary key,
  referrer_id bigint references app.customers(id),
  created_at timestamp not null default now()
);

create table if not exists app.products (
  id bigint primary key,
  cost numeric(12,4) not null
);

create table if not exists app.orders (
  id bigint primary key,
  customer_id bigint not null references app.customers(id),
  status text not null,
  created_at timestamp not null
) partition by range (created_at);

create table if not exists app.orders_2025q1
  partition of app.orders for values from ('2025-01-01') to ('2025-04-01');

create table if not exists app.order_items (
  order_id bigint not null references app.orders(id),
  product_id bigint not null references app.products(id),
  qty int not null check (qty > 0),
  price numeric(12,4) not null check (price >= 0),
  discount_pct numeric(5,4) default 0 check (discount_pct between 0 and 1),
  primary key (order_id, product_id)
);

create table if not exists app.payments (
  order_id bigint not null references app.orders(id),
  amount numeric(12,2) not null check (amount >= 0),
  paid_at timestamp not null,
  primary key (order_id, paid_at)
);

create table if not exists app.events (
  id bigserial primary key,
  customer_id bigint not null references app.customers(id),
  happened_at timestamp not null default now(),
  payload jsonb not null
);

create index if not exists idx_events_customer_time on app.events (customer_id, happened_at);
create index if not exists idx_events_payload_gin on app.events using gin (payload);

create table if not exists app.customer_campaign_history (
  customer_id bigint not null references app.customers(id),
  campaign text not null,
  valid_from timestamp not null,
  valid_to timestamp,
  current boolean not null default true,
  primary key (customer_id, campaign, valid_from)
);

create or replace function app.fn_upsert_campaign()
returns trigger language plpgsql as $$
declare c text;
begin
  c := coalesce(new.payload->>'utm_campaign', new.payload->>'campaign');
  if c is null then
    return new;
  end if;
  update app.customer_campaign_history
     set valid_to = new.happened_at, current = false
   where customer_id = new.customer_id
     and current = true;
  insert into app.customer_campaign_history(customer_id, campaign, valid_from, current)
  values (new.customer_id, c, new.happened_at, true);
  return new;
end $$;

drop trigger if exists trg_upsert_campaign on app.events;
create trigger trg_upsert_campaign
after insert on app.events
for each row execute procedure app.fn_upsert_campaign();
