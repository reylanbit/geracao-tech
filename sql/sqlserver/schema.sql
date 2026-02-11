create schema app;

create table app.customers (
  id bigint primary key,
  referrer_id bigint null references app.customers(id),
  created_at datetime2 not null default sysdatetime()
);

create table app.products (
  id bigint primary key,
  cost decimal(12,4) not null
);

create partition function pfDateRange (datetime2)
as range right for values ('2025-01-01', '2025-04-01', '2025-07-01', '2025-10-01', '2026-01-01');

create partition scheme psDateRange
as partition pfDateRange
all to ([PRIMARY]);

create table app.orders (
  id bigint primary key,
  customer_id bigint not null references app.customers(id),
  status nvarchar(20) not null,
  created_at datetime2 not null
) on psDateRange(created_at);

create table app.order_items (
  order_id bigint not null references app.orders(id),
  product_id bigint not null references app.products(id),
  qty int not null check (qty > 0),
  price decimal(12,4) not null check (price >= 0),
  discount_pct decimal(5,4) default 0 check (discount_pct between 0 and 1),
  primary key (order_id, product_id)
);

create table app.payments (
  order_id bigint not null references app.orders(id),
  amount decimal(12,2) not null check (amount >= 0),
  paid_at datetime2 not null,
  primary key (order_id, paid_at)
);

create table app.events (
  id bigint identity(1,1) primary key,
  customer_id bigint not null references app.customers(id),
  happened_at datetime2 not null default sysdatetime(),
  payload nvarchar(max) not null,
  campaign as coalesce(json_value(payload,'$.utm_campaign'), json_value(payload,'$.campaign'))
);

create index idx_events_customer_time on app.events(customer_id, happened_at);
create index idx_events_campaign on app.events(campaign);

create table app.customer_campaign_history (
  customer_id bigint not null references app.customers(id),
  campaign nvarchar(255) not null,
  valid_from datetime2 not null,
  valid_to datetime2 null,
  current bit not null default 1,
  constraint pk_cch primary key (customer_id, campaign, valid_from)
);

create or alter trigger trg_upsert_campaign on app.events
after insert
as
begin
  set nocount on;
  update cch
     set valid_to = i.happened_at, current = 0
    from app.customer_campaign_history cch
    join inserted i on i.customer_id = cch.customer_id
   where cch.current = 1;
  insert into app.customer_campaign_history(customer_id, campaign, valid_from, current)
  select i.customer_id, i.campaign, i.happened_at, 1
  from inserted i
  where i.campaign is not null;
end;
