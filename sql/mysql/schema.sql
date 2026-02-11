create schema if not exists app;
use app;

create table if not exists customers (
  id bigint primary key,
  referrer_id bigint,
  created_at timestamp not null default current_timestamp,
  constraint fk_customers_ref foreign key (referrer_id) references customers(id)
);

create table if not exists products (
  id bigint primary key,
  cost decimal(12,4) not null
);

create table if not exists orders (
  id bigint primary key,
  customer_id bigint not null,
  status varchar(20) not null,
  created_at timestamp not null,
  constraint fk_orders_cust foreign key (customer_id) references customers(id)
) /* partitioning example by year */ 
partition by range (year(created_at)) (
  partition p2025 values less than (2026)
);

create table if not exists order_items (
  order_id bigint not null,
  product_id bigint not null,
  qty int not null check (qty > 0),
  price decimal(12,4) not null check (price >= 0),
  discount_pct decimal(5,4) default 0 check (discount_pct between 0 and 1),
  primary key (order_id, product_id),
  constraint fk_items_order foreign key (order_id) references orders(id),
  constraint fk_items_product foreign key (product_id) references products(id)
);

create table if not exists payments (
  order_id bigint not null,
  amount decimal(12,2) not null check (amount >= 0),
  paid_at timestamp not null,
  primary key (order_id, paid_at),
  constraint fk_pay_order foreign key (order_id) references orders(id)
);

create table if not exists events (
  id bigint auto_increment primary key,
  customer_id bigint not null,
  happened_at timestamp not null default current_timestamp,
  payload json not null,
  constraint fk_events_cust foreign key (customer_id) references customers(id),
  campaign varchar(255) generated always as (
    coalesce(json_unquote(json_extract(payload, '$.utm_campaign')),
             json_unquote(json_extract(payload, '$.campaign')))
  ) stored
);

create index idx_events_customer_time on events (customer_id, happened_at);
create index idx_events_campaign on events (campaign);

create table if not exists customer_campaign_history (
  customer_id bigint not null,
  campaign varchar(255) not null,
  valid_from timestamp not null,
  valid_to timestamp null,
  current boolean not null default true,
  primary key (customer_id, campaign, valid_from)
);

drop trigger if exists trg_upsert_campaign;
delimiter $$
create trigger trg_upsert_campaign
after insert on events
for each row
begin
  if new.campaign is not null then
    update customer_campaign_history
       set valid_to = new.happened_at, current = false
     where customer_id = new.customer_id
       and current = true;
    insert into customer_campaign_history(customer_id, campaign, valid_from, current)
    values (new.customer_id, new.campaign, new.happened_at, true);
  end if;
end$$
delimiter ;
