-- Portfólio fictício: informações em linhas de código
-- O que este sample cobre 
--  - Particionamento por intervalo, índices GIN para JSONB, constraints e checks. 
--  - Trigger SCD2 para histórico de campanha do cliente a partir de eventos. 
--  - Função analítica que calcula RFM/coortes/LTV com janela, LATERAL, JSON e recursão. 
--  - Materialized view com índice e rotina de refresh concorrente. 
--  - RLS simples para filtrar leitura de eventos por cliente via parâmetro de sessão. 
--  - Agregação avançada com GROUPING SETS para visão por campanha/coorte/total. 
--  Se preferir, adapto para MySQL/SQL Server e incluo PIVOT/UNPIVOT, CUBE/ROLLUP mais extenso, ou benchmarks de índices conforme seu volume de dados.

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

create or replace function app.fn_ltv_by_cohort(start_date date, end_date date)
returns table (
  cohort_month date,
  campaign text,
  customers int,
  orders int,
  revenue numeric(14,2),
  margin numeric(14,2),
  rev_ma3 numeric(14,2),
  mar_ma3 numeric(14,2),
  m0 int,
  m1 int,
  m2 int,
  m3 int,
  direct_refs int,
  indirect_refs int
) language sql as $$
with fo as (
  select o.customer_id, min(o.created_at)::date as first_order_date
  from app.orders o
  where o.created_at >= start_date and o.created_at < end_date + interval '1 day'
  group by o.customer_id
),
orders_f as (
  select o.*, fo.first_order_date,
         date_trunc('month', o.created_at)::date as order_month,
         extract(month from age(o.created_at, fo.first_order_date))::int as months_since_first
  from app.orders o
  join fo on fo.customer_id = o.customer_id
  where o.status = 'paid'
),
order_revenue as (
  select oi.order_id,
         sum(oi.qty * oi.price * (1 - coalesce(oi.discount_pct,0)))::numeric(14,2) as gross_revenue,
         sum(oi.qty * p.cost)::numeric(14,2) as cogs
  from app.order_items oi
  join app.products p on p.id = oi.product_id
  group by oi.order_id
),
metrics as (
  select of.*, orv.gross_revenue, orv.cogs,
         (orv.gross_revenue - orv.cogs)::numeric(14,2) as gross_margin
  from orders_f of
  join order_revenue orv on orv.order_id = of.id
),
first_touch as (
  select c.id as customer_id, et.campaign
  from app.customers c
  left join lateral (
    select coalesce(e.payload->>'utm_campaign', e.payload->>'campaign') as campaign
    from app.events e
    where e.customer_id = c.id
      and (e.payload ? 'utm_campaign' or e.payload ? 'campaign')
    order by e.happened_at
    limit 1
  ) et on true
),
rfm as (
  select m.customer_id,
         count(*) as freq,
         max(m.created_at) as recency_date,
         sum(m.gross_revenue)::numeric(14,2) as monetary,
         ft.campaign
  from metrics m
  left join first_touch ft on ft.customer_id = m.customer_id
  group by m.customer_id, ft.campaign
),
cohort as (
  select fo.customer_id, date_trunc('month', fo.first_order_date)::date as cohort_month
  from fo
),
cohort_orders as (
  select c.cohort_month, m.customer_id, m.months_since_first, m.gross_revenue, m.gross_margin
  from cohort c
  join metrics m on m.customer_id = c.customer_id
),
retention as (
  select cohort_month,
         count(distinct customer_id) filter (where months_since_first = 0) as m0,
         count(distinct customer_id) filter (where months_since_first = 1) as m1,
         count(distinct customer_id) filter (where months_since_first = 2) as m2,
         count(distinct customer_id) filter (where months_since_first = 3) as m3
  from cohort_orders
  group by cohort_month
),
cohort_rev as (
  select cohort_month,
         sum(gross_revenue) as revenue,
         sum(gross_margin) as margin
  from cohort_orders
  group by cohort_month
),
cohort_ma as (
  select cohort_month,
         revenue,
         margin,
         avg(revenue) over (order by cohort_month rows between 2 preceding and current row) as rev_ma3,
         avg(margin) over (order by cohort_month rows between 2 preceding and current row) as mar_ma3
  from cohort_rev
),
recursive_referrals as (
  with recursive t as (
    select c.id as root_id, c.id as customer_id, c.referrer_id, 0 as depth
    from app.customers c
    union all
    select t.root_id, c.id, c.referrer_id, t.depth + 1
    from app.customers c
    join t on c.referrer_id = t.customer_id
    where t.depth < 4
  )
  select root_id, customer_id, depth from t
),
influence as (
  select root_id,
         count(*) filter (where depth = 1) as direct_refs,
         count(*) filter (where depth > 1) as indirect_refs
  from recursive_referrals
  group by root_id
),
final as (
  select c.cohort_month,
         r.campaign,
         count(distinct r.customer_id) as customers,
         count(distinct m.id) as orders,
         coalesce(ma.revenue,0) as revenue,
         coalesce(ma.margin,0) as margin,
         ma.rev_ma3,
         ma.mar_ma3,
         coalesce(ret.m0,0) as m0,
         coalesce(ret.m1,0) as m1,
         coalesce(ret.m2,0) as m2,
         coalesce(ret.m3,0) as m3,
         coalesce(inf.direct_refs,0) as direct_refs,
         coalesce(inf.indirect_refs,0) as indirect_refs
  from cohort c
  left join rfm r on r.customer_id = c.customer_id
  left join metrics m on m.customer_id = c.customer_id
  left join cohort_ma ma on ma.cohort_month = c.cohort_month
  left join retention ret on ret.cohort_month = c.cohort_month
  left join influence inf on inf.root_id = c.customer_id
  group by c.cohort_month, r.campaign, ma.revenue, ma.margin, ma.rev_ma3, ma.mar_ma3, ret.m0, ret.m1, ret.m2, ret.m3, inf.direct_refs, inf.indirect_refs
)
select * from final
order by cohort_month, campaign nulls last
$$;

create materialized view if not exists app.mv_cohort_overview as
select *
from app.fn_ltv_by_cohort('2025-01-01'::date, '2025-12-31'::date);

create index if not exists idx_mv_cohort_month_campaign on app.mv_cohort_overview (cohort_month, campaign);

create or replace function app.refresh_mv_cohort()
returns void language sql as $$
refresh materialized view concurrently app.mv_cohort_overview
$$;

alter table app.events enable row level security;
create role app_reader noinherit;
grant select on app.events to app_reader;
create policy events_per_customer on app.events
  for select
  to app_reader
  using (customer_id = current_setting('app.current_customer_id')::bigint);

select set_config('app.current_customer_id', '123', false);

select cohort_month, campaign,
       sum(revenue) as revenue,
       sum(margin) as margin
from app.mv_cohort_overview
group by grouping sets ((cohort_month, campaign), (cohort_month), ())
order by cohort_month nulls last, campaign nulls last;
