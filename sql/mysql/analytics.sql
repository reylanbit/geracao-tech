use app;

with fo as (
  select o.customer_id, min(o.created_at) as first_order_date
  from orders o
  group by o.customer_id
),
orders_f as (
  select o.*, fo.first_order_date,
         date_format(o.created_at, '%Y-%m-01') as order_month,
         timestampdiff(month, fo.first_order_date, o.created_at) as months_since_first
  from orders o
  join fo on fo.customer_id = o.customer_id
  where o.status = 'paid'
),
order_revenue as (
  select oi.order_id,
         sum(oi.qty * oi.price * (1 - ifnull(oi.discount_pct,0))) as gross_revenue,
         sum(oi.qty * p.cost) as cogs
  from order_items oi
  join products p on p.id = oi.product_id
  group by oi.order_id
),
metrics as (
  select of.*, orv.gross_revenue, orv.cogs,
         (orv.gross_revenue - orv.cogs) as gross_margin
  from orders_f of
  join order_revenue orv on orv.order_id = of.id
),
rfm as (
  select m.customer_id,
         count(*) as freq,
         max(m.created_at) as recency_date,
         sum(m.gross_revenue) as monetary,
         min(e.campaign) as campaign
  from metrics m
  left join events e on e.customer_id = m.customer_id
  group by m.customer_id
),
cohort as (
  select fo.customer_id, date_format(fo.first_order_date, '%Y-%m-01') as cohort_month
  from fo
),
cohort_orders as (
  select c.cohort_month, m.customer_id, m.months_since_first, m.gross_revenue, m.gross_margin
  from cohort c
  join metrics m on m.customer_id = c.customer_id
),
retention as (
  select cohort_month,
         sum(months_since_first = 0) as m0,
         sum(months_since_first = 1) as m1,
         sum(months_since_first = 2) as m2,
         sum(months_since_first = 3) as m3
  from (
    select distinct cohort_month, customer_id, months_since_first
    from cohort_orders
  ) t
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
influence as (
  with recursive t as (
    select c.id as root_id, c.id as customer_id, c.referrer_id, 0 as depth
    from customers c
    union all
    select t.root_id, c.id, c.referrer_id, t.depth + 1
    from customers c
    join t on c.referrer_id = t.customer_id
    where t.depth < 4
  )
  select root_id,
         sum(depth = 1) as direct_refs,
         sum(depth > 1) as indirect_refs
  from t
  group by root_id
)
select c.cohort_month,
       r.campaign,
       count(distinct r.customer_id) as customers,
       count(distinct m.id) as orders,
       ifnull(ma.revenue,0) as revenue,
       ifnull(ma.margin,0) as margin,
       ma.rev_ma3,
       ma.mar_ma3,
       ifnull(ret.m0,0) as m0,
       ifnull(ret.m1,0) as m1,
       ifnull(ret.m2,0) as m2,
       ifnull(ret.m3,0) as m3,
       ifnull(inf.direct_refs,0) as direct_refs,
       ifnull(inf.indirect_refs,0) as indirect_refs
from cohort c
left join rfm r on r.customer_id = c.customer_id
left join metrics m on m.customer_id = c.customer_id
left join cohort_ma ma on ma.cohort_month = c.cohort_month
left join retention ret on ret.cohort_month = c.cohort_month
left join influence inf on inf.root_id = c.customer_id
group by c.cohort_month, r.campaign, ma.revenue, ma.margin, ma.rev_ma3, ma.mar_ma3, ret.m0, ret.m1, ret.m2, ret.m3, inf.direct_refs, inf.indirect_refs
order by c.cohort_month, r.campaign;

insert into customers(id) values (1), (2), (3);
insert into products(id, cost) values (10, 3.50), (11, 1.20);
insert into orders(id, customer_id, status, created_at) values
  (100, 1, 'paid', '2025-01-10'),
  (101, 1, 'paid', '2025-02-14'),
  (102, 2, 'paid', '2025-01-20');
insert into order_items(order_id, product_id, qty, price, discount_pct) values
  (100, 10, 2, 9.90, 0.05),
  (101, 11, 1, 4.50, 0.00),
  (102, 10, 3, 9.50, 0.10);
insert into events(customer_id, happened_at, payload) values
  (1, '2025-01-05', json_object('utm_campaign','Launch')),
  (2, '2025-01-12', json_object('campaign','PromoA'));
