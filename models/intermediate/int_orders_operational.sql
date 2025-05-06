with 

source1 as (
    select * from {{ ref("stg_gz_raw_data__raw_gz_ship") }}
),

source as (
    select * from {{ ref("int_sales_margin") }}
),

renamed as (
    select
        sal.*,
        pr.shipping_fee, cast(pr.ship_cost as FLOAT64) as ship_cost, pr.logcost
    from source as sal
    left join source1 as pr
        on sal.orders_id = pr.orders_id
)

select orders_id, date_date,  sum(revenue) as revenue, sum(quantity) as quantity, sum(purchase_cost) as purchase_cost,  sum(margin) as margin, sum(margin)+sum(shipping_fee)-sum(logcost)-sum(ship_cost) as operationnal_margin
from renamed
GROUP BY orders_id, date_date
order by quantity desc