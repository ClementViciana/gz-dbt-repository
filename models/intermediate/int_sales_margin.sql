with 

source1 as (
    select * from {{ ref("stg_gz_raw_data__raw_gz_product") }}
),

source as (
    select * from {{ ref("stg_gz_raw_data__raw_gz_sales") }}
),

renamed as (
    select
        sal.*,
        pr.purchase_price
    from source as sal
    left join source1 as pr
        on sal.products_id = pr.products_id
)

select date_date, orders_id, products_id, revenue, quantity, purchase_price*quantity as purchase_cost, revenue-(purchase_price*quantity) as margin 
from renamed
order by quantity desc