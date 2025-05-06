with

source as (
    select * from {{ ref("int_orders_operational") }}
),

renamed as (

    SELECT date_date, count(orders_id) as nb_transaction, sum(revenue) as revenue, sum(revenue)/count(orders_id) as average_basket,
    sum(margin) as margin, sum(operationnal_margin) as operationnal_margin
    FROM source
    GROUP BY date_date
    ORDER BY date_date asc)

SELECT * FROM renamed