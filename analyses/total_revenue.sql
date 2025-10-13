with payments as (

    select * from {{ ref('stg_stripe__payments') }}
),

aggregated as (

    select 
        sum(amount) as total_successful
    from payments 
    where status = 'success' 
)

select * from aggregated
