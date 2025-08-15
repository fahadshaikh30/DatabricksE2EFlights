/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/
{{ config(materialized="table") }}

with parent_query as (
select 
    F.Amount,
    D.country
from
    workspace.gold.FactBookings F
left join
    workspace.gold.DimAirports D
ON
    F.DimAirportsKey = D.DimAirportsKey
)

select country, sum(Amount) as total_amount
from parent_query
group by country
    /*
    Uncomment the line below to remove records with null `id` values
*/
    -- where id is not null
