select pickup_zone, revenue_monthly_total_amount
from {{ ref('fct_monthly_zone_revenue') }}
where service_type = 'Green' 
    and extract(year from revenue_month) = 2020
order by revenue_monthly_total_amount desc
limit 1