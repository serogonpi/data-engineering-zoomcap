select count(*)
from {{ source('fhv', 'fhv_tripdata') }}
where dispatching_base_num IS NOT null
    and extract(year from cast(pickup_datetime as date)) = 2019 
