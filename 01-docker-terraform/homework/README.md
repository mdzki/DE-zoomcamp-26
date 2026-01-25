## Q1
    docker run -it \
        --rm \
        --entrypoint=bash \
        python:3.13
    pip -V
### Answer: 25.3

## Q2 
Given the docker-compose.yaml, what is the hostname and port that pgadmin should use to connect to the postgres database?
### Answer: postgres:5432 or db:5432


## Q3 Counting short trips
 For the trips in November 2025 (lpep_pickup_datetime between '2025-11-01' and '2025-12-01', exclusive of the upper bound), how many trips had a trip_distance of less than or equal to 1 mile?

    select count(*)
    from public.green_taxi_trips_2025_11 g
    where g.trip_distance <= 1
    and g.lpep_pickup_datetime between '2025-11-01' and '2025-12-01';
###  Answer:    8,007


## Q4 Longest trip for each day
-- Which was the pick up day with the longest trip distance? Only consider trips with trip_distance less than 100 miles (to exclude data errors).
-- Use the pick up time for your calculations.

    select cast(lpep_pickup_datetime as date), trip_distance
    from public.green_taxi_trips_2025_11 g
    where trip_distance < 100
    order by trip_distance desc limit 1;
### Answer:    2025-11-14


## Q5 Biggest pickup zone
-- Which was the pickup zone with the largest total_amount (sum of all trips) on November 18th, 2025?

    select start_zone."Zone", sum(total_amount)
    from public.green_taxi_trips_2025_11 g
    join public.taxi_zones start_zone on start_zone."LocationID" = g."PULocationID"
    where cast(g.lpep_pickup_datetime as date) = TO_DATE('20251118', 'YYYYMMDD')
    group by start_zone."Zone"
    order by 2 desc
    limit 1;
### Answer:     East Harlem North


## Q6 Largest tip
For the passengers picked up in the zone named "East Harlem North" in November 2025, which was the drop off zone that had the largest tip?
Note: it's tip , not trip. We need the name of the zone, not the ID.
    
    select end_zone."Zone"
    from public.green_taxi_trips_2025_11 g
    join public.taxi_zones start_zone on start_zone."LocationID" = g."PULocationID" and start_zone."Zone" = 'East Harlem North'
    join public.taxi_zones end_zone on end_zone."LocationID" = g."DOLocationID"
    where lpep_pickup_datetime between '2025-11-01' and '2025-12-01'
    order by g.tip_amount desc
    limit 1;   
### Answer:    Yorkville West

## Q7 

Which of the following sequences, respectively, describes the workflow for:

+ Downloading the provider plugins and setting up backend,
+ Generating proposed changes and auto-executing the plan
+ Remove all resources managed by terraform`

### Answer:  terraform init, terraform apply -auto-approve, terraform destroy
