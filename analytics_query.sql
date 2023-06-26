  CREATE OR REPLACE TABLE UBER_DATABASE.UBER.tbl_analytics AS (
  SELECT
    f.trip_id,
    f.VendorID,
    d.tpep_pickup_datetime,
    d.tpep_dropoff_datetime,
    f.passenger_count,
    r.RatecodeID_name,
    pickup.pickup_latitude,
    pickup.pickup_longitude,
    dropoff.dropoff_latitude,
    dropoff.dropoff_longitude,
    p.payment_type_name,
    f.fare_amount,
    f.extra,
    f.mta_tax,
    f.tip_amount,
    f.tolls_amount,
    f.improvement_surcharge,
    f.total_amount
  FROM
    UBER_DATABASE.UBER.fact_table AS f
  JOIN
    UBER_DATABASE.UBER.datetime_dim AS d ON f.datetime_id = d.datetime_id
  JOIN
    UBER_DATABASE.UBER.RatecodeID AS r ON f.RatecodeID_id = r.RatecodeID_id
  JOIN
    UBER_DATABASE.UBER.payment_type_dim AS p ON f.payment_type_id = p.payment_type_id
  JOIN
    UBER_DATABASE.UBER.pickup_location_dim AS pickup ON f.pickup_location_id = pickup.pickup_location_id
  JOIN
    UBER_DATABASE.UBER.dropoff_location_dim AS dropoff ON f.dropoff_location_id = dropoff.dropoff_location_id
  JOIN
     UBER_DATABASE.UBER.store_and_fwd_flag_dim AS s ON f.store_and_fwd_flag_id = s.store_and_fwd_flag_id
)

select * from tbl_analytics ;

