SELECT
    pickup_year,
    pickup_month,
    pickup_day_of_week,
    pickup_hour,
    is_weekend,
    COUNT(*) AS trip_count,
    AVG(fare_amount) AS avg_fare,
    AVG(trip_distance) AS avg_distance,
    AVG(trip_duration_minutes) AS avg_duration,
    SUM(total_amount) AS total_revenue,
    AVG(tip_amount) AS avg_tip,
    AVG(speed_mph) AS avg_speed
FROM {{ ref('stg_trips') }}
GROUP BY 1, 2, 3, 4, 5