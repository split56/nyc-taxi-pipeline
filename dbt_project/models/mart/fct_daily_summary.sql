SELECT
    pickup_year,
    pickup_month,
    pickup_day_of_week,
    is_weekend,
    SUM(trip_count) AS total_trips,
    SUM(total_revenue) AS daily_revenue,
    AVG(avg_fare) AS avg_fare,
    AVG(avg_distance) AS avg_distance,
    AVG(avg_duration) AS avg_duration,
    AVG(avg_speed) AS avg_speed
FROM {{ ref('int_hourly_stats') }}
GROUP BY 1, 2, 3, 4