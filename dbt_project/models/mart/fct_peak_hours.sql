WITH ranked AS (
    SELECT
        pickup_hour,
        is_weekend,
        SUM(trip_count) AS total_trips,
        AVG(avg_fare) AS avg_fare,
        ROW_NUMBER() OVER (
            PARTITION BY is_weekend
            ORDER BY SUM(trip_count) DESC
        ) AS demand_rank
    FROM {{ ref('int_hourly_stats') }}
    GROUP BY pickup_hour, is_weekend
)
SELECT * FROM ranked WHERE demand_rank <= 5