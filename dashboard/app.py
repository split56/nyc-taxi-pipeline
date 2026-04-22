import streamlit as st
import duckdb
import pandas as pd
from pathlib import Path

DB_PATH = str(Path(__file__).parent.parent / "taxi.db")

st.set_page_config(page_title="NYC Taxi Analytics", layout="wide")


@st.cache_data(ttl=60)
def query(sql):
    con = duckdb.connect(DB_PATH, read_only=True)
    df = con.execute(sql).fetchdf()
    con.close()
    return df


page = st.sidebar.radio("Navigate", [
    "Overview",
    "Peak Hours",
    "Revenue Trends",
    "Speed & Distance",
    "Payment & Tips",
    "Passenger Patterns",
])

if page == "Overview":
    st.title("NYC Yellow Taxi — Executive Overview")

    totals = query("""
        SELECT
            SUM(total_trips)                    AS total_trips,
            ROUND(SUM(daily_revenue), 0)        AS total_revenue,
            ROUND(AVG(avg_distance), 2)         AS avg_distance,
            ROUND(AVG(avg_duration), 1)         AS avg_duration,
            ROUND(AVG(avg_fare), 2)             AS avg_fare,
            ROUND(AVG(avg_speed), 1)            AS avg_speed
        FROM analytics.fct_daily_summary
    """)

    col1, col2, col3 = st.columns(3)
    col1.metric("Total Trips", f"{int(totals['total_trips'][0]):,}")
    col2.metric("Total Revenue", f"${totals['total_revenue'][0]:,.0f}")
    col3.metric("Avg Fare", f"${totals['avg_fare'][0]:.2f}")

    col4, col5, col6 = st.columns(3)
    col4.metric("Avg Trip Distance", f"{totals['avg_distance'][0]} mi")
    col5.metric("Avg Trip Duration", f"{totals['avg_duration'][0]} min")
    col6.metric("Avg Speed", f"{totals['avg_speed'][0]} mph")

    st.divider()
    st.subheader("Trips & Revenue by Month")
    monthly = query("""
        SELECT
            pickup_year || '-' || LPAD(pickup_month::VARCHAR, 2, '0') AS month,
            SUM(total_trips)              AS total_trips,
            ROUND(SUM(daily_revenue), 0)  AS revenue,
            ROUND(AVG(avg_fare), 2)       AS avg_fare
        FROM analytics.fct_daily_summary
        GROUP BY pickup_year, pickup_month
        ORDER BY pickup_year, pickup_month
    """)
    col_a, col_b = st.columns(2)
    with col_a:
        st.caption("Monthly Trip Volume")
        st.bar_chart(monthly.set_index("month")["total_trips"])
    with col_b:
        st.caption("Monthly Revenue ($)")
        st.bar_chart(monthly.set_index("month")["revenue"])

    st.divider()
    st.subheader("Weekday vs Weekend Summary")
    wk = query("""
        SELECT
            CASE WHEN is_weekend THEN 'Weekend' ELSE 'Weekday' END AS day_type,
            SUM(total_trips)              AS total_trips,
            ROUND(AVG(avg_fare), 2)       AS avg_fare,
            ROUND(AVG(avg_distance), 2)   AS avg_distance,
            ROUND(AVG(avg_duration), 1)   AS avg_duration,
            ROUND(AVG(avg_speed), 1)      AS avg_speed
        FROM analytics.fct_daily_summary
        GROUP BY is_weekend
    """)
    st.dataframe(wk.set_index("day_type"), use_container_width=True)

elif page == "Peak Hours":
    st.title("Peak Hours Analysis")
    st.caption("Top-5 busiest hours by trip volume, split by weekday vs weekend.")

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Weekday Top-5 Hours")
        df_wd = query("""
            SELECT pickup_hour, total_trips, ROUND(avg_fare, 2) AS avg_fare, demand_rank
            FROM analytics.fct_peak_hours
            WHERE is_weekend = false
            ORDER BY demand_rank
        """)
        st.dataframe(df_wd.set_index("demand_rank"), use_container_width=True)
        st.bar_chart(df_wd.set_index("pickup_hour")["total_trips"])

    with col2:
        st.subheader("Weekend Top-5 Hours")
        df_we = query("""
            SELECT pickup_hour, total_trips, ROUND(avg_fare, 2) AS avg_fare, demand_rank
            FROM analytics.fct_peak_hours
            WHERE is_weekend = true
            ORDER BY demand_rank
        """)
        st.dataframe(df_we.set_index("demand_rank"), use_container_width=True)
        st.bar_chart(df_we.set_index("pickup_hour")["total_trips"])

    st.divider()
    st.subheader("Hourly Avg Fare: Weekday vs Weekend")
    st.caption("Higher fares during late-night weekend hours may reflect surge pricing.")
    fare_cmp = query("""
        SELECT
            pickup_hour,
            ROUND(AVG(avg_fare) FILTER (WHERE is_weekend = false), 2) AS weekday_fare,
            ROUND(AVG(avg_fare) FILTER (WHERE is_weekend = true), 2)  AS weekend_fare
        FROM analytics.fct_peak_hours
        GROUP BY pickup_hour
        ORDER BY pickup_hour
    """)
    st.line_chart(fare_cmp.set_index("pickup_hour")[["weekday_fare", "weekend_fare"]])

elif page == "Revenue Trends":
    st.title("Revenue Deep Dive")

    st.subheader("Monthly Revenue & Avg Fare Trend")
    df = query("""
        SELECT
            pickup_year || '-' || LPAD(pickup_month::VARCHAR, 2, '0') AS month,
            ROUND(SUM(daily_revenue), 0)  AS total_revenue,
            ROUND(AVG(avg_fare), 2)       AS avg_fare,
            SUM(total_trips)              AS total_trips
        FROM analytics.fct_daily_summary
        GROUP BY pickup_year, pickup_month
        ORDER BY pickup_year, pickup_month
    """)
    st.line_chart(df.set_index("month")[["total_revenue", "avg_fare"]])

    st.divider()
    st.subheader("Revenue by Day of Week")
    dow_map = {1: "Sun", 2: "Mon", 3: "Tue", 4: "Wed", 5: "Thu", 6: "Fri", 7: "Sat"}
    dow = query("""
        SELECT
            pickup_day_of_week,
            SUM(total_trips)              AS total_trips,
            ROUND(SUM(daily_revenue), 0)  AS total_revenue,
            ROUND(AVG(avg_fare), 2)       AS avg_fare
        FROM analytics.fct_daily_summary
        GROUP BY pickup_day_of_week
        ORDER BY pickup_day_of_week
    """)
    dow["day_name"] = dow["pickup_day_of_week"].map(dow_map)
    col1, col2 = st.columns(2)
    with col1:
        st.caption("Total Revenue by Day")
        st.bar_chart(dow.set_index("day_name")["total_revenue"])
    with col2:
        st.caption("Avg Fare by Day")
        st.bar_chart(dow.set_index("day_name")["avg_fare"])

    st.divider()
    st.subheader("Revenue per Trip over Time")
    st.caption("Rising revenue-per-trip may indicate longer rides or fare increases.")
    df["revenue_per_trip"] = (df["total_revenue"] / df["total_trips"]).round(2)
    st.line_chart(df.set_index("month")["revenue_per_trip"])

elif page == "Speed & Distance":
    st.title("Trip Speed & Distance Characteristics")

    st.subheader("Avg Speed & Distance by Hour of Day")
    df = query("""
        SELECT
            pickup_hour,
            ROUND(AVG(speed_mph), 1)             AS avg_speed_mph,
            ROUND(AVG(trip_distance), 2)         AS avg_distance_mi,
            ROUND(AVG(trip_duration_minutes), 1) AS avg_duration_min
        FROM gold.fct_trips
        GROUP BY pickup_hour
        ORDER BY pickup_hour
    """)
    st.line_chart(df.set_index("pickup_hour")[["avg_speed_mph", "avg_distance_mi"]])

    st.divider()
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Slowest Hours — Congestion")
        st.caption("Likely reflects rush-hour gridlock.")
        slow = df.nsmallest(5, "avg_speed_mph")[["pickup_hour", "avg_speed_mph", "avg_duration_min"]]
        st.dataframe(slow.set_index("pickup_hour"), use_container_width=True)
    with col2:
        st.subheader("Fastest Hours")
        st.caption("Overnight / early morning runs with clear roads.")
        fast = df.nlargest(5, "avg_speed_mph")[["pickup_hour", "avg_speed_mph", "avg_distance_mi"]]
        st.dataframe(fast.set_index("pickup_hour"), use_container_width=True)

    st.divider()
    st.subheader("Speed & Distance by Day of Week")
    dow_map = {1: "Sun", 2: "Mon", 3: "Tue", 4: "Wed", 5: "Thu", 6: "Fri", 7: "Sat"}
    dow = query("""
        SELECT
            pickup_day_of_week,
            ROUND(AVG(avg_speed), 1)    AS avg_speed_mph,
            ROUND(AVG(avg_distance), 2) AS avg_distance_mi,
            ROUND(AVG(avg_duration), 1) AS avg_duration_min
        FROM analytics.fct_daily_summary
        GROUP BY pickup_day_of_week
        ORDER BY pickup_day_of_week
    """)
    dow["day_name"] = dow["pickup_day_of_week"].map(dow_map)
    st.bar_chart(dow.set_index("day_name")[["avg_speed_mph", "avg_distance_mi"]])

elif page == "Payment & Tips":
    st.title("Payment Method & Tipping Analysis")

    st.subheader("Trips & Avg Tip by Payment Type")
    pay = query("""
        SELECT
            CASE payment_type
                WHEN 1 THEN 'Credit Card'
                WHEN 2 THEN 'Cash'
                WHEN 3 THEN 'No Charge'
                WHEN 4 THEN 'Dispute'
                WHEN 5 THEN 'Unknown'
                ELSE 'Other'
            END AS payment_label,
            COUNT(*)                                                         AS trip_count,
            ROUND(AVG(tip_amount), 2)                                        AS avg_tip,
            ROUND(AVG(fare_amount), 2)                                       AS avg_fare,
            ROUND(AVG(total_amount), 2)                                      AS avg_total,
            ROUND(AVG(tip_amount / NULLIF(fare_amount, 0)) * 100, 1)        AS tip_pct
        FROM gold.fct_trips
        GROUP BY payment_type
        ORDER BY trip_count DESC
    """)
    st.dataframe(pay.set_index("payment_label"), use_container_width=True)

    st.divider()
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Avg Tip % by Payment Type")
        st.bar_chart(pay.set_index("payment_label")["tip_pct"])
    with col2:
        st.subheader("Trip Volume by Payment Type")
        st.bar_chart(pay.set_index("payment_label")["trip_count"])

    st.divider()
    st.subheader("Tipping by Hour of Day (Credit Card only)")
    st.caption("Cash tips are not captured — credit card trips only.")
    tip_hour = query("""
        SELECT
            pickup_hour,
            ROUND(AVG(tip_amount), 2)                                  AS avg_tip,
            ROUND(AVG(tip_amount / NULLIF(fare_amount, 0)) * 100, 1)  AS tip_pct,
            COUNT(*)                                                    AS trip_count
        FROM gold.fct_trips
        WHERE payment_type = 1
        GROUP BY pickup_hour
        ORDER BY pickup_hour
    """)
    st.line_chart(tip_hour.set_index("pickup_hour")[["avg_tip", "tip_pct"]])

elif page == "Passenger Patterns":
    st.title("Passenger Count Patterns")

    st.subheader("Trip Stats by Passenger Count")
    pax = query("""
        SELECT
            passenger_count,
            COUNT(*)                                AS trip_count,
            ROUND(AVG(fare_amount), 2)              AS avg_fare,
            ROUND(AVG(trip_distance), 2)            AS avg_distance,
            ROUND(AVG(trip_duration_minutes), 1)    AS avg_duration,
            ROUND(AVG(tip_amount), 2)               AS avg_tip
        FROM gold.fct_trips
        GROUP BY passenger_count
        ORDER BY passenger_count
    """)
    st.dataframe(pax.set_index("passenger_count"), use_container_width=True)

    st.divider()
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Avg Fare by Passenger Count")
        st.bar_chart(pax.set_index("passenger_count")["avg_fare"])
    with col2:
        st.subheader("Avg Tip by Passenger Count")
        st.bar_chart(pax.set_index("passenger_count")["avg_tip"])

    st.divider()
    st.subheader("Solo vs Group Rides by Hour")
    st.caption("Solo = 1 passenger. Group = 2+ passengers.")
    solo_group = query("""
        SELECT
            pickup_hour,
            COUNT(*) FILTER (WHERE passenger_count = 1)  AS solo_trips,
            COUNT(*) FILTER (WHERE passenger_count >= 2) AS group_trips
        FROM gold.fct_trips
        GROUP BY pickup_hour
        ORDER BY pickup_hour
    """)
    st.line_chart(solo_group.set_index("pickup_hour")[["solo_trips", "group_trips"]])
