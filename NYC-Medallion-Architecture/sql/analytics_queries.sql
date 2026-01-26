-- ============================================================
-- NYC Taxi Analytics Queries
-- ============================================================
-- These queries run against the Silver and Gold layer tables
-- to answer common business questions.
-- ============================================================


-- ============================================================
-- VENDOR ANALYTICS
-- ============================================================

-- Which vendor makes the most revenue?
SELECT
    vendor,
    ROUND(SUM(total_amount), 2) AS total_revenue,
    COUNT(*) AS total_trips,
    ROUND(AVG(fare_amount), 2) AS avg_fare
FROM nyctaxi.`02_silver`.yellow_trips_joined
GROUP BY vendor
ORDER BY total_revenue DESC;


-- ============================================================
-- LOCATION ANALYTICS
-- ============================================================

-- Most popular pickup borough
SELECT
    pu_borough,
    COUNT(*) AS total_trips,
    ROUND(SUM(total_amount), 2) AS total_revenue
FROM nyctaxi.`02_silver`.yellow_trips_joined
WHERE pu_borough IS NOT NULL
GROUP BY pu_borough
ORDER BY total_trips DESC;


-- Most popular dropoff borough
SELECT
    do_borough,
    COUNT(*) AS total_trips,
    ROUND(SUM(total_amount), 2) AS total_revenue
FROM nyctaxi.`02_silver`.yellow_trips_joined
WHERE do_borough IS NOT NULL
GROUP BY do_borough
ORDER BY total_trips DESC;


-- Most common journey (borough to borough)
SELECT
    pu_borough,
    do_borough,
    COUNT(*) AS total_trips,
    ROUND(AVG(fare_amount), 2) AS avg_fare,
    ROUND(AVG(trip_distance), 2) AS avg_distance
FROM nyctaxi.`02_silver`.yellow_trips_joined
WHERE pu_borough IS NOT NULL
  AND do_borough IS NOT NULL
GROUP BY pu_borough, do_borough
ORDER BY total_trips DESC
LIMIT 10;


-- Top 10 pickup zones
SELECT
    pu_zone,
    pu_borough,
    COUNT(*) AS total_trips
FROM nyctaxi.`02_silver`.yellow_trips_joined
WHERE pu_zone IS NOT NULL
GROUP BY pu_zone, pu_borough
ORDER BY total_trips DESC
LIMIT 10;


-- ============================================================
-- PAYMENT ANALYTICS
-- ============================================================

-- Payment type distribution
SELECT
    payment_type,
    COUNT(*) AS trip_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS percentage,
    ROUND(SUM(total_amount), 2) AS total_revenue
FROM nyctaxi.`02_silver`.yellow_trips_joined
GROUP BY payment_type
ORDER BY trip_count DESC;


-- ============================================================
-- TIME SERIES ANALYTICS (Gold Layer)
-- ============================================================

-- Daily trips and revenue
SELECT
    pickup_date,
    total_trips,
    total_revenue
FROM nyctaxi.`03_gold`.daily_trip_summary
ORDER BY pickup_date;


-- Weekly summary
SELECT
    DATE_TRUNC('week', pickup_date) AS week_start,
    SUM(total_trips) AS weekly_trips,
    ROUND(SUM(total_revenue), 2) AS weekly_revenue,
    ROUND(AVG(average_fare_per_trip), 2) AS avg_fare
FROM nyctaxi.`03_gold`.daily_trip_summary
GROUP BY DATE_TRUNC('week', pickup_date)
ORDER BY week_start;


-- Busiest days of the week
SELECT
    DAYOFWEEK(pickup_date) AS day_of_week,
    CASE DAYOFWEEK(pickup_date)
        WHEN 1 THEN 'Sunday'
        WHEN 2 THEN 'Monday'
        WHEN 3 THEN 'Tuesday'
        WHEN 4 THEN 'Wednesday'
        WHEN 5 THEN 'Thursday'
        WHEN 6 THEN 'Friday'
        WHEN 7 THEN 'Saturday'
    END AS day_name,
    SUM(total_trips) AS total_trips,
    ROUND(AVG(total_trips), 0) AS avg_daily_trips
FROM nyctaxi.`03_gold`.daily_trip_summary
GROUP BY DAYOFWEEK(pickup_date)
ORDER BY total_trips DESC;


-- ============================================================
-- FARE ANALYTICS
-- ============================================================

-- Fare distribution by rate type
SELECT
    rate_type,
    COUNT(*) AS trip_count,
    ROUND(AVG(fare_amount), 2) AS avg_fare,
    ROUND(AVG(trip_distance), 2) AS avg_distance,
    ROUND(AVG(trip_duration), 1) AS avg_duration_mins
FROM nyctaxi.`02_silver`.yellow_trips_joined
GROUP BY rate_type
ORDER BY trip_count DESC;


-- Airport trips analysis
SELECT
    pu_zone,
    do_zone,
    COUNT(*) AS trip_count,
    ROUND(AVG(fare_amount), 2) AS avg_fare,
    ROUND(AVG(trip_distance), 2) AS avg_distance
FROM nyctaxi.`02_silver`.yellow_trips_joined
WHERE pu_zone LIKE '%Airport%'
   OR do_zone LIKE '%Airport%'
GROUP BY pu_zone, do_zone
ORDER BY trip_count DESC
LIMIT 10;


-- ============================================================
-- DATA QUALITY CHECKS
-- ============================================================

-- Check for nulls in key fields
SELECT
    COUNT(*) AS total_rows,
    SUM(CASE WHEN vendor IS NULL THEN 1 ELSE 0 END) AS null_vendor,
    SUM(CASE WHEN pu_borough IS NULL THEN 1 ELSE 0 END) AS null_pu_borough,
    SUM(CASE WHEN do_borough IS NULL THEN 1 ELSE 0 END) AS null_do_borough,
    SUM(CASE WHEN payment_type IS NULL THEN 1 ELSE 0 END) AS null_payment_type
FROM nyctaxi.`02_silver`.yellow_trips_joined;


-- Check trip duration anomalies
SELECT
    CASE
        WHEN trip_duration < 0 THEN 'Negative'
        WHEN trip_duration = 0 THEN 'Zero'
        WHEN trip_duration BETWEEN 1 AND 60 THEN '1-60 mins'
        WHEN trip_duration BETWEEN 61 AND 120 THEN '61-120 mins'
        ELSE '120+ mins'
    END AS duration_bucket,
    COUNT(*) AS trip_count
FROM nyctaxi.`02_silver`.yellow_trips_joined
GROUP BY 1
ORDER BY 1;
