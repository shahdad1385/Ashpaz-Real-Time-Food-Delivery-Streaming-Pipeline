-- 1. Ingest distinct locations first
INSERT INTO locations (location_name, listed_in_city)
SELECT DISTINCT location, "listed_in (city)"
FROM raw_zomato_staging
WHERE location IS NOT NULL AND "listed_in (city)" IS NOT NULL;

-- 2. Ingest unique restaurants mapping them to their location_id
INSERT INTO restaurants (name, address, phone, location_id)
SELECT DISTINCT r.name, r.address, r.phone, l.location_id
FROM raw_zomato_staging r
JOIN locations l ON r.location = l.location_name AND r."listed_in (city)" = l.listed_in_city;

-- 3. Ingest historical metrics and services
INSERT INTO restaurant_metrics_services (
    restaurant_id, online_order, book_table, rate, votes, 
    rest_type, cuisines, approx_cost_two, menu_item, listed_in_type
)
SELECT 
    res.restaurant_id,
    CASE WHEN raw.online_order = 'Yes' THEN TRUE ELSE FALSE END,
    CASE WHEN raw.book_table = 'Yes' THEN TRUE ELSE FALSE END,
    CAST(NULLIF(SPLIT_PART(raw.rate, '/', 1), 'NEW') AS NUMERIC), -- cleans '4.2/5' to 4.2
    raw.votes,
    raw.rest_type,
    raw.cuisines,
    CAST(REPLACE(raw."approx_cost (for two people)", ',', '') AS INTEGER), -- removes comma formatting
    raw.menu_item,
    raw."listed_in (type)"
FROM raw_zomato_staging raw
JOIN locations loc ON raw.location = loc.location_name AND raw."listed_in (city)" = loc.listed_in_city
JOIN restaurants res ON raw.name = res.name AND raw.address = res.address AND res.location_id = loc.location_id;


SELECT 
    COUNT(DISTINCT restaurant_id) AS total_restaurants,
    ROUND(AVG(approx_cost_two), 2) AS avg_approx_cost_for_two
FROM restaurant_metrics_services
WHERE listed_in_type = 'Delivery';


SELECT 
    l.listed_in_city AS city,
    CASE WHEN rms.online_order THEN 'Yes' ELSE 'No' END AS online_order_status,
    ROUND(AVG(rms.approx_cost_two), 2) AS avg_approx_cost_for_two,
    SUM(rms.votes) AS total_votes
FROM restaurant_metrics_services rms
JOIN restaurants r ON rms.restaurant_id = r.restaurant_id
JOIN locations l ON r.location_id = l.location_id
GROUP BY l.listed_in_city, rms.online_order
ORDER BY city ASC, online_order_status DESC;

SELECT 
    l.location_name AS neighborhood,
    ROUND(AVG(rms.rate), 2) AS average_rating,
    COUNT(DISTINCT r.restaurant_id) AS total_restaurants
FROM restaurants r
JOIN locations l ON r.location_id = l.location_id
JOIN restaurant_metrics_services rms ON r.restaurant_id = rms.restaurant_id
GROUP BY l.location_id, l.location_name
HAVING AVG(rms.rate) > 4.2 AND COUNT(DISTINCT r.restaurant_id) >= 50;


SELECT 
    l.listed_in_city AS city,
    CASE 
        WHEN rms.approx_cost_two <= 500 THEN 'Budget'
        WHEN rms.approx_cost_two > 500 AND rms.approx_cost_two < 1000 THEN 'Mid-Range'
        WHEN rms.approx_cost_two >= 1000 THEN 'Premium'
    END AS pricing_tier,
    COUNT(DISTINCT r.restaurant_id) AS total_restaurants,
    ROUND(AVG(rms.rate), 2) AS average_rating
FROM restaurant_metrics_services rms
JOIN restaurants r ON rms.restaurant_id = r.restaurant_id
JOIN locations l ON r.location_id = l.location_id
WHERE rms.approx_cost_two IS NOT NULL
GROUP BY l.listed_in_city, pricing_tier
ORDER BY city ASC, total_restaurants DESC;


