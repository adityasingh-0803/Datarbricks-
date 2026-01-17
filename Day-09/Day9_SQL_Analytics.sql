-- Day 9: SQL Analytics & Dashboards
-- Topics: SQL Warehouses, Analytics, Dashboards

-- -----------------------------------
-- 1. Revenue with 7-day Moving Average
-- -----------------------------------
WITH daily AS (
  SELECT
    event_date,
    SUM(revenue) AS rev
  FROM gold.products
  GROUP BY event_date
)
SELECT
  event_date,
  rev,
  AVG(rev) OVER (
    ORDER BY event_date
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
  ) AS ma7
FROM daily
ORDER BY event_date;

-- -----------------------------------
-- 2. Conversion Funnel by Category
-- -----------------------------------
SELECT
  category_code,
  SUM(views) AS views,
  SUM(purchases) AS purchases,
  ROUND(SUM(purchases) * 100.0 / SUM(views), 2) AS conversion_rate
FROM gold.products
GROUP BY category_code
ORDER BY conversion_rate DESC;

-- -----------------------------------
-- 3. Customer Tier Analysis
-- -----------------------------------
SELECT
  CASE
    WHEN cnt >= 10 THEN 'VIP'
    WHEN cnt >= 5 THEN 'Loyal'
    ELSE 'Regular'
  END AS customer_tier,
  COUNT(*) AS customers,
  ROUND(AVG(total_spent), 2) AS avg_ltv
FROM (
  SELECT
    user_id,
    COUNT(*) AS cnt,
    SUM(price) AS total_spent
  FROM silver.events
  WHERE event_type = 'purchase'
  GROUP BY user_id
)
GROUP BY customer_tier
ORDER BY avg_ltv DESC;
