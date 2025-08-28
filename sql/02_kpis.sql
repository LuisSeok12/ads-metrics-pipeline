WITH period AS (
  SELECT *
  FROM ads_spend
  WHERE date BETWEEN $start_date AND $end_date
),
agg AS (
  SELECT
    SUM(spend) AS spend,
    SUM(conversions) AS conversions
  FROM period
)
SELECT
  spend,
  conversions,
  conversions * 100.0 AS revenue,
  CASE WHEN conversions > 0 THEN spend / conversions END AS CAC,
  CASE WHEN spend > 0 THEN (conversions * 100.0) / spend END AS ROAS
FROM agg;
