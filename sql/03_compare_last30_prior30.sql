WITH bounds AS (
  SELECT
    MAX(date) AS max_date,
    (MAX(date) - INTERVAL 29 DAY) AS last30_start,
    (MAX(date) - INTERVAL 30 DAY) AS prior30_end,
    (MAX(date) - INTERVAL 59 DAY) AS prior30_start
  FROM ads_spend
),
last30 AS (
  SELECT SUM(spend) AS spend, SUM(conversions) AS conversions,
         SUM(conversions) * 100.0 AS revenue
  FROM ads_spend, bounds
  WHERE date BETWEEN bounds.last30_start AND bounds.max_date
),
prior30 AS (
  SELECT SUM(spend) AS spend, SUM(conversions) AS conversions,
         SUM(conversions) * 100.0 AS revenue
  FROM ads_spend, bounds
  WHERE date BETWEEN bounds.prior30_start AND bounds.prior30_end
)
SELECT 'Spend' AS metric, l.spend AS last_30d, p.spend AS prev_30d,
       (l.spend - p.spend) AS delta_abs,
       CASE WHEN p.spend <> 0 THEN (l.spend - p.spend) / p.spend * 100 END AS delta_pct
FROM last30 l, prior30 p
UNION ALL
SELECT 'Conversions', l.conversions, p.conversions,
       (l.conversions - p.conversions),
       CASE WHEN p.conversions <> 0 THEN (l.conversions - p.conversions) / p.conversions * 100 END
FROM last30 l, prior30 p
UNION ALL
SELECT 'Revenue', l.revenue, p.revenue,
       (l.revenue - p.revenue),
       CASE WHEN p.revenue <> 0 THEN (l.revenue - p.revenue) / p.revenue * 100 END
FROM last30 l, prior30 p
UNION ALL
SELECT 'CAC',
       CASE WHEN l.conversions > 0 THEN l.spend / l.conversions END,
       CASE WHEN p.conversions > 0 THEN p.spend / p.conversions END,
       CASE WHEN p.conversions > 0 AND l.conversions > 0
            THEN (l.spend / l.conversions) - (p.spend / p.conversions) END,
       CASE WHEN p.conversions > 0 AND (p.spend / p.conversions) <> 0 AND l.conversions > 0
            THEN ((l.spend / l.conversions) - (p.spend / p.conversions)) / (p.spend / p.conversions) * 100 END
FROM last30 l, prior30 p
UNION ALL
SELECT 'ROAS',
       CASE WHEN l.spend > 0 THEN l.revenue / l.spend END,
       CASE WHEN p.spend > 0 THEN p.revenue / p.spend END,
       CASE WHEN p.spend > 0 AND l.spend > 0
            THEN (l.revenue / l.spend) - (p.revenue / p.spend) END,
       CASE WHEN p.spend > 0 AND (p.revenue / p.spend) <> 0 AND l.spend > 0
            THEN ((l.revenue / l.spend) - (p.revenue / p.spend)) / (p.revenue / p.spend) * 100 END
FROM last30 l, prior30 p;
