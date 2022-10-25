-- SQL Big Query Project
-- Link instruction: https://docs.google.com/spreadsheets/d/1WnBJsZXj_4FDi2DyfLH1jkWtfTridO2icWbWCh7PLs8/edit#gid=0

-- Query 01: calculate total visit, pageview, transaction and revenue for Jan, Feb and March 2017 order by month

#standardSQL
SELECT
	format_date("%Y%m",	PARSE_DATE('%Y%m%d',date)) AS MONTH,
	SUM(totals.visits) AS visits,
	SUM(totals.pageviews) AS pageviews,
	SUM(totals.transactions) AS transactions,
	safe_divide(SUM(totals.totalTransactionRevenue),1000000) AS revenue
FROM
	`bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE
	_table_suffix BETWEEN '20170101' AND '20170331'
GROUP BY
	1
ORDER BY
	1;

-- Query 02: Bounce rate per traffic source in July 2017
#standardSQL
SELECT
	trafficSource.SOURCE AS source,
	SUM(totals.visits) AS total_visits,
	SUM(totals.bounces) AS total_no_of_bounces,
	round(safe_divide(SUM(totals.bounces), SUM(totals.visits))* 100, 8) AS bounce_rate
FROM
	`bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
GROUP BY
	1
ORDER BY
	2 DESC
LIMIT 4;

-- Query 3: Revenue by traffic source by week, by month in June 2017
#standardSQL
WITH 
rev_month AS (
	SELECT
		"Month" AS time_type, format_date("%Y%m", PARSE_DATE('%Y%m%d', date)) AS time,
		trafficSource.SOURCE AS source,
		safe_divide(SUM(totals.totalTransactionRevenue), 1000000) AS revenue
	FROM
		`bigquery-public-data.google_analytics_sample.ga_sessions_201706*`
	GROUP BY
		source,
		time_type,
		time
),

rev_week AS (
	SELECT
		"Week" AS time_type, format_date("%Y%W", PARSE_DATE('%Y%m%d', date)) AS time,
		trafficSource.SOURCE AS source,
		safe_divide(SUM(totals.totalTransactionRevenue), 1000000) AS revenue
	FROM
		`bigquery-public-data.google_analytics_sample.ga_sessions_201706 *`
	GROUP BY
		source,
		time_type,
		time
)

SELECT
	*
FROM
	rev_month
UNION ALL 
SELECT
	*
FROM
	rev_week
ORDER BY
	revenue DESC
LIMIT 4;

--Query 04: Average number of product pageviews by purchaser type (purchasers vs non-purchasers) in June, July 2017. Note: totals.transactions >=1 for purchaser and totals.transactions is null for non-purchaser

#standardSQL
WITH
pur_yes AS (
	SELECT
		format_date("%Y%m",	PARSE_DATE('%Y%m%d', date)) AS MONTH,
		SUM(totals.pageviews) AS total_yes,
		fullVisitorId
	FROM
		`bigquery-public-data.google_analytics_sample.ga_sessions_ *`
	WHERE
		_table_suffix BETWEEN '20170601' AND '20170731'
		AND totals.transactions >= 1
	GROUP BY
		fullVisitorId,
		MONTH
),

pur_no AS (
	SELECT
		format_date("%Y%m",	PARSE_DATE('%Y%m%d', date)) AS MONTH,
		SUM(totals.pageviews) AS total_no,
		fullVisitorId
	FROM
		`bigquery-public-data.google_analytics_sample.ga_sessions_ *`
	WHERE
		_table_suffix BETWEEN '20170601' AND '20170731'
		AND totals.transactions IS NULL
	GROUP BY
		fullVisitorId,
		MONTH
)

SELECT
	pur_yes.month, 
	round(avg(total_yes), 8) AS avg_pageviews_purchase,
	round(avg(pur_no), 8) AS avg_pageviews_non_purchase
FROM
	pur_yes
JOIN pur_no ON
	pur_yes.month = pur_no.month
GROUP BY
	MONTH
ORDER BY
	MONTH;
-- Query 05: Average number of transactions per user that made a purchase in July 2017

#standardSQL
WITH 
sum_cte AS (
	SELECT
		format_date("%Y%m",	PARSE_DATE('%Y%m%d', date)) AS MONTH,
		SUM(totals.transactions) AS total,
		fullVisitorId
	FROM
		`bigquery-public-data.google_analytics_sample.ga_sessions_201707 *`
	WHERE
		totals.transactions >= 1
	GROUP BY
		fullVisitorId,
		MONTH
)

SELECT
	MONTH,
	round(avg(total), 8) AS Avg_total_transactions_per_user
FROM
	sum_cte
GROUP BY
	MONTH;

-- Query 06: Average amount of money spent per session

#standardSQL
WITH sum_cte AS (
	SELECT
		format_date("%Y%m",	PARSE_DATE('%Y%m%d', date)) AS MONTH,
		SUM(totals.totaltransactionRevenue) AS total_revenue,
		visitId
	FROM
		`bigquery-public-data.google_analytics_sample.ga_sessions_201707 *`
	WHERE
		totals.transactions IS NOT NULL
	GROUP BY
		visitId,
		MONTH
)

SELECT
	MONTH,
	round(avg(total_revenue), 6) AS avg_revenue_by_user_per_visit
FROM
	SUM_cte
GROUP BY
	MONTH;

-- Query 07: Other products purchased by customers who purchased product "YouTube Men's Vintage Henley" in July 2017

#standardSQL
WITH cte_unnest AS (
	SELECT
		fullVisitorId,
		v2ProductName,
	FROM
		`bigquery-public-data.google_analytics_sample.ga_sessions_201707 *`
	CROSS JOIN UNNEST(hits)
	CROSS JOIN UNNEST(product)
	WHERE
		v2ProductName = "YouTube Men's Vintage Henley"
		AND productRevenue IS NOT NULL
)
SELECT
	v2ProductName AS other_purchased_products,
	SUM(productQuantity) AS quantity
FROM
	`bigquery-public-data.google_analytics_sample.ga_sessions_201707 *`
CROSS JOIN UNNEST(hits)
CROSS JOIN UNNEST(product)
WHERE
	fullVisitorId IN (
	SELECT
		fullVisitorId
	FROM
		cte_unnest)
	AND productRevenue IS NOT NULL
	AND v2ProductName != "YouTube Men's Vintage Henley"
GROUP BY
	v2ProductName
ORDER BY
	quantity DESC;

--Query 08: Calculate cohort map from pageview to addtocart to purchase in last 3 month. For example, 100% pageview then 40% add_to_cart and 10% purchase.

#standardSQL
WITH 
type2 AS (
	SELECT
		format_date("%Y%m",	parse_date("%Y%m%d", date)) AS MONTH,
		COUNT(v2ProductName) AS num_product_view
	FROM
		`bigquery-public-data.google_analytics_sample.ga_sessions_2017 *`
	CROSS JOIN UNNEST(hits)
	CROSS JOIN UNNEST(product)
	WHERE
		ecommerceaction.action_type = '2'
		AND (isImpression IS NULL
			OR isImpression = FALSE)
	GROUP BY
		MONTH
),

type3 AS (
	SELECT
		format_date("%Y%m",	parse_date("%Y%m%d", date)) AS MONTH,
		COUNT(v2ProductName) AS num_addtocart
	FROM
		`bigquery-public-data.google_analytics_sample.ga_sessions_2017 *`
	CROSS JOIN UNNEST(hits)
	CROSS JOIN UNNEST(product)
	WHERE
		ecommerceaction.action_type = '3'
	GROUP BY
		MONTH
),

type6 AS (
	SELECT
		format_date("%Y%m",	parse_date("%Y%m%d", date)) AS MONTH,
		COUNT(v2ProductName) AS num_purchase
	FROM
		`bigquery-public-data.google_analytics_sample.ga_sessions_2017 *`
	CROSS JOIN UNNEST(hits)
	CROSS JOIN UNNEST(product)
	WHERE
		ecommerceaction.action_type = '6'
	GROUP BY
		MONTH
)

SELECT
	type2.month,
	num_product_view,
	num_addtocart,
	num_purchase,
	round(Safe_divide(num_addtocart, num_product_view)* 100, 2) AS add_to_cart_rate,
	round(Safe_divide(num_purchase,	num_product_view)* 100,	2) AS purchase_rate
FROM
	type2
JOIN type3
		USING(MONTH)
JOIN type6
		USING(MONTH)
ORDER BY
	MONTH
LIMIT 3;