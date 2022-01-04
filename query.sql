-- Select Company, Time, HourOfDay, StockPrice
SELECT
    name AS Company, SUBSTRING(ts, 12, 2) || ':00' AS Time,
    CAST(SUBSTRING(ts,12, 2) AS Integer) - 8 AS HourOfDay,
    ROUND(MAX(high),2) AS HighestStockPrice

-- Get data from Table 03 defined by glue crawler
FROM "03"

-- Group by HourOfDay, TimeOfDay and Company
GROUP BY
    CAST(SUBSTRING(ts,12, 2) AS Integer),
    name,
    SUBSTRING(ts,12, 2) || ':00'

-- Order by Company and TimeOfDay
ORDER BY
    name,
    CAST(SUBSTRING(ts,12, 2) As Integer)
