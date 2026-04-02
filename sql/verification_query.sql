SELECT 'region' AS table_name, COUNT(*) AS row_count FROM region
UNION ALL
SELECT 'nation', COUNT(*) FROM nation
UNION ALL
SELECT 'supplier', COUNT(*) FROM supplier
UNION ALL
SELECT 'customer', COUNT(*) FROM customer
UNION ALL
SELECT 'part', COUNT(*) FROM part
UNION ALL
SELECT 'partsupp', COUNT(*) FROM partsupp
UNION ALL
SELECT 'orders', COUNT(*) FROM orders
UNION ALL
SELECT 'lineitem', COUNT(*) FROM lineitem
ORDER BY table_name;
