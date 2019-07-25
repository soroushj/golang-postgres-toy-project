-- Run on 1M random records


EXPLAIN ANALYZE
SELECT *
FROM data_items
WHERE
  namespace = '500' AND
  key = '500000000000';
-- Index Scan using data_items_nkai on data_items  (cost=0.42..8.45 rows=1 width=54) (actual time=0.019..0.019 rows=0 loops=1)
--   Index Cond: ((namespace = '500'::text) AND (key = '500000000000'::text))
-- Planning Time: 0.357 ms
-- Execution Time: 0.039 ms


EXPLAIN ANALYZE
SELECT *
FROM data_items
WHERE
  namespace = '500' AND
  key = '500000000000' AND
  id < 5687385292800000000
ORDER BY id DESC
LIMIT 100;
-- Limit  (cost=0.42..8.45 rows=1 width=54) (actual time=0.056..0.056 rows=0 loops=1)
--   ->  Index Scan using data_items_nki on data_items  (cost=0.42..8.45 rows=1 width=54) (actual time=0.054..0.054 rows=0 loops=1)
--         Index Cond: ((namespace = '500'::text) AND (key = '500000000000'::text) AND (id < '5687385292800000000'::bigint))
-- Planning Time: 0.188 ms
-- Execution Time: 0.083 ms


EXPLAIN ANALYZE
SELECT *
FROM data_items
WHERE
  namespace = '500' AND
  key = '500000000000' AND
  content->>'author' = '50000' AND
  id < 5687385292800000000
ORDER BY id DESC
LIMIT 100;
-- Limit  (cost=0.42..8.45 rows=1 width=54) (actual time=0.018..0.018 rows=0 loops=1)
--   ->  Index Scan using data_items_nkai on data_items  (cost=0.42..8.45 rows=1 width=54) (actual time=0.015..0.015 rows=0 loops=1)
--         Index Cond: ((namespace = '500'::text) AND (key = '500000000000'::text) AND ((content ->> 'author'::text) = '50000'::text) AND (id < '5687385292800000000'::bigint))
-- Planning Time: 0.147 ms
-- Execution Time: 0.047 ms


EXPLAIN ANALYZE
SELECT *
FROM namespace_most_recent_ids
WHERE most_recent_id < 5687385292800000000
ORDER BY most_recent_id DESC
LIMIT 100;
-- Limit  (cost=0.28..8.29 rows=1 width=11) (actual time=0.006..0.006 rows=0 loops=1)
--   ->  Index Scan using namespace_most_recent_ids_i_unique on namespace_most_recent_ids  (cost=0.28..8.29 rows=1 width=11) (actual time=0.005..0.005 rows=0 loops=1)
--         Index Cond: (most_recent_id < '5687385292800000000'::bigint)
-- Planning Time: 0.280 ms
-- Execution Time: 0.025 ms


EXPLAIN ANALYZE
INSERT INTO data_items (namespace, key, content)
VALUES ('500', '500000000000', '{"author":"50000"}'::jsonb)
RETURNING *;
-- Insert on data_items  (cost=0.00..0.26 rows=1 width=112) (actual time=5.095..5.097 rows=1 loops=1)
--   ->  Result  (cost=0.00..0.26 rows=1 width=112) (actual time=4.661..4.661 rows=1 loops=1)
-- Planning Time: 0.048 ms
-- Trigger refresh_namespace_most_recent_ids: time=442.107 calls=1
-- Trigger set_version: time=0.290 calls=1
-- Execution Time: 447.344 ms
