DROP TABLE IF EXISTS input;

CREATE TEMP TABLE input(depth text);

COPY input FROM '/Users/coleiain/aoc_2021/01/input';

WITH indexed as (
    SELECT ROW_NUMBER() OVER(ORDER BY 1) as row_num, CAST(depth as int)
    FROM input
)
SELECT SUM(CASE WHEN cur.depth > prev.depth THEN 1 ELSE 0 END) as increases
FROM indexed cur
LEFT JOIN indexed prev ON prev.row_num = cur.row_num -1;