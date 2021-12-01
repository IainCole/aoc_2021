DROP TABLE IF EXISTS input;

CREATE TEMP TABLE input(depth text);

COPY input FROM '/Users/coleiain/aoc_2021/01/input';

WITH indexed as (
    SELECT ROW_NUMBER() OVER(ORDER BY 1) as row_num, CAST(depth as int)
    FROM input
), windows as (
    SELECT m1.row_num, m1.depth, m2.depth, m3.depth, m1.depth + m2.depth + m3.depth as window_total
    FROM indexed m1
             INNER JOIN indexed m2 ON m2.row_num = m1.row_num + 1
             INNER JOIN indexed m3 ON m3.row_num = m1.row_num + 2
)
SELECT SUM(CASE WHEN cur.window_total > prev.window_total THEN 1 ELSE 0 END) as increases
FROM windows cur
LEFT JOIN windows prev on prev.row_num = cur.row_num - 1;