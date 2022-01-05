DROP TABLE IF EXISTS input;

CREATE TEMP TABLE input(line text);

COPY input FROM '/Users/coleiain/aoc_2021/07/input';

WITH recursive positions as (
    SELECT regexp_split_to_table::int as position
    FROM regexp_split_to_table((SELECT line FROM input LIMIT 1), ',')
), minmax as (
    SELECT MIN(position) min, MAX(position) max
    FROM positions
), candidates as (
    SELECT min target, max
    FROM minmax
    UNION ALL
    SELECT target+1 position, max
    FROM candidates
    WHERE target < max
), fuel as (
    SELECT c.target, SUM(ABS(p.position - c.target)) as fuel_used
    FROM candidates c
    INNER JOIN positions p ON true
    GROUP BY c.target
)
SELECT target, fuel_used as solution
FROM fuel
ORDER BY fuel_used
LIMIT 1