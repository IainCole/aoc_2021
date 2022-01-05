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
), fuel_usage as (
    SELECT c.target moves, SUM(c2.target) fuel_used
    FROM candidates c
    INNER JOIN candidates c2 ON c2.target <= c.target
    GROUP BY c.target
), fuel as (
    SELECT c.target, SUM(f.fuel_used) fuel_used
    FROM candidates c
    INNER JOIN positions p ON true
    INNER JOIN fuel_usage f ON f.moves = ABS(p.position - c.target)
    GROUP BY c.target
)
SELECT target, fuel_used as solution
FROM fuel
ORDER BY fuel_used
LIMIT 1