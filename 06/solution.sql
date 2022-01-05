DROP TABLE IF EXISTS input;

CREATE TEMP TABLE input(line text);

COPY input FROM '/Users/coleiain/aoc_2021/06/input';

WITH recursive initial_fish as (
    SELECT regexp_split_to_table::int as fish
    FROM regexp_split_to_table((SELECT line FROM input LIMIT 1), ',')
), days (day, fish) as (
    SELECT 0, fish
    FROM initial_fish
    UNION ALL
    select day, fish from (
       with cycle as (
           SELECT day + 1 as day, fish
           FROM days d
           WHERE d.day <= 80
       )
        select day, CASE WHEN fish = 0 THEN 6 ELSE fish - 1 END as fish
        from cycle
        UNION ALL
        SELECT day, 8 from cycle where fish = 0
    ) with_offspring
)
SELECT COUNT(*) as solution
FROM days
WHERE day = 80