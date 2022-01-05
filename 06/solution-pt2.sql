DROP TABLE IF EXISTS input;

CREATE TEMP TABLE input(line text);

COPY input FROM '/Users/coleiain/aoc_2021/06/input';

WITH recursive initial_fish as (
    SELECT regexp_split_to_table::int as fish
    FROM regexp_split_to_table((SELECT line FROM input LIMIT 1), ',')
), days (day, zeroes, ones, twos, threes, fours, fives, sixes, sevens, eights) as (
    SELECT 0 as day,
           (SELECT COUNT(*) from initial_fish where fish = 0 LIMIT 1) as zeroes,
           (SELECT COUNT(*) from initial_fish where fish = 1 LIMIT 1) as ones,
           (SELECT COUNT(*) from initial_fish where fish = 2 LIMIT 1) as twos,
           (SELECT COUNT(*) from initial_fish where fish = 3 LIMIT 1) as threes,
           (SELECT COUNT(*) from initial_fish where fish = 4 LIMIT 1) as fours,
           (SELECT COUNT(*) from initial_fish where fish = 5 LIMIT 1) as fives,
           (SELECT COUNT(*) from initial_fish where fish = 6 LIMIT 1) as sixes,
           (SELECT COUNT(*) from initial_fish where fish = 7 LIMIT 1) as sevens,
           (SELECT COUNT(*) from initial_fish where fish = 8 LIMIT 1) as eights
    UNION ALL
    SELECT day + 1, ones, twos, threes, fours, fives, sixes, sevens + zeroes, eights, zeroes
    FROM days d
    WHERE d.day < 256
)
SELECT zeroes + ones + twos + threes + fours + fives + sixes + sevens + eights as solution
FROM days
WHERE day = 256