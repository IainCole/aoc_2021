DROP TABLE IF EXISTS input;

CREATE TEMP TABLE input(raw text);

COPY input FROM '/Users/coleiain/aoc_2021/02/input';

with instructions as (
    SELECT split_part(raw, ' ', 1) as instruction, CAST(split_part(raw, ' ', 2) as int) as amount
    FROM input
), components as (
    SELECT SUM(CASE WHEN instruction = 'forward' THEN amount ELSE 0 END)                                       as horizontal,
           SUM(CASE WHEN instruction = 'down' THEN amount WHEN instruction = 'up' THEN amount * -1 ELSE 0 END) as vertical
    FROM instructions
)
SELECT horizontal, vertical, horizontal * vertical as solution
FROM components;