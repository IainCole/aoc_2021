DROP TABLE IF EXISTS input;

CREATE TEMP TABLE input(raw text);

COPY input FROM '/Users/coleiain/aoc_2021/02/input';

with instructions as (
    SELECT ROW_NUMBER() OVER (ORDER BY 1) as row_num, split_part(raw, ' ', 1) as instruction, CAST(split_part(raw, ' ', 2) as int) as amount
    FROM input
), aim as (
    SELECT i.row_num, SUM(CASE WHEN i2.instruction = 'down' THEN i2.amount WHEN i2.instruction = 'up' THEN i2.amount * -1 ELSE 0 END) as aim
    FROM instructions i
    LEFT JOIN instructions i2 on i2.row_num < i.row_num
    GROUP BY i.row_num
), components as (
    SELECT SUM(CASE WHEN i.instruction = 'forward' THEN amount ELSE 0 END)       as horizontal,
           SUM(CASE WHEN i.instruction = 'forward' THEN amount * aim ELSE 0 END) as vertical
    FROM instructions i
             INNER JOIN aim a on a.row_num = i.row_num
)
SELECT horizontal, vertical, horizontal * vertical as solution
FROM components;