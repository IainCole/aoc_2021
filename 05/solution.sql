DROP TABLE IF EXISTS input;

CREATE TEMP TABLE input(line text);

COPY input FROM '/Users/coleiain/aoc_2021/05/input';

WITH recursive split1 as (
    SELECT split_part(line, ' -> ', 1) as start, split_part(line, ' -> ', 2) as finish
    FROM input
), raw_input as (
    SELECT split_part(start, ',', 1)::int as x1, split_part(start, ',', 2)::int as y1, split_part(finish, ',', 1)::int as x2, split_part(finish, ',', 2)::int as y2
    FROM split1
), non_diagonal as (
    SELECT x1, x2, y1, y2
    FROM raw_input
    WHERE x1 = x2 OR y1 = y2
), all_non_diagonal as (
    SELECT x1 as x, y1 as y
    FROM non_diagonal
    UNION ALL
    SELECT x2 as x, y2 as y
    FROM non_diagonal
), x AS (
    select MIN(x) as x
    FROM all_non_diagonal
    UNION ALL
    SELECT x + 1
    FROM x WHERE x < (SELECT MAX(x) FROM all_non_diagonal)
), y AS (
    select MIN(y) as y
    FROM all_non_diagonal
    UNION ALL
    SELECT y + 1
    FROM y WHERE y < (SELECT MAX(y) FROM all_non_diagonal)
), pairs as (
    SELECT concat(x.x::text, ','::text, y1::text) as coord
    FROM non_diagonal
    INNER JOIN x x ON x.x BETWEEN x1 AND x2 OR x.x BETWEEN x2 AND x1
    WHERE y1 = y2
    UNION ALL
    SELECT concat(x1::text, ','::text, y.y::text) as coord
    FROM non_diagonal
    INNER JOIN y y ON y.y BETWEEN y1 AND y2 OR y.y BETWEEN y2 AND y1
    WHERE x1 = x2
), num_overlaps as (
    SELECT coord, COUNT(*) as hits
    FROM pairs
    GROUP BY coord
    HAVING COUNT(*) > 1
)
SELECT COUNT(*) as solution
FROM num_overlaps