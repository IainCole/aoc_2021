DROP TABLE IF EXISTS input;

CREATE TEMP TABLE input(line text);

COPY input FROM '/Users/coleiain/aoc_2021/08/input';

WITH number_segments as (
    SELECT *, LENGTH(segments) as length
    FROM (
      VALUES
        (0, 'abcefg'),
        (1, 'cf'),
        (2, 'acdeg'),
        (3, 'acdfg'),
        (4, 'bcdf'),
        (5, 'abdfg'),
        (6, 'abdefg'),
        (7, 'acf'),
        (8, 'abcdefg'),
        (9, 'abcdfg')
    ) AS t (num, segments)
), unique_length_numbers as (
    SELECT MIN(ns2.num) as num, MIN(ns2.segments) as segments, ns1.length
    FROM number_segments ns1
    INNER JOIN number_segments ns2 ON ns2.length = ns1.length
    GROUP BY ns1.length
    HAVING COUNT(*) = 1
    ORDER BY num
), raw_outputs as (
    SELECT row_number() over (ORDER BY 1) as pattern_num, split_part(line, ' | ', 2) as output
    FROM input
), outputs as (
    SELECT o1.pattern_num, row_number() over (partition by o1.pattern_num order by (select null)) as digit_num, digit.digit as segments
    FROM raw_outputs o1
    INNER JOIN regexp_split_to_table(o1.output, ' ') digit ON 1=1
)
SELECT COUNT(*) as solution
FROM outputs o
INNER JOIN unique_length_numbers uln ON uln.length = LENGTH(o.segments)