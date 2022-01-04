DROP TABLE IF EXISTS input;

CREATE TEMP TABLE input(line text);

COPY input FROM '/Users/coleiain/aoc_2021/04/input';

WITH numbered_lines as (
    SELECT ROW_NUMBER() over (ORDER BY 1) as row_num, regexp_replace(REPLACE(line, '  ', ' '), '^ ', '') as line
    FROM input
), numbers as (
    SELECT regexp_split_to_table((SELECT line FROM numbered_lines WHERE row_num = 1), ',') as number
), ordered_numbers as (
    SELECT ROW_NUMBER() over (ORDER BY 1) as row_num, number::int as number
    FROM numbers
), board_lines as (
    SELECT row_num - 2 as idx, FLOOR((row_num - 2) / 6) + 1 as board_num, (row_num - 2) % 6 as row, line
    FROM numbered_lines
    WHERE row_num > 2 AND (row_num - 2) % 6 != 0
), col_values as (
    SELECT regexp_split_to_table((SELECT line FROM board_lines br where br.idx = br2.idx), ' ') as value
    FROM board_lines br2
), numbered_cols as (
    SELECT ROW_NUMBER() OVER(ORDER BY 1) as idx, value
    FROM col_values
), boards as (
    SELECT ((idx-1) / 25) + 1 as board_num, FLOOR((idx - 1) / 5) + 1 - ((idx - 1) / 25) * 5 as row, ((idx - 1) % 5) + 1 as col, value::int as number
    FROM numbered_cols
), board_nums as (
    SELECT DISTINCT board_num as board_num FROM boards
), winner as (
    SELECT row_num, number, bnum.board_num, col, row
    FROM ordered_numbers onum
             INNER JOIN board_nums bnum ON 1 = 1
             LEFT JOIN LATERAL (SELECT board_num, col, COUNT(*) as num_hits
                                FROM boards
                                WHERE board_num = bnum.board_num
                                  AND number IN (SELECT number FROM ordered_numbers where row_num <= onum.row_num)
                                GROUP BY board_num, col
                                HAVING COUNT(*) = 5) cols ON cols.board_num = bnum.board_num
             LEFT JOIN LATERAL (SELECT board_num, row, COUNT(*) as num_hits
                                FROM boards
                                WHERE board_num = bnum.board_num
                                  AND number IN (SELECT number FROM ordered_numbers where row_num <= onum.row_num)
                                GROUP BY board_num, row
                                HAVING COUNT(*) = 5) rows ON rows.board_num = bnum.board_num
    WHERE cols.num_hits IS NOT NULL
       OR rows.num_hits IS NOT NULL
    ORDER BY row_num ASC
    LIMIT 1
)
SELECT w.board_num, SUM(b.number) * w.number as solution
FROM winner w
INNER JOIN boards b on w.board_num = b.board_num AND b.number NOT IN (SELECT number FROM ordered_numbers WHERE row_num <= w.row_num)
GROUP BY w.board_num, w.number