DROP TABLE IF EXISTS input;

CREATE TEMP TABLE input(bintext text);

COPY input FROM '/Users/coleiain/aoc_2021/03/input';

WITH recursive indexed as (
    SELECT ROW_NUMBER() OVER(ORDER BY 1) as row_num,
           bintext
    FROM input
), length as (
    SELECT MAX(LENGTH(bintext)) as len FROM indexed
), idx_values (row_num, bintext, idx, value) as (
    SELECT row_num, bintext, 1, SUBSTRING(i.bintext, 1, 1)
    FROM indexed i
    UNION ALL
    SELECT row_num, bintext, idx + 1, SUBSTRING(i.bintext, idx + 1, 1)
    FROM idx_values i
    JOIN length l ON 1=1
    WHERE idx < len
), oxygen (idx, row_nums) as (
    SELECT 1, array_agg(iv.row_num)
    FROM idx_values iv
    WHERE iv.idx = 1 AND iv.value = (
        SELECT CASE WHEN SUM(CASE WHEN iv.value = '1' THEN 1 ELSE 0 END) >= SUM(CASE WHEN iv.value = '0' THEN 1 ELSE 0 END) THEN '1' ELSE '0' END as value
        FROM idx_values iv
        WHERE iv.idx = 1
    )
    UNION ALL
    SELECT o.idx + 1, (
        SELECT array_agg(iv.row_num) as row_nums
        FROM idx_values iv
        WHERE iv.row_num = ANY(o.row_nums) AND iv.idx = o.idx + 1 AND iv.value = (
            SELECT CASE WHEN SUM(CASE WHEN iv.value = '1' THEN 1 ELSE 0 END) >= SUM(CASE WHEN iv.value = '0' THEN 1 ELSE 0 END) THEN '1' ELSE '0' END as value
            FROM idx_values iv
            WHERE iv.idx = o.idx + 1 AND iv.row_num = ANY (o.row_nums)
        )
    )
    FROM oxygen o
    JOIN length l ON 1=1
    WHERE o.idx < l.len
), co2 (idx, row_nums) as (
    SELECT 1, array_agg(iv.row_num)
    FROM idx_values iv
    WHERE iv.idx = 1 AND iv.value = (
        SELECT CASE WHEN SUM(CASE WHEN iv.value = '0' THEN 1 ELSE 0 END) <= SUM(CASE WHEN iv.value = '1' THEN 1 ELSE 0 END) THEN '0' ELSE '1' END as value
        FROM idx_values iv
        WHERE iv.idx = 1
    )
    UNION ALL
    SELECT o.idx + 1, (
        SELECT array_agg(iv.row_num) as row_nums
        FROM idx_values iv
        WHERE iv.row_num = ANY(o.row_nums) AND iv.idx = o.idx + 1 AND iv.value = (
            SELECT CASE WHEN SUM(CASE WHEN iv.value = '0' THEN 1 ELSE 0 END) <= SUM(CASE WHEN iv.value = '1' THEN 1 ELSE 0 END) THEN '0' ELSE '1' END as value
            FROM idx_values iv
            WHERE iv.idx = o.idx + 1 AND iv.row_num = ANY (o.row_nums)
        )
    )
    FROM co2 o
             JOIN length l ON 1=1
    WHERE o.idx < l.len
), oxygen_final as (
    SELECT i.bintext as value
    FROM oxygen o2
    INNER JOIN indexed i ON i.row_num = o2.row_nums[1]
    order by idx desc
    limit 1
), co2_final as (
    SELECT i.bintext as value
    FROM co2 co2
    INNER JOIN indexed i ON i.row_num = co2.row_nums[1]
    order by idx desc
    limit 1
), dec_vals(idx, o2, co2, o2str, co2str) as (
    SELECT 1,
           2^(l.len-1) * CAST(SUBSTRING(o2.value, 1, 1) as int),
           2^(l.len-1) * CAST(SUBSTRING(co2.value, 1, 1) as int),
           o2.value,
           co2.value
    FROM oxygen_final o2
    JOIN co2_final co2 on 1=1
    JOIN length l on 1=1
    UNION ALL
    SELECT idx + 1,
           dv.o2 + (2^((l.len - 1) - idx) * CAST(SUBSTRING(dv.o2str, idx + 1, 1) as int)),
           dv.co2 + (2^((l.len - 1) - idx)) * CAST(SUBSTRING(dv.co2str, idx + 1, 1) as int),
           dv.o2str,
           dv.co2str
    FROM dec_vals dv
             JOIN length l on 1=1
    WHERE idx < l.len
)
SELECT o2, co2, o2 * co2 as solution
FROM dec_vals v
JOIN length l on 1=1
WHERE v.idx = l.len