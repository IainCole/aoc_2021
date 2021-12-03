DROP TABLE IF EXISTS input;

CREATE TEMP TABLE input(bintext text);

COPY input FROM '/Users/coleiain/aoc_2021/03/input';

WITH recursive indexed as (
    SELECT ROW_NUMBER() OVER(ORDER BY 1) as row_num,
           bintext
    FROM input
), length as (
    SELECT MAX(LENGTH(bintext)) as len FROM indexed
), vals (idx, gamma, epsilon) as (
    SELECT 1, mode() WITHIN GROUP (ORDER BY SUBSTRING(i.bintext, 1, 1)), CAST(1-CAST(mode() WITHIN GROUP (ORDER BY SUBSTRING(i.bintext, 1, 1)) as int) as text)
    FROM indexed i
    UNION ALL
    SELECT s.idx + 1,
    CONCAT(s.gamma, (SELECT mode() WITHIN GROUP (ORDER BY SUBSTRING(i.bintext, s.idx + 1, 1)) FROM indexed i)),
    CONCAT(s.epsilon, (SELECT CAST(1-CAST(mode() WITHIN GROUP (ORDER BY SUBSTRING(i.bintext, s.idx + 1, 1)) as int) as text) FROM indexed i))
    FROM vals s
    JOIN length l ON 1=1
    WHERE idx < l.len
), dec_vals(idx, gamma, epsilon, gammastr, epsilonstr) as (
    SELECT 1,
           2^(l.len-1) * CAST(SUBSTRING(v.gamma, 1, 1) as int),
           2^(l.len-1) * CAST(SUBSTRING(v.epsilon, 1, 1) as int),
           v.gamma,
           v.epsilon
    FROM vals v
    JOIN length l on 1=1
    WHERE v.idx = l.len
    UNION ALL
    SELECT idx + 1,
           dv.gamma + (2^((l.len - 1) - idx) * CAST(SUBSTRING(dv.gammastr, idx + 1, 1) as int)),
           dv.epsilon + (2^((l.len - 1) - idx)) * CAST(SUBSTRING(dv.epsilonstr, idx + 1, 1) as int),
           dv.gammastr,
           dv.epsilonstr
    FROM dec_vals dv
    JOIN length l on 1=1
    WHERE idx < l.len
)
SELECT gamma, epsilon, gamma * epsilon as solution
FROM dec_vals v
JOIN length l on 1=1
WHERE v.idx = l.len
