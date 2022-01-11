DROP TABLE IF EXISTS input;

CREATE TEMP TABLE input(line text);

COPY input FROM '/Users/coleiain/aoc_2021/08/input';

WITH recursive number_segments as (
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
), raw_signal_patterns as (
    SELECT row_number() over (ORDER BY 1) as pattern_num, split_part(line, ' | ', 1) as sequence, split_part(line, ' | ', 2) as output
    FROM input
), patterns as (
    SELECT p1.pattern_num, segment.segment
    FROM raw_signal_patterns p1
    INNER JOIN regexp_split_to_table(p1.sequence, ' ') segment ON 1=1
), pattern_sequences as (
    SELECT pattern_num, row_number() over (partition by pattern_num order by (select null)) as sequence_num, s.segment as segments, LENGTH(s.segment) AS num_segments
    FROM patterns s
), one as (
    SELECT ps.pattern_num, sequence_num, ps.segments, segment as display_segment
    FROM pattern_sequences ps
    INNER JOIN regexp_split_to_table(ps.segments,'') as T(segment) ON 1=1
    WHERE ps.num_segments = 2
), seven as (
    SELECT ps.pattern_num, sequence_num, ps.segments, segment as display_segment
    FROM pattern_sequences ps
    INNER JOIN regexp_split_to_table(ps.segments,'') as T(segment) ON 1=1
    WHERE ps.num_segments = 3
), segment_a as ( -- in 7 not 1
    SELECT s.pattern_num, 'a' as wire_segment, s.display_segment
    FROM seven s
    LEFT JOIN one o on o.pattern_num = s.pattern_num AND o.display_segment = s.display_segment
    WHERE o.display_segment IS NULL
), four as (
    SELECT ps.pattern_num, sequence_num, ps.segments, segment as display_segment
    FROM pattern_sequences ps
    INNER JOIN regexp_split_to_table(ps.segments,'') as T(segment) ON 1=1
    WHERE ps.num_segments = 4
), five_two_three as (
    SELECT ps.pattern_num, sequence_num, ps.segments, segment as display_segment
    FROM pattern_sequences ps
    INNER JOIN regexp_split_to_table(ps.segments,'') as T(segment) ON 1=1
    WHERE ps.num_segments = 5
), three as (
    with seq as (
        SELECT ftt.pattern_num, ftt.sequence_num
        FROM five_two_three ftt
        INNER JOIN one ON one.pattern_num = ftt.pattern_num AND one.display_segment = ftt.display_segment
        GROUP BY ftt.pattern_num, ftt.sequence_num
        HAVING COUNT(*) = 2
    )
    SELECT ftt.*
    FROM seq
    INNER JOIN five_two_three ftt ON ftt.pattern_num = seq.pattern_num AND ftt.sequence_num = seq.sequence_num
), segment_g as (
    SELECT t.pattern_num, 'g' as wire_segment, t.display_segment
    FROM three t
    LEFT JOIN segment_a a ON a.pattern_num = t.pattern_num AND a.display_segment = t.display_segment
    LEFT JOIN four f ON f.pattern_num = t.pattern_num AND f.display_segment = t.display_segment
    WHERE a.display_segment IS NULL AND f.display_segment IS NULL
), segment_d as (
    SELECT ftt.pattern_num, 'd' as wire_segment, ftt.display_segment
    FROM five_two_three ftt
    INNER JOIN segment_a a ON a.pattern_num = ftt.pattern_num AND a.display_segment != ftt.display_segment
    INNER JOIN segment_g g ON g.pattern_num = ftt.pattern_num AND g.display_segment != ftt.display_segment
    GROUP BY ftt.pattern_num, ftt.display_segment
    HAVING COUNT(*) = 3
), five_two as (
    SELECT DISTINCT ftt.*
    FROM five_two_three ftt
    INNER JOIN three t on t.pattern_num = ftt.pattern_num AND t.sequence_num != ftt.sequence_num
), five as (
    with seq as (
        SELECT ft.pattern_num, ft.sequence_num
        FROM five_two ft
        LEFT JOIN four four ON four.pattern_num = ft.pattern_num AND four.display_segment = ft.display_segment
        INNER JOIN segment_a a ON a.pattern_num = ft.pattern_num AND a.display_segment != ft.display_segment
        WHERE four.display_segment IS NULL
        GROUP BY ft.pattern_num, ft.sequence_num
        HAVING COUNT(*) = 1
    )
    SELECT ft.*
    FROM seq
    INNER JOIN five_two ft ON ft.pattern_num = seq.pattern_num AND ft.sequence_num = seq.sequence_num
), segment_b as ( -- in 5 not 3
    SELECT f.pattern_num, 'b' as wire_segment, f.display_segment
    FROM five f
    LEFT JOIN three t on t.pattern_num = f.pattern_num AND t.display_segment = f.display_segment
    WHERE t.display_segment IS NULL
), segment_c as ( -- in 1 not 5
    SELECT o.pattern_num, 'c' as wire_segment, o.display_segment
    FROM one o
    LEFT JOIN five f on f.pattern_num = o.pattern_num AND f.display_segment = o.display_segment
    WHERE f.display_segment IS NULL
), two as (
    SELECT DISTINCT ft.*
    FROM five_two ft
    INNER JOIN five f on f.pattern_num = ft.pattern_num AND f.sequence_num != ft.sequence_num
), segment_f as ( -- in 7 not 2
    SELECT s.pattern_num, 'f' as wire_segment, s.display_segment
    FROM seven s
    LEFT JOIN two t on t.pattern_num = s.pattern_num AND t.display_segment = s.display_segment
    WHERE t.display_segment IS NULL
), segment_e as ( -- in 2 not 3
    SELECT tw.pattern_num, 'e' as wire_segment, tw.display_segment
    FROM two tw
    LEFT JOIN three th on th.pattern_num = tw.pattern_num AND th.display_segment = tw.display_segment
    WHERE th.display_segment IS NULL
), mapping as (
    SELECT * FROM segment_a
    UNION ALL
    SELECT * FROM segment_b
    UNION ALL
    SELECT * FROM segment_c
    UNION ALL
    SELECT * FROM segment_d
    UNION ALL
    SELECT * FROM segment_e
    UNION ALL
    SELECT * FROM segment_f
    UNION ALL
    SELECT * FROM segment_g
), outputs as (
    SELECT p1.pattern_num, segment.segment
    FROM raw_signal_patterns p1
    INNER JOIN regexp_split_to_table(p1.output, ' ') segment ON 1=1
), output_sequences as (
    SELECT pattern_num, row_number() over (partition by pattern_num order by (select null)) as sequence_num, s.segment as segments
    FROM outputs s
), output_sequence_segments as (
    SELECT pattern_num, sequence_num, row_number() over (partition by pattern_num, sequence_num order by (select null)) as pos, segment.segment
    FROM output_sequences se
    INNER JOIN regexp_split_to_table(se.segments, '') segment ON 1=1
), ordered_output as (
    SELECT oss.pattern_num, oss.sequence_num, m.wire_segment
    FROM output_sequence_segments oss
    INNER JOIN mapping m ON m.pattern_num = oss.pattern_num AND oss.segment = m.display_segment
    ORDER BY oss.pattern_num, oss.sequence_num, m.wire_segment
), fixed_output as (
    SELECT pattern_num, sequence_num, STRING_AGG(wire_segment, '') as fixed_segments
    FROM ordered_output oo
    GROUP BY pattern_num, sequence_num
), values as (
    SELECT pattern_num, sequence_num, fixed_segments, 10^(row_number() over (partition by pattern_num order by sequence_num desc)-1) * ns.num as value
    FROM fixed_output oo
    INNER JOIN number_segments ns ON ns.segments = fixed_segments
), per_pattern as (
    SELECT pattern_num, SUM(value) as value
    FROM values oo
    GROUP BY pattern_num
)
SELECT SUM(value) as solution
FROM per_pattern