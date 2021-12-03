DROP TYPE IF EXISTS deckrow;
CREATE TYPE deckrow AS (f1 int, f2 int);

WITH RECURSIVE deck as (
    VALUES
        (gen_random_uuid(), 1),
        (gen_random_uuid(), 2),
        (gen_random_uuid(), 3),
        (gen_random_uuid(), 4),
        (gen_random_uuid(), 5),
        (gen_random_uuid(), 6),
        (gen_random_uuid(), 7),
        (gen_random_uuid(), 8),
        (gen_random_uuid(), 9),
        (gen_random_uuid(), 10)
), ordered_deck as (
    SELECT ROW_NUMBER() OVER (ORDER BY column1) as idx, column2 as card_value
    FROM deck
    ORDER BY column1
), p1 as (
    SELECT ROW_NUMBER() OVER (ORDER BY idx) as idx, card_value
    FROM ordered_deck
    WHERE idx <= 5
    ORDER BY idx
), p2 as (
    SELECT ROW_NUMBER() OVER (ORDER BY idx) as idx, card_value
    FROM ordered_deck
    WHERE idx > 5
    ORDER BY idx
), round(p1h, p2h, round_num) AS (
    SELECT (SELECT json_agg(row_to_json((idx, card_value))) FROM p1), (SELECT json_agg(row_to_json((idx, card_value))) FROM p2), 1
    UNION ALL
    SELECT
        (
            SELECT json_agg(row_to_json((f1, f2)))
            FROM (
                     SELECT *
                     FROM json_populate_recordset(null::deckrow, p1h) p1t
                     WHERE p1t.f1 > round_num
                     UNION ALL
                     SELECT * FROM (
                                       VALUES
                                           (
                                               (
                                                   SELECT MAX(f1) + 1
                                                   FROM json_populate_recordset(null::deckrow, p1h)
                                               ),
                                               (
                                                   SELECT f2
                                                   FROM json_populate_recordset(null::deckrow, p1h)
                                                   WHERE f1 = round_num
                                               )
                                           ),
                                           (
                                               (
                                                   SELECT MAX(f1) + 2
                                                   FROM json_populate_recordset(null::deckrow, p1h)
                                               ),
                                               (
                                                   SELECT f2
                                                   FROM json_populate_recordset(null::deckrow, p2h)
                                                   WHERE f1 = round_num
                                               )
                                           )
                                   ) as wincheck
                     WHERE
                             (
                                 SELECT f2
                                 FROM json_populate_recordset(null::deckrow, p1h)
                                 WHERE f1 = round_num
                             )
                             >
                             (
                                 SELECT f2
                                 FROM json_populate_recordset(null::deckrow, p2h)
                                 WHERE f1 = round_num
                             )
                 ) as p1r
        ),
        (
            SELECT json_agg(row_to_json((f1, f2)))
            FROM (
                     SELECT *
                     FROM json_populate_recordset(null::deckrow, p2h) p2t
                     WHERE p2t.f1 > round_num
                     UNION ALL
                     SELECT * FROM (
                                       VALUES
                                           (
                                               (
                                                   SELECT MAX(f1) + 1
                                                   FROM json_populate_recordset(null::deckrow, p2h)
                                               ),
                                               (
                                                   SELECT f2
                                                   FROM json_populate_recordset(null::deckrow, p2h)
                                                   WHERE f1 = round_num
                                               )
                                           ),
                                           (
                                               (
                                                   SELECT MAX(f1) + 2
                                                   FROM json_populate_recordset(null::deckrow, p2h)
                                               ),
                                               (
                                                   SELECT f2
                                                   FROM json_populate_recordset(null::deckrow, p1h)
                                                   WHERE f1 = round_num
                                               )
                                           )
                                   ) as wincheck
                     WHERE
                             (
                                 SELECT f2
                                 FROM json_populate_recordset(null::deckrow, p2h)
                                 WHERE f1 = round_num
                             )
                             >
                             (
                                 SELECT f2
                                 FROM json_populate_recordset(null::deckrow, p1h)
                                 WHERE f1 = round_num
                             )
                 ) as p2r
        ),
        round_num + 1
    FROM round
    WHERE LEAST(
                  (SELECT COUNT(*) as p1c FROM json_populate_recordset(null::deckrow, p1h) p1t WHERE p1t.f1 >= round_num),
                  (SELECT COUNT(*) as p2c FROM json_populate_recordset(null::deckrow, p2h) p2t WHERE p2t.f1 >= round_num)
              ) != 0
      AND
            round_num <= 100
)
SELECT
    (SELECT json_agg(to_json((f2))) FROM json_populate_recordset(null::deckrow, p1h)) as p1_hand,
    (SELECT json_agg(to_json((f2))) FROM json_populate_recordset(null::deckrow, p2h)) as p2_hand,
    CASE WHEN
                 (SELECT COUNT(*) as p1c FROM json_populate_recordset(null::deckrow, p1h)) = 10
             THEN
             CONCAT(CAST('Player 1 wins with a score of ' as text), CAST((SELECT SUM(f2 * (10 - (p1sum.f1 - rds.round_num))) FROM json_populate_recordset(null::deckrow, p1h) p1sum  WHERE p1sum.f1 >= rds.round_num) as text))
         WHEN (SELECT COUNT(*) as p2c FROM json_populate_recordset(null::deckrow, p2h)) = 10
             THEN
             CONCAT(CAST('Player 2 wins with a score of ' as text), CAST((SELECT SUM(f2 * (10 - (p2sum.f1 - rds.round_num))) FROM json_populate_recordset(null::deckrow, p2h) p2sum  WHERE p2sum.f1 >= rds.round_num) as text))
         ELSE
             'Player 1 wins after stalemate'
        END as score
FROM round rds ORDER BY round_num DESC LIMIT 1;