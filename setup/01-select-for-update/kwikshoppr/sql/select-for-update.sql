-- initialize table
DROP TABLE IF EXISTS t;
CREATE TABLE t (k INT PRIMARY KEY, v INT);
INSERT INTO t (k, v) VALUES (1, 0), (2, 0), (3, 0);
SELECT * FROM t;

-- normal select & update transaction
BEGIN;
SELECT * FROM t WHERE k = 2;
UPDATE t SET v = 5 WHERE k = 2;
COMMIT;

-- select for update

-- implicit select for update
