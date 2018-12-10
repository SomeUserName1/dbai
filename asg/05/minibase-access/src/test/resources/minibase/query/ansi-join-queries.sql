SELECT * FROM Sailors AS S NATURAL JOIN Reserves AS R;
SELECT * FROM Reserves AS R JOIN Sailors AS S ON R.sid = S.sid;

-- The following query causes an assertion violation (bug in SqlJoinedTableSuffix#toExpression())
-- SELECT * FROM Reserves AS R JOIN Sailors AS S ON S.sid = R.sid;

SELECT * FROM Sailors AS S JOIN Reserves AS R ON S.sid = R.sid WHERE S.age > 25;
SELECT * FROM Sailors AS S INNER JOIN Reserves AS R ON S.sid = R.sid WHERE S.rating < 4;
SELECT * FROM Sailors AS S JOIN Reserves AS R ON S.sid = R.sid JOIN Boats B ON R.bid = B.bid;