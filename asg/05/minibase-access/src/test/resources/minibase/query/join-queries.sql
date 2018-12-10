SELECT * FROM Sailors AS S, Reserves AS R WHERE S.age < 30;
SELECT * FROM Sailors AS S, Reserves AS R WHERE S.sname = S.sname;
SELECT * FROM Sailors AS S, Reserves AS R WHERE S.sid = R.sid;

SELECT * FROM Sailors AS S, Reserves AS R, Boats AS B WHERE B.bid = R.bid;
SELECT * FROM Sailors AS S, Reserves AS R, Boats AS B WHERE S.sid = R.sid AND R.bid = B.bid;

SELECT * FROM Sailors AS S, Test AS T, Boats AS B WHERE S.sid = T.attr1 AND T.attr1 = B.bid;