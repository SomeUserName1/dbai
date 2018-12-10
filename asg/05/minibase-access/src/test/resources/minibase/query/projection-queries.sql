SELECT sname AS name FROM Sailors;
SELECT sid AS id, sname AS name FROM Sailors;
SELECT sid, sname FROM Sailors;
SELECT s.sid, sname FROM Sailors AS s;
SELECT S.sname, sid FROM Sailors AS S;

SELECT bid, sid FROM Reserves;
SELECT color FROM Boats;

SELECT attr3 AS ATTR3, attr2 AS ATTR2 FROM Test;
SELECT attr2, attr3, attr4 FROM Test;

SELECT * FROM Sailors AS s, Reserves AS a;
SELECT s.sname, r.* FROM Sailors AS s, Reserves AS r;