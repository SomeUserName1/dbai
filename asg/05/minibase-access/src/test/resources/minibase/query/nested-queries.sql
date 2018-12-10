-- Should produce multiple errors, but does not...
SELECT B.bname AS Name, B.color AS Name FROM Boats B, Boats AS B;

-- Nesting in SELECT-clause
SELECT (SELECT bname FROM Boats) AS B FROM Reserves;

-- Nesting in FROM-clause
SELECT * FROM (SELECT bid FROM Boats) AS B, Reserves AS R WHERE B.bid = R.rid;

-- Nesting in WHERE-clause
SELECT * FROM Reserves WHERE bid IN (SELECT bid FROM Boats WHERE bname = 'La Scholle');
SELECT * FROM Reserves WHERE bid = (SELECT bid FROM Boats WHERE bname = 'La Scholle');
SELECT * FROM Reserves WHERE bid = (SELECT bid FROM Boats WHERE bname IN (SELECT * FROM Boats));

-- Nesting galore!
SELECT (SELECT bname FROM Boats) 
FROM (SELECT * FROM Reserves) AS R
WHERE B.bname IN (SELECT sname FROM Sailors) AND B.bname IN (SELECT sname FROM Sailors);

-- The famous "saudumme query"
SELECT A.id, agg_x, agg_y
  FROM A,
       (SELECT id, MAX(x) AS agg_x FROM B GROUP BY id) AS B1,
       (SELECT id, MIN(y) AS agg_y FROM B GROUP BY id) AS B2
 WHERE A.id = B1.id AND A.id = B2.id;
