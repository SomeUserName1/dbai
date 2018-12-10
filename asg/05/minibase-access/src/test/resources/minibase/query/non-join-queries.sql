SELECT sname AS name FROM Sailors UNION SELECT bname AS name FROM Boats;
SELECT sname AS name FROM Sailors EXCEPT ALL SELECT bname AS name FROM Boats;
(SELECT sname AS name FROM Sailors) INTERSECT DISTINCT (SELECT bname AS name FROM Boats);

SELECT sname AS name 
FROM Sailors 
INTERSECT DISTINCT 
SELECT bname AS name FROM Boats
UNION ALL 
SELECT * FROM Reserves 
EXCEPT DISTINCT 
(SELECT * FROM Boats)
INTERSECT 
SELECT * FROM Reserves 
UNION 
SELECT * FROM Boats 
UNION 
SELECT * FROM Boats;

-- Query Expression -> Joined Table
(SELECT * FROM Sailors) AS S NATURAL JOIN Reserves AS R;
-- Query Expression -> Non-Join Query Expression
(SELECT * FROM Sailors);