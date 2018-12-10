SELECT S.*, R.day, B.bname
  FROM Sailors S NATURAL JOIN Reserves R INNER JOIN Boats B ON R.bid = B.bid
 WHERE S.rating BETWEEN 3 AND 5 AND
       R.day >= '01-01-2015'
 GROUP BY S.name
HAVING S.name IN (SELECT sname FROM Sailors WHERE sname IN ('Leo', 'Manuel'))
 ORDER BY R.day;