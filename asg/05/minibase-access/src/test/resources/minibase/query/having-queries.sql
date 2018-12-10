SELECT * FROM Sailors HAVING age < 30;
SELECT * FROM Sailors HAVING 30 > age;
SELECT * FROM Sailors HAVING rating > 3;
SELECT * FROM Sailors HAVING age < 30.0 AND rating > 3;
SELECT * FROM Sailors HAVING rating > 2.5 AND rating < 4.5;
SELECT * FROM Sailors HAVING age < 30 AND age > 25;
SELECT * FROM Sailors HAVING sname = 'Leo';
SELECT * FROM Sailors HAVING sname = 'Manuel';
SELECT * FROM Sailors 
HAVING age > 30.0 AND rating < 4 OR 
      age < 26.0 AND rating > 3;

SELECT * FROM Reserves HAVING sid < 4.1;
SELECT * FROM Reserves HAVING sid > bid;

SELECT * FROM Boats HAVING color = 'white' AND bname = 'Queen Constantia';

SELECT * FROM Test HAVING attr2 > '2009-01-01' OR attr3 > '12:00:00';
SELECT * FROM Test HAVING attr1 > 3.1;
SELECT * FROM Test 
HAVING attr2 < '2009-12-30' AND 
      attr3 < '18:30:00' AND 
      attr4 < '2009-03-01 09:56:10.9';