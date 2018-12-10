SELECT * FROM Sailors WHERE age < 30;
SELECT * FROM Sailors WHERE 30 > age;
SELECT * FROM Sailors WHERE rating > 3;
SELECT * FROM Sailors WHERE age < 30.0 AND rating > 3;
SELECT * FROM Sailors WHERE rating > 2.5 AND rating < 4.5;
SELECT * FROM Sailors WHERE age < 30 AND age > 25;
SELECT * FROM Sailors WHERE sname = 'Leo';
SELECT * FROM Sailors WHERE sname = 'Manuel';
SELECT * FROM Sailors 
WHERE age > 30.0 AND rating < 4 OR 
      age < 26.0 AND rating > 3;

SELECT * FROM Reserves WHERE sid < 4.1;
SELECT * FROM Reserves WHERE sid > bid;

SELECT * FROM Boats WHERE color = 'white' AND bname = 'Queen Constantia';
SELECT * FROM Boats WHERE Boats.color = 'white' AND bname = 'Queen Constantia';

SELECT * FROM Test WHERE attr2 > '2009-01-01' OR attr3 > '12:00:00';
SELECT * FROM Test WHERE attr1 > 3.1;
SELECT * FROM Test 
WHERE attr2 < '2009-12-30' AND 
      attr3 < '18:30:00' AND 
      attr4 < '2009-03-01 09:56:10.9';