-- <delete statement: searched>
DELETE FROM Sailors WHERE sname = 'Benedict';

-- <insert statement>
INSERT INTO Sailors ( sid, sname ) OVERRIDING USER VALUE SELECT sid, name FROM Reserves NATURAL JOIN Boats;
INSERT INTO Sailors ( sid, sname ) VALUES ( 123, 'Leo' );
INSERT INTO Sailors VALUES ROW ( 123, 'Leo' );

-- <update statement: searched>
UPDATE Sailors SET sname = 'Leo', rating = 5 WHERE sname = 'Leonard';
UPDATE Sailors SET sname = 'Hans' WHERE sid IN (SELECT sid FROM Reserves WHERE bid = 5);