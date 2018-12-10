CREATE TABLE Sailors (
   sid      SMALLINT, 
   sname    CHAR(30), 
   rating   FLOAT, 
   age      DOUBLE
);
CREATE TABLE Boats (
   bid     INT, 
   bname   CHAR(30), 
   color   CHAR(10)
);
CREATE TABLE Reserves (
   sid   SMALLINT   REFERENCES Sailors(sid), 
   bid   INT        REFERENCES Boats(bid), 
   day   CHAR(10)
);
CREATE TABLE Test (
   attr1        TINYINT,
   attr2        DATE,
   attr3        TIME,
   attr4        DATETIME,
   CONSTRAINT   pk_Test PRIMARY KEY (attr1, attr2),
   CONSTRAINT   fk_Test FOREIGN KEY (attr1) REFERENCES Sailors(sid)
);

INSERT INTO Sailors VALUES (0, 'Manuel', 5.1, 24.75);
INSERT INTO Sailors VALUES (1, 'Leo', 4.5, 25.8);
INSERT INTO Sailors VALUES (2, 'Andreas', 2, 34.32);
INSERT INTO Sailors VALUES (3, 'Christoph', 3.9, 29.95);
INSERT INTO Sailors VALUES (4, 'Alex', 4.3, 37.23);
INSERT INTO Sailors VALUES (5, 'Christian', 3.6, 38.23);

INSERT INTO Boats VALUES (0, 'Queen Constantia', 'white');
INSERT INTO Boats VALUES (1, 'La Scholle', 'blue');
INSERT INTO Boats VALUES (2, 'Pride of Kreuzlingen', 'pink');

INSERT INTO Reserves VALUES (0, 0, '1988-06-01');
INSERT INTO Reserves VALUES (0, 0, '2000-04-02');
INSERT INTO Reserves VALUES (0, 1, '2001-01-03');
INSERT INTO Reserves VALUES (0, 0, '2009-07-04');
INSERT INTO Reserves VALUES (3, 2, '2010-04-05');
INSERT INTO Reserves VALUES (1, 1, '2099-03-06');
INSERT INTO Reserves VALUES (1, 1, '2007-08-06');

INSERT INTO Test VALUES (0, DATE '2000-11-10', TIME '18:30:00', TIMESTAMP '2000-03-01 09:56:10.9');
INSERT INTO Test VALUES (1, DATE '2010-01-29', TIME '00:00:00', TIMESTAMP '2003-04-02 02:20:12.3');
INSERT INTO Test VALUES (2, DATE '2009-12-30', TIME '12:45:00', TIMESTAMP '2005-05-20 15:40:09');
INSERT INTO Test VALUES (3, DATE '2005-09-21', TIME '23:59:59', TIMESTAMP '2007-09-23 18:30:3');
INSERT INTO Test VALUES (4, DATE '2003-06-10', TIME '15:09:11', TIMESTAMP '2010-12-20 23:48:4');

-- Hacking the system catalog
UPDATE systables SET card = 750 WHERE name = 'Sailors';
UPDATE systables SET card = 250 WHERE name = 'Boats';
UPDATE systables SET card = 1500 WHERE name = 'Reserves';
UPDATE systables SET card = 12345 WHERE name = 'Test';