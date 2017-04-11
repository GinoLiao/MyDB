
-- DROP SCHEMA IF EXISTS assignment6db;
-- CREATE SCHEMA assignment6db;
-- CREATE USER assignment6db PASSWORD 'assignment6db_pwd';
-- GRANT ALL ON SCHEMA assignment6db to assignment6db;
-- GRANT ALL ON ALL TABLES IN SCHEMA assignment6db to assignment6db;

--postgresql://ricedb:zl15ricedb@localhost/postgres


DROP TABLE IF EXISTS Org CASCADE;
DROP TABLE IF EXISTS Meet CASCADE;
DROP TABLE IF EXISTS Participant CASCADE;
DROP TABLE IF EXISTS Leg CASCADE;
DROP TABLE IF EXISTS Stroke CASCADE;
DROP TABLE IF EXISTS Distance CASCADE;
DROP TABLE IF EXISTS Event CASCADE;
DROP TABLE IF EXISTS StrokeOf CASCADE;
DROP TABLE IF EXISTS Heat CASCADE;
DROP TABLE IF EXISTS Swim CASCADE;

CREATE TABLE Org (
    id VARCHAR(10),
    name VARCHAR(20),
    is_univ BOOLEAN NOT NULL,
    PRIMARY KEY (id)

);

CREATE TABLE Meet (
    name VARCHAR(20),
    start_date DATE,        --can be unknown
    num_days INT,           --can be unknown
    org_id VARCHAR(10),     --can be unknown  
    PRIMARY KEY (name),
    FOREIGN KEY (org_id) REFERENCES Org (id),
    --when num_days is not null, it should be a number larger then 0
    CONSTRAINT chk_num_days
    CHECK (num_days > 0)
);


CREATE TABLE Participant (
    id VARCHAR(10),
    gender VARCHAR(1) NOT NULL,
    org_id VARCHAR(10) NOT NULL,
    name VARCHAR(20),
    PRIMARY KEY (id),
    FOREIGN KEY (org_id) REFERENCES Org (id),
    CONSTRAINT chk_gender CHECK (gender IN ('M', 'F'))
);

CREATE TABLE Leg (
    leg INT,
    PRIMARY KEY (leg),
    CONSTRAINT chk_leg CHECK (leg > 0)
);

CREATE TABLE Stroke (
    stroke VARCHAR(20),
    PRIMARY KEY (stroke)
);

CREATE TABLE Distance (
    distance INT,
    PRIMARY KEY (distance),
    CONSTRAINT chk_distance CHECK (distance > 0)
);


CREATE TABLE Event (
    id VARCHAR(10),
    gender VARCHAR(1) NOT NULL,
    distance INT NOT NULL,
    PRIMARY KEY (id),
    FOREIGN KEY (distance) REFERENCES Distance (distance),
    CONSTRAINT chk_gender CHECK (gender IN ('M', 'F'))
);

CREATE TABLE StrokeOf (
    event_id VARCHAR(10),
    leg INT,
    stroke VARCHAR(20) NOT NULL,
    PRIMARY KEY (event_id, leg),
    FOREIGN KEY (event_id) REFERENCES Event (id),
    FOREIGN KEY (leg) REFERENCES Leg (leg),
    FOREIGN KEY (stroke) REFERENCES Stroke (stroke)
);


CREATE TABLE Heat (
    id VARCHAR(10),
    event_id VARCHAR(10),
    meet_name VARCHAR(20),
    PRIMARY KEY (id, event_id, meet_name),
    FOREIGN KEY (event_id) REFERENCES Event (id),
    FOREIGN KEY (meet_name) REFERENCES Meet (name)
);

CREATE TABLE Swim (
    heat_id VARCHAR(10),
    event_id VARCHAR(10),
    meet_name VARCHAR(20),
    participant_id VARCHAR(10),
    leg INT NOT NULL,
    --app side should add '.0' for integer input time
    t DECIMAL,
    PRIMARY KEY (heat_id, event_id, meet_name, participant_id),
    FOREIGN KEY (heat_id, event_id, meet_name) REFERENCES Heat (id, event_id ,meet_name),
    FOREIGN KEY (participant_id) REFERENCES Participant (id),
    FOREIGN KEY (leg) REFERENCES Leg (leg),
    CONSTRAINT chk_time CHECK (t > 0),
    CONSTRAINT chk_gender 
    CHECK (checkSwimGender(event_id, participant_id)),
    --for a relay race event, one school can have only one group in a heat
    --(heat_id, event_id, meet_name, org_id of participant, leg) must be unique
    CONSTRAINT chk_relay_school 
    CHECK (checkRelaySchool(heat_id, event_id, meet_name, participant_id, leg))
);







--------------------------------------
--------------------------------------
----------Check functions-------------
--------------------------------------
--------------------------------------
DROP FUNCTION IF EXISTS checkRelaySchool(
    heat_id_value VARCHAR(10),
    event_id_value VARCHAR(10),
    meet_name_value VARCHAR(20),
    participant_id_value VARCHAR(10),
    leg_value INT) CASCADE;
CREATE OR REPLACE FUNCTION checkRelaySchool(
    heat_id_value VARCHAR(10),
    event_id_value VARCHAR(10),
    meet_name_value VARCHAR(20),
    participant_id_value VARCHAR(10),
    leg_value INT)
RETURNS BOOLEAN
AS $$
    DECLARE 
        event_leg_count INT;
        count_schools INT;  --count of schools for each leg in a relay heat
    BEGIN
        SELECT COUNT(*) into event_leg_count 
        From StrokeOf 
        WHERE event_id=event_id_value;
        IF event_leg_count > 1 THEN
            SELECT Count(*) into count_schools
            FROM Swim
            INNER JOIN Participant 
            ON Participant.id = Swim.participant_id
            GROUP BY heat_id, event_id, meet_name, 
            Participant.org_id, leg;
            IF count_schools >= 1 THEN
                RETURN FALSE;
            END IF;
        END IF;
        RETURN TRUE;
    END $$
LANGUAGE plpgsql
STABLE;



DROP FUNCTION IF EXISTS checkSwimGender(
    event_id_value VARCHAR(10),
    participant_id_value VARCHAR(10)) CASCADE;
CREATE OR REPLACE FUNCTION checkSwimGender (
    event_id_value VARCHAR(10),
    participant_id_value VARCHAR(10))
RETURNS BOOLEAN
AS $$
    DECLARE 
        event_gender VARCHAR(1);
        participant_gender VARCHAR(1);
    BEGIN
        SELECT gender into event_gender 
        From Event 
        WHERE id=event_id_value;
        SELECT gender into participant_gender 
        From Participant 
        WHERE id=participant_id_value;
        IF event_gender!=participant_gender THEN
            RETURN FALSE;
        ELSE
            RETURN TRUE;       
        END IF;
    END $$
LANGUAGE plpgsql
STABLE;



--------------------------------------
--------------------------------------
----------CRUD operations-------------
--------------------------------------
--------------------------------------


--read operations
DROP FUNCTION IF EXISTS GetOrg(VARCHAR(10));
CREATE OR REPLACE FUNCTION GetOrg (id_value VARCHAR(10))
RETURNS TABLE (id VARCHAR(10), name VARCHAR(20), is_univ BOOLEAN)
AS $$
    BEGIN
        RETURN QUERY SELECT * FROM Org WHERE Org.id = id_value;
    END $$
LANGUAGE plpgsql
STABLE;

DROP FUNCTION IF EXISTS GetMeet (name_value VARCHAR(20));
CREATE OR REPLACE FUNCTION GetMeet (name_value VARCHAR(20))
RETURNS TABLE (name VARCHAR(20), start_date DATE, num_days INT, org_id VARCHAR(10))
AS $$
    BEGIN
        RETURN QUERY SELECT * FROM Meet WHERE Meet.name = name_value;
    END $$
LANGUAGE plpgsql
STABLE;

DROP FUNCTION IF EXISTS GetParticipant (id_value VARCHAR(10));
CREATE OR REPLACE FUNCTION GetParticipant (id_value VARCHAR(10))
RETURNS TABLE (id VARCHAR(10), gender VARCHAR(1), org_id VARCHAR(10), name VARCHAR(20))
AS $$
    BEGIN
        RETURN QUERY SELECT * FROM Participant WHERE Participant.id = id_value;
    END $$
LANGUAGE plpgsql
STABLE;

DROP FUNCTION IF EXISTS GetLeg (leg_value INT);
CREATE OR REPLACE FUNCTION GetLeg (leg_value INT)
RETURNS TABLE (leg INT)
AS $$
    BEGIN
        RETURN QUERY SELECT * FROM Leg WHERE Leg.leg = leg_value;
    END $$
LANGUAGE plpgsql
STABLE;

DROP FUNCTION IF EXISTS GetStroke (stroke_value VARCHAR(20));
CREATE OR REPLACE FUNCTION GetStroke (stroke_value VARCHAR(20))
RETURNS TABLE (leg INT)
AS $$
    BEGIN
        RETURN QUERY SELECT * FROM Stroke WHERE Stroke.stroke = stroke_value;
    END $$
LANGUAGE plpgsql
STABLE;

DROP FUNCTION IF EXISTS GetDistance (distance_value INT);
CREATE OR REPLACE FUNCTION GetDistance (distance_value INT)
RETURNS TABLE (leg INT)
AS $$
    BEGIN
        RETURN QUERY SELECT * FROM Distance WHERE Distance.distance = distance_value;
    END $$
LANGUAGE plpgsql
STABLE;

DROP FUNCTION IF EXISTS GetEvent (id_value VARCHAR(10));
CREATE OR REPLACE FUNCTION GetEvent (id_value VARCHAR(10))
RETURNS TABLE (id VARCHAR(10), gender VARCHAR(1), distance INT)
AS $$
    BEGIN
        RETURN QUERY SELECT * FROM Event WHERE Event.id = id_value;
    END $$
LANGUAGE plpgsql
STABLE;

DROP FUNCTION IF EXISTS GetStrokeOf (event_id_value VARCHAR(10), leg_value INT);
CREATE OR REPLACE FUNCTION GetStrokeOf (event_id_value VARCHAR(10), leg_value INT)
RETURNS TABLE (event_id VARCHAR(10), leg INT, stroke VARCHAR(20))
AS $$
    BEGIN
        RETURN QUERY SELECT * FROM StrokeOf 
        WHERE StrokeOf.event_id = event_id_value AND StrokeOf.leg = leg_value;
    END $$
LANGUAGE plpgsql
STABLE;


DROP FUNCTION IF EXISTS GetHeat (id_value VARCHAR(10), 
                                    event_id_value VARCHAR(10), meet_name_value VARCHAR(20));
CREATE OR REPLACE FUNCTION GetHeat (id_value VARCHAR(10), 
                                    event_id_value VARCHAR(10), meet_name_value VARCHAR(20))
RETURNS TABLE (id VARCHAR(10), event_id VARCHAR(10), meet_name VARCHAR(20))
AS $$
    BEGIN
        RETURN QUERY SELECT * FROM Heat WHERE Heat.id = id_value 
        AND Heat.event_id = event_id_value AND Heat.meet_name = meet_name_value;
    END $$
LANGUAGE plpgsql
STABLE;


DROP FUNCTION IF EXISTS GetSwim (heat_id_value VARCHAR(10), 
                                    event_id_value VARCHAR(10), meet_name_value VARCHAR(20),
                                    participant_id_value VARCHAR(10));

CREATE OR REPLACE FUNCTION GetSwim (heat_id_value VARCHAR(10), 
                                    event_id_value VARCHAR(10), meet_name_value VARCHAR(20),
                                    participant_id_value VARCHAR(10))
RETURNS TABLE (heat_id VARCHAR(10), event_id VARCHAR(10), meet_name VARCHAR(20), participant_id VARCHAR(10), leg INT, t DECIMAL)
AS $$
    BEGIN
        RETURN QUERY SELECT * FROM Swim WHERE Swim.heat_id = heat_id_value 
        AND Swim.event_id = event_id_value AND Swim.meet_name = meet_name_value
        AND Swim.participant_id = participant_id_value;
    END $$
LANGUAGE plpgsql
STABLE;





--------------------------------------
--------------------------------------
----------upsert functions------------
--------------------------------------
--------------------------------------

DROP FUNCTION IF EXISTS upsertOrg (
    id_value VARCHAR(10), 
    name_value VARCHAR(20), 
    is_univ_value BOOLEAN);
CREATE OR REPLACE FUNCTION upsertOrg (
    id_value VARCHAR(10), 
    name_value VARCHAR(20), 
    is_univ_value BOOLEAN)
RETURNS VOID
AS $$
    DECLARE 
        matches INT;
    BEGIN
        SELECT COUNT(*) into matches From Org 
        WHERE id=id_value;
        IF matches=0 THEN
            INSERT INTO Org VALUES 
            (id_value, name_value, is_univ_value);
        ELSE
            UPDATE Org SET 
            name=name_value, is_univ=is_univ_value 
            WHERE id=id_value;
        END IF;
    END $$
LANGUAGE plpgsql;



DROP FUNCTION IF EXISTS upsertMeet (
    name_value VARCHAR(20), 
    start_date_value DATE, 
    num_days_value INT, 
    org_id_value VARCHAR(10));
CREATE OR REPLACE FUNCTION upsertMeet (
    name_value VARCHAR(20), 
    start_date_value DATE, 
    num_days_value INT, 
    org_id_value VARCHAR(10))
RETURNS VOID
AS $$
    DECLARE 
        matches INT;
    BEGIN
        SELECT COUNT(*) into matches From Meet 
        WHERE name=name_value;
        IF matches=0 THEN
            INSERT INTO Meet VALUES 
            (name_value, start_date_value, num_days_value, org_id_value);
        ELSE
            UPDATE Meet SET 
            start_date=start_date_value,
            num_days=num_days_value, 
            org_id=org_id_value 
            WHERE name=name_value;       
        END IF;
    END $$
LANGUAGE plpgsql;





DROP FUNCTION IF EXISTS upsertParticipant (
    id_value VARCHAR(10), 
    gender_value VARCHAR(1), 
    org_id_value VARCHAR(10), 
    name_value VARCHAR(20));
CREATE OR REPLACE FUNCTION upsertParticipant (
    id_value VARCHAR(10), 
    gender_value VARCHAR(1), 
    org_id_value VARCHAR(10), 
    name_value VARCHAR(20))
RETURNS VOID
AS $$
    DECLARE 
        matches INT;
    BEGIN
        SELECT COUNT(*) into matches From Participant 
        WHERE id=id_value;
        IF matches=0 THEN
            INSERT INTO Participant VALUES 
            (id_value, gender_value, org_id_value, name_value);
        ELSE
            UPDATE Participant SET 
            gender=gender_value,
            org_id=org_id_value, 
            name=name_value 
            WHERE id=id_value;   
        END IF;
    END $$
LANGUAGE plpgsql;



DROP FUNCTION IF EXISTS upsertLeg (leg_value INT);
CREATE OR REPLACE FUNCTION upsertLeg (leg_value INT)
RETURNS VOID
AS $$
    DECLARE 
        matches INT;
    BEGIN
        SELECT COUNT(*) into matches From Leg 
        WHERE leg=leg_value;
        IF matches=0 THEN
            INSERT INTO Leg VALUES (leg_value);
        END IF;
    END $$
LANGUAGE plpgsql;



DROP FUNCTION IF EXISTS upsertStroke (stroke_value VARCHAR(20));
CREATE OR REPLACE FUNCTION upsertStroke (stroke_value VARCHAR(20))
RETURNS VOID
AS $$
    DECLARE 
        matches INT;
    BEGIN
        SELECT COUNT(*) into matches From Stroke 
        WHERE stroke=stroke_value;
        IF matches=0 THEN
            INSERT INTO Stroke VALUES (stroke_value);
        END IF;
    END $$
LANGUAGE plpgsql;



DROP FUNCTION IF EXISTS upsertDistance (distance_value INT);
CREATE OR REPLACE FUNCTION upsertDistance (distance_value INT)
RETURNS VOID
AS $$
    DECLARE 
        matches INT;
    BEGIN
        SELECT COUNT(*) into matches From Distance 
        WHERE distance=distance_value;
        IF matches=0 THEN
            INSERT INTO Distance VALUES (distance_value);
        END IF;
    END $$
LANGUAGE plpgsql;



DROP FUNCTION IF EXISTS upsertEvent (
    id_value VARCHAR(10), 
    gender_value VARCHAR(1), 
    distance_value INT);
CREATE OR REPLACE FUNCTION upsertEvent (
    id_value VARCHAR(10), 
    gender_value VARCHAR(1), 
    distance_value INT)
RETURNS VOID
AS $$
    DECLARE 
        matches INT;
    BEGIN
        SELECT COUNT(*) into matches From Event 
        WHERE id=id_value;
        IF matches=0 THEN
            INSERT INTO Event VALUES 
            (id_value, gender_value, distance_value);
        ELSE
            UPDATE Event SET 
            gender=gender_value,
            distance=distance_value
            WHERE id=id_value;   
        END IF;
    END $$
LANGUAGE plpgsql;





DROP FUNCTION IF EXISTS upsertStrokeOf (
    event_id_value VARCHAR(10), 
    leg_value INT, 
    stroke_value VARCHAR(20));
CREATE OR REPLACE FUNCTION upsertStrokeOf (
    event_id_value VARCHAR(10), 
    leg_value INT, 
    stroke_value VARCHAR(20))
RETURNS VOID
AS $$
    DECLARE 
        matches INT;
    BEGIN
        SELECT COUNT(*) into matches From StrokeOf 
        WHERE event_id=event_id_value
        AND leg=leg_value;
        IF matches=0 THEN
            INSERT INTO StrokeOf VALUES 
            (event_id_value, leg_value, stroke_value);
        ELSE
            UPDATE StrokeOf SET 
            stroke=stroke_value
            WHERE event_id=event_id_value
            AND leg=leg_value;  
        END IF;
    END $$
LANGUAGE plpgsql;




DROP FUNCTION IF EXISTS upsertHeat (
    id_value VARCHAR(10), 
    event_id_value VARCHAR(10), 
    meet_name_value VARCHAR(20));
CREATE OR REPLACE FUNCTION upsertHeat (
    id_value VARCHAR(10), 
    event_id_value VARCHAR(10), 
    meet_name_value VARCHAR(20))
RETURNS VOID
AS $$
    DECLARE 
        matches INT;
    BEGIN
        SELECT COUNT(*) into matches From Heat 
        WHERE id=id_value
        AND event_id=event_id_value
        AND meet_name=meet_name_value;
        IF matches=0 THEN
            INSERT INTO Heat VALUES 
            (id_value, event_id_value, meet_name_value);
        END IF;
    END $$
LANGUAGE plpgsql;




DROP FUNCTION IF EXISTS upsertSwim (
    heat_id_value VARCHAR(10), 
    event_id_value VARCHAR(10), 
    meet_name_value VARCHAR(20),
    participant_id_value VARCHAR(10),
    leg_value INT,
    t_value DECIMAL);
CREATE OR REPLACE FUNCTION  upsertSwim (
    heat_id_value VARCHAR(10), 
    event_id_value VARCHAR(10), 
    meet_name_value VARCHAR(20),
    participant_id_value VARCHAR(10),
    leg_value INT,
    t_value DECIMAL)
RETURNS VOID
AS $$
    DECLARE 
        matches INT;
    BEGIN
        SELECT COUNT(*) into matches From Swim 
        WHERE heat_id=heat_id_value
        AND event_id=event_id_value
        AND meet_name=meet_name_value
        AND participant_id=participant_id_value;
        IF matches=0 THEN
            INSERT INTO Swim VALUES 
            (heat_id_value, event_id_value, meet_name_value,
             participant_id_value, leg_value, t_value);
        ELSE
            UPDATE Swim SET 
            leg=leg_value,
            t=t_value
            WHERE heat_id=heat_id_value
            AND event_id=event_id_value
            AND meet_name=meet_name_value
            AND participant_id=participant_id_value;
        END IF;
    END $$
LANGUAGE plpgsql;







--queries












