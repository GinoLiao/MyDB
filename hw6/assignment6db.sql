
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
    FOREIGN KEY (heat_id, event_id, meet_name) 
        REFERENCES Heat (id, event_id ,meet_name),
    FOREIGN KEY (participant_id) 
        REFERENCES Participant (id),
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
        event_leg_count INT;    --count of legs in this event
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


--read operations by primary keys
DROP FUNCTION IF EXISTS GetOrg(VARCHAR(10));
CREATE OR REPLACE FUNCTION GetOrg (
    id_value VARCHAR(10))
RETURNS TABLE (id VARCHAR(10), 
    name VARCHAR(20), is_univ BOOLEAN)
AS $$
    BEGIN
        RETURN QUERY SELECT * FROM Org 
        WHERE Org.id = id_value;
    END $$
LANGUAGE plpgsql
STABLE;

DROP FUNCTION IF EXISTS GetMeet (
    name_value VARCHAR(20));
CREATE OR REPLACE FUNCTION GetMeet (
    name_value VARCHAR(20))
RETURNS TABLE (name VARCHAR(20), start_date DATE, 
    num_days INT, org_id VARCHAR(10))
AS $$
    BEGIN
        RETURN QUERY SELECT * FROM Meet 
        WHERE Meet.name = name_value;
    END $$
LANGUAGE plpgsql
STABLE;

DROP FUNCTION IF EXISTS GetParticipant (
    id_value VARCHAR(10));
CREATE OR REPLACE FUNCTION GetParticipant (
    id_value VARCHAR(10))
RETURNS TABLE (id VARCHAR(10), gender VARCHAR(1), 
    org_id VARCHAR(10), name VARCHAR(20))
AS $$
    BEGIN
        RETURN QUERY SELECT * FROM Participant 
        WHERE Participant.id = id_value;
    END $$
LANGUAGE plpgsql
STABLE;

DROP FUNCTION IF EXISTS GetLeg (leg_value INT);
CREATE OR REPLACE FUNCTION GetLeg (leg_value INT)
RETURNS TABLE (leg INT)
AS $$
    BEGIN
        RETURN QUERY SELECT * FROM Leg 
        WHERE Leg.leg = leg_value;
    END $$
LANGUAGE plpgsql
STABLE;

DROP FUNCTION IF EXISTS GetStroke (stroke_value VARCHAR(20));
CREATE OR REPLACE FUNCTION GetStroke (stroke_value VARCHAR(20))
RETURNS TABLE (stroke VARCHAR(20))
AS $$
    BEGIN
        RETURN QUERY SELECT * FROM Stroke 
        WHERE Stroke.stroke = stroke_value;
    END $$
LANGUAGE plpgsql
STABLE;

DROP FUNCTION IF EXISTS GetDistance (distance_value INT);
CREATE OR REPLACE FUNCTION GetDistance (distance_value INT)
RETURNS TABLE (distance INT)
AS $$
    BEGIN
        RETURN QUERY SELECT * FROM Distance 
        WHERE Distance.distance = distance_value;
    END $$
LANGUAGE plpgsql
STABLE;

DROP FUNCTION IF EXISTS GetEvent (id_value VARCHAR(10));
CREATE OR REPLACE FUNCTION GetEvent (id_value VARCHAR(10))
RETURNS TABLE (id VARCHAR(10), gender VARCHAR(1), distance INT)
AS $$
    BEGIN
        RETURN QUERY SELECT * FROM Event 
        WHERE Event.id = id_value;
    END $$
LANGUAGE plpgsql
STABLE;

DROP FUNCTION IF EXISTS GetStrokeOf (
    event_id_value VARCHAR(10), leg_value INT);
CREATE OR REPLACE FUNCTION GetStrokeOf (
    event_id_value VARCHAR(10), leg_value INT)
RETURNS TABLE (event_id VARCHAR(10), leg INT, stroke VARCHAR(20))
AS $$
    BEGIN
        RETURN QUERY SELECT * FROM StrokeOf 
        WHERE StrokeOf.event_id = event_id_value 
        AND StrokeOf.leg = leg_value;
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







---read all data from table
DROP FUNCTION IF EXISTS GetAllOrg();
CREATE OR REPLACE FUNCTION GetAllOrg ()
RETURNS TABLE (id VARCHAR(10), name VARCHAR(20), is_univ BOOLEAN)
AS $$
    BEGIN
        RETURN QUERY SELECT * FROM Org;
    END $$
LANGUAGE plpgsql
STABLE;

DROP FUNCTION IF EXISTS GetAllMeet ();
CREATE OR REPLACE FUNCTION GetAllMeet ()
RETURNS TABLE (name VARCHAR(20), start_date DATE, 
    num_days INT, org_id VARCHAR(10))
AS $$
    BEGIN
        RETURN QUERY SELECT * FROM Meet;
    END $$
LANGUAGE plpgsql
STABLE;

DROP FUNCTION IF EXISTS GetAllParticipant ();
CREATE OR REPLACE FUNCTION GetAllParticipant ()
RETURNS TABLE (id VARCHAR(10), gender VARCHAR(1), 
    org_id VARCHAR(10), name VARCHAR(20))
AS $$
    BEGIN
        RETURN QUERY SELECT * FROM Participant;
    END $$
LANGUAGE plpgsql
STABLE;

DROP FUNCTION IF EXISTS GetAllLeg ();
CREATE OR REPLACE FUNCTION GetAllLeg ()
RETURNS TABLE (leg INT)
AS $$
    BEGIN
        RETURN QUERY SELECT * FROM Leg;
    END $$
LANGUAGE plpgsql
STABLE;

DROP FUNCTION IF EXISTS GetAllStroke ();
CREATE OR REPLACE FUNCTION GetAllStroke ()
RETURNS TABLE (stroke VARCHAR(20))
AS $$
    BEGIN
        RETURN QUERY SELECT * FROM Stroke;
    END $$
LANGUAGE plpgsql
STABLE;

DROP FUNCTION IF EXISTS GetAllDistance ();
CREATE OR REPLACE FUNCTION GetAllDistance ()
RETURNS TABLE (distance INT)
AS $$
    BEGIN
        RETURN QUERY SELECT * FROM Distance;
    END $$
LANGUAGE plpgsql
STABLE;

DROP FUNCTION IF EXISTS GetAllEvent ();
CREATE OR REPLACE FUNCTION GetAllEvent ()
RETURNS TABLE (id VARCHAR(10), gender VARCHAR(1), distance INT)
AS $$
    BEGIN
        RETURN QUERY SELECT * FROM Event;
    END $$
LANGUAGE plpgsql
STABLE;

DROP FUNCTION IF EXISTS GetAllStrokeOf ();
CREATE OR REPLACE FUNCTION GetAllStrokeOf ()
RETURNS TABLE (event_id VARCHAR(10), leg INT, stroke VARCHAR(20))
AS $$
    BEGIN
        RETURN QUERY SELECT * FROM StrokeOf;
    END $$
LANGUAGE plpgsql
STABLE;


DROP FUNCTION IF EXISTS GetAllHeat ();
CREATE OR REPLACE FUNCTION GetAllHeat ()
RETURNS TABLE (id VARCHAR(10), event_id VARCHAR(10), 
    meet_name VARCHAR(20))
AS $$
    BEGIN
        RETURN QUERY SELECT * FROM Heat;
    END $$
LANGUAGE plpgsql
STABLE;


DROP FUNCTION IF EXISTS GetAllSwim ();
CREATE OR REPLACE FUNCTION GetAllSwim ()
RETURNS TABLE (
    heat_id VARCHAR(10), event_id VARCHAR(10), 
    meet_name VARCHAR(20), participant_id VARCHAR(10), 
    leg INT, t DECIMAL)
AS $$
    BEGIN
        RETURN QUERY SELECT * FROM Swim;
    END $$
LANGUAGE plpgsql
STABLE;




--------------------------------------
--------------------------------------
----------upsert functions------------
--------------------------------------
--------------------------------------

DROP FUNCTION IF EXISTS UpsertOrg (
    id_value VARCHAR(10), 
    name_value VARCHAR(20), 
    is_univ_value BOOLEAN);
CREATE OR REPLACE FUNCTION UpsertOrg (
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



DROP FUNCTION IF EXISTS UpsertMeet (
    name_value VARCHAR(20), 
    start_date_value DATE, 
    num_days_value INT, 
    org_id_value VARCHAR(10));
CREATE OR REPLACE FUNCTION UpsertMeet (
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





DROP FUNCTION IF EXISTS UpsertParticipant (
    id_value VARCHAR(10), 
    gender_value VARCHAR(1), 
    org_id_value VARCHAR(10), 
    name_value VARCHAR(20));
CREATE OR REPLACE FUNCTION UpsertParticipant (
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



DROP FUNCTION IF EXISTS UpsertLeg (leg_value INT);
CREATE OR REPLACE FUNCTION UpsertLeg (leg_value INT)
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



DROP FUNCTION IF EXISTS UpsertStroke (stroke_value VARCHAR(20));
CREATE OR REPLACE FUNCTION UpsertStroke (stroke_value VARCHAR(20))
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



DROP FUNCTION IF EXISTS UpsertDistance (distance_value INT);
CREATE OR REPLACE FUNCTION UpsertDistance (distance_value INT)
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



DROP FUNCTION IF EXISTS UpsertEvent (
    id_value VARCHAR(10), 
    gender_value VARCHAR(1), 
    distance_value INT);
CREATE OR REPLACE FUNCTION UpsertEvent (
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





DROP FUNCTION IF EXISTS UpsertStrokeOf (
    event_id_value VARCHAR(10), 
    leg_value INT, 
    stroke_value VARCHAR(20));
CREATE OR REPLACE FUNCTION UpsertStrokeOf (
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




DROP FUNCTION IF EXISTS UpsertHeat (
    id_value VARCHAR(10), 
    event_id_value VARCHAR(10), 
    meet_name_value VARCHAR(20));
CREATE OR REPLACE FUNCTION UpsertHeat (
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




DROP FUNCTION IF EXISTS UpsertSwim (
    heat_id_value VARCHAR(10), 
    event_id_value VARCHAR(10), 
    meet_name_value VARCHAR(20),
    participant_id_value VARCHAR(10),
    leg_value INT,
    t_value DECIMAL);
CREATE OR REPLACE FUNCTION  UpsertSwim (
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





--------------------------------------
--------------------------------------
-------------Heat Sheet --------------
----------functions and views---------
--------------------------------------
--------------------------------------

--useful view to facilitate heat sheet functions

--individual event ids
DROP VIEW IF EXISTS individual_events;
CREATE VIEW individual_events AS
SELECT event_id
        From StrokeOf 
        GROUP BY event_id
        HAVING Count(*) = 1;

--relay event ids
DROP VIEW IF EXISTS relay_events;
CREATE VIEW relay_events AS
SELECT event_id
        From StrokeOf 
        GROUP BY event_id
        HAVING Count(*) > 1;



--heat sheet of individual events
DROP VIEW IF EXISTS meet_individual_info;
CREATE VIEW meet_individual_info AS
SELECT sw.meet_name, 
    sw.event_id,  e.gender, e.distance, s.stroke, 
    sw.heat_id, 
    Org.id AS org_id, Org.name AS school, 
    sw.participant_id, p.name AS swimmer_name, 
    t, 
    RANK() OVER   
    (PARTITION BY sw.meet_name, sw.event_id, sw.participant_id 
        ORDER BY sw.t ASC) 
    AS personal_rank,
    --only choose the best time of a participant in an event
    CASE WHEN ( RANK() OVER   
    (PARTITION BY sw.meet_name, sw.event_id, sw.participant_id 
        ORDER BY sw.t ASC))=1
    THEN RANK() OVER   
    (PARTITION BY sw.meet_name, sw.event_id ORDER BY sw.t ASC) 
    END AS event_rank 
FROM Swim sw
INNER JOIN individual_events ind 
    ON ind.event_id = sw.event_id
INNER JOIN Event e 
    ON sw.event_id = e.id
INNER JOIN StrokeOf s 
    ON s.event_id = e.id AND s.leg=sw.leg
INNER JOIN Participant p 
    ON p.id = sw.participant_id
INNER JOIN Org 
    ON p.org_id = Org.id
ORDER BY sw.meet_name, sw.event_id, 
    sw.heat_id,
    event_rank,
    school ASC
;



--time and rank of relay events
DROP VIEW IF EXISTS meet_group_time_rank CASCADE;
CREATE VIEW meet_group_time_rank AS
SELECT sw.meet_name, 
    sw.event_id, e.gender, e.distance, s.stroke,
    sw.heat_id, 
    --only choose the best time of the group in an event
    CASE WHEN  
        (RANK() OVER   
        (PARTITION BY sw.meet_name, sw.event_id, Org.id 
         ORDER BY Sum(t) ASC) )=1
    THEN 
        RANK() OVER   
        (PARTITION BY sw.meet_name, sw.event_id ORDER BY Sum(t) ASC) 
    END AS group_event_rank,
    Sum(t) AS group_time,
    Org.id AS org_id, Org.name AS school,
    RANK() OVER   
    (PARTITION BY sw.meet_name, sw.event_id, Org.id 
        ORDER BY Sum(t) ASC) 
        AS school_rank
    
FROM Swim sw
INNER JOIN relay_events r ON r.event_id = sw.event_id
INNER JOIN Event e ON sw.event_id = e.id
INNER JOIN StrokeOf s ON s.event_id = e.id AND s.leg=sw.leg
INNER JOIN Participant p ON p.id = sw.participant_id
INNER JOIN Org ON p.org_id = Org.id
GROUP BY sw.meet_name, 
    sw.event_id, e.gender, e.distance, s.stroke, 
    sw.heat_id, 
    Org.id, Org.name
ORDER BY sw.meet_name, sw.event_id, group_event_rank ASC
;


--heat sheet of relay events
DROP VIEW IF EXISTS meet_group_info;
CREATE VIEW meet_group_info AS
SELECT sw.meet_name, 
    sw.event_id,  e.gender, e.distance, s.stroke, 
    sw.heat_id, 
    m.group_event_rank,
    m.group_time,
    Org.id AS org_id, Org.name AS school, 
    sw.leg,
    sw.participant_id, p.name AS swimmer_name, 
    t AS individual_time
FROM Swim sw
INNER JOIN relay_events r ON r.event_id = sw.event_id
INNER JOIN Event e ON sw.event_id = e.id
INNER JOIN StrokeOf s ON s.event_id = e.id AND s.leg=sw.leg
INNER JOIN Participant p ON p.id = sw.participant_id
INNER JOIN Org ON p.org_id = Org.id
INNER JOIN meet_group_time_rank m 
ON sw.meet_name=m.meet_name 
    AND sw.event_id=m.event_id
    AND sw.heat_id=m.heat_id
    AND Org.id=m.org_id

ORDER BY sw.meet_name, sw.event_id, sw.heat_id, 
    group_event_rank, leg ASC
;




--heat sheet functions

--1. For a Meet, display a Heat Sheet.
--get heat sheet of individual events in a meet
DROP FUNCTION IF EXISTS GetMeetInfoInd(
    meet_name_value VARCHAR(20));
CREATE OR REPLACE FUNCTION GetMeetInfoInd (
    meet_name_value VARCHAR(20))
RETURNS TABLE 
(gender VARCHAR(1), distance INT, stroke VARCHAR(20),
 heat_id VARCHAR(10),
 org_id VARCHAR(10), 
 school VARCHAR(20),
 participant_id VARCHAR(10),
 swimmer_name VARCHAR(20), 
 event_rank bigint ,
 t DECIMAL)
AS $$
    BEGIN
        RETURN QUERY (SELECT 
        m.gender, m.distance, m.stroke,
        m.heat_id, 
        m.org_id, m.school,
        m.participant_id, m.swimmer_name, 
        m.event_rank, m.t
        FROM meet_individual_info m 
        WHERE m.meet_name = meet_name_value);
    END $$
LANGUAGE plpgsql
STABLE;


--get heat sheet of group events with individual times (of a meet)
DROP FUNCTION IF EXISTS GetMeetInfoGroup(
    meet_name_value VARCHAR(20));
CREATE OR REPLACE FUNCTION GetMeetInfoGroup (
    meet_name_value VARCHAR(20))
RETURNS TABLE 
(gender VARCHAR(1), distance INT, stroke VARCHAR(20),
 heat_id VARCHAR(10),
 group_event_rank bigint ,
 group_time DECIMAL,
 org_id VARCHAR(10), 
 school VARCHAR(20),
 leg INT,
 participant_id VARCHAR(10),
 swimmer_name VARCHAR(20), 
 individual_time DECIMAL
 )
AS $$
    BEGIN
        RETURN QUERY (SELECT 
        m.gender, m.distance, m.stroke,
        m.heat_id, 
        m.group_event_rank,
        m.group_time,
        m.org_id, m.school,
        m.leg,
        m.participant_id, m.swimmer_name, 
        m.individual_time
        FROM meet_group_info m WHERE m.meet_name = meet_name_value);
    END $$
LANGUAGE plpgsql
STABLE;


--get heat sheet of relay events inf a meet
DROP FUNCTION IF EXISTS GetMeetInfoGroupOnly(
    meet_name_value VARCHAR(20));
CREATE OR REPLACE FUNCTION GetMeetInfoGroupOnly (
    meet_name_value VARCHAR(20))
RETURNS TABLE 
(gender VARCHAR(1), distance INT, stroke VARCHAR(20),
 heat_id VARCHAR(10),
 group_event_rank bigint ,
 group_time DECIMAL,
 org_id VARCHAR(10), 
 school VARCHAR(20)
 )
AS $$
    BEGIN
        RETURN QUERY (SELECT 
        m.gender, m.distance, m.stroke,
        m.heat_id, 
        m.group_event_rank,
        m.group_time,
        m.org_id, m.school
        FROM meet_group_time_rank m WHERE m.meet_name = meet_name_value);
    END $$
LANGUAGE plpgsql
STABLE;







--2.
--For a Participant and Meet, display a Heat Sheet 
--limited to just that swimmer,
--including any relays they are in.

--get heat sheet of a partcipant in individual events of a meet
DROP FUNCTION IF EXISTS GetParticipantInfoInd(
    meet_name_value VARCHAR(20), 
    participant_id_value VARCHAR(10));
CREATE OR REPLACE FUNCTION GetParticipantInfoInd (
    meet_name_value VARCHAR(20), 
    participant_id_value VARCHAR(10))
RETURNS TABLE 
(gender VARCHAR(1), distance INT, stroke VARCHAR(20),
 heat_id VARCHAR(10),
 org_id VARCHAR(10), 
 school VARCHAR(20),
 event_rank bigint,
 t DECIMAL)
AS $$
    BEGIN
        RETURN QUERY (SELECT 
        m.gender, m.distance, m.stroke,
        m.heat_id, 
        m.org_id, m.school,
        m.event_rank, m.t
        FROM meet_individual_info m 
        WHERE m.meet_name = meet_name_value
        AND m.participant_id=participant_id_value
        );
    END $$
LANGUAGE plpgsql
STABLE;


--get heat sheet of a partcipant in group events of a meet
DROP FUNCTION IF EXISTS GetParticipantInfoGroup(
    meet_name_value VARCHAR(20), 
    participant_id_value VARCHAR(10));
CREATE OR REPLACE FUNCTION GetParticipantInfoGroup (
    meet_name_value VARCHAR(20), 
    participant_id_value VARCHAR(10))
RETURNS TABLE 
(gender VARCHAR(1), distance INT, stroke VARCHAR(20),
 heat_id VARCHAR(10),
 group_event_rank bigint ,
 group_time DECIMAL,
 org_id VARCHAR(10), 
 school VARCHAR(20),
 leg INT,
 individual_time DECIMAL)
AS $$
    BEGIN
        RETURN QUERY (SELECT 
        m.gender, m.distance, m.stroke,
        m.heat_id, 
        m.group_event_rank,
        m.group_time,
        m.org_id, m.school,
        m.leg,
        m.individual_time
        FROM meet_group_info m 
        WHERE m.meet_name = meet_name_value
        AND m.participant_id=participant_id_value
        );
    END $$
LANGUAGE plpgsql
STABLE;





--3.
--For a School and Meet, display a Heat Sheet 
--limited to just that School’s swimmers

-- get heat sheet of all partcipants of the school
-- in individual events of a meet
DROP FUNCTION IF EXISTS GetSchoolInfoInd(
    meet_name_value VARCHAR(20), 
    org_id_value VARCHAR(10));
CREATE OR REPLACE FUNCTION GetSchoolInfoInd (
    meet_name_value VARCHAR(20), 
    org_id_value VARCHAR(10))
RETURNS TABLE 
(gender VARCHAR(1), distance INT, stroke VARCHAR(20),
 heat_id VARCHAR(10),
 participant_id VARCHAR(10),
 swimmer_name VARCHAR(20), 
 event_rank bigint,
 t DECIMAL)
AS $$
    BEGIN
        RETURN QUERY (SELECT 
        m.gender, m.distance, m.stroke,
        m.heat_id, 
        m.participant_id, m.swimmer_name, 
        m.event_rank, m.t
        FROM meet_individual_info m 
        WHERE m.meet_name = meet_name_value
        AND m.org_id=org_id_value
        );
    END $$
LANGUAGE plpgsql
STABLE;



-- get heat sheet of all partcipants of the school
-- in relay events of a meet
DROP FUNCTION IF EXISTS GetSchoolInfoGroup(
    meet_name_value VARCHAR(20), 
    org_id_value VARCHAR(10));
CREATE OR REPLACE FUNCTION GetSchoolInfoGroup (
    meet_name_value VARCHAR(20), 
    org_id_value VARCHAR(10))
RETURNS TABLE 
(gender VARCHAR(1), distance INT, stroke VARCHAR(20),
 heat_id VARCHAR(10),
 group_event_rank bigint ,
 group_time DECIMAL,
 participant_id VARCHAR(10),
 swimmer_name VARCHAR(20), 
 leg INT,
 individual_time DECIMAL)
AS $$
    BEGIN
        RETURN QUERY (SELECT 
        m.gender, m.distance, m.stroke,
        m.heat_id, 
        m.group_event_rank,
        m.group_time,
        m.participant_id, m.swimmer_name, 
        m.leg,
        m.individual_time
        FROM meet_group_info m 
        WHERE m.meet_name = meet_name_value
        AND m.org_id=org_id_value
        );
    END $$
LANGUAGE plpgsql
STABLE;






--4.
--For a School and Meet, display 
--just the names of the competing swimmers.
DROP FUNCTION IF EXISTS GetSchoolSwimmers(
    meet_name_value VARCHAR(20), 
    org_id_value VARCHAR(10));
CREATE OR REPLACE FUNCTION GetSchoolSwimmers (
    meet_name_value VARCHAR(20), 
    org_id_value VARCHAR(10))
RETURNS TABLE 
(participant_id VARCHAR(10),
 swimmer_name VARCHAR(20)
)
AS $$
    BEGIN
        RETURN QUERY (SELECT DISTINCT p.id, p.name
        FROM Swim s 
        INNER JOIN Participant p
        On p.org_id = org_id_value
        AND p.id=s.participant_id
        WHERE s.meet_name = meet_name_value
        ORDER BY p.name
        );
    END $$
LANGUAGE plpgsql
STABLE;





--5.
--For an Event and Meet, display all results sorted by time.
--Include the heat, swimmer(s) name(s), and rank.

--given event_id
--return 
--event gender, distance, stroke
--event type as '' empty string for an individual event
--event type as 'relay' string for a relay event
DROP FUNCTION IF EXISTS GetEventType(
    event_id_value VARCHAR(10));
CREATE OR REPLACE FUNCTION GetEventType (
    event_id_value VARCHAR(10))
RETURNS VARCHAR(10)

AS $$
    DECLARE
        event_type VARCHAR(10);
        legs INT;
    BEGIN
        SELECT COUNT(*) into legs
        FROM StrokeOf 
        WHERE event_id=event_id_value;
        IF legs > 1 THEN
            event_type = 'relay';
        ELSE
            event_type = '';
        END IF;
        RETURN event_type;
    END $$
LANGUAGE plpgsql
STABLE;



--given event_id
--return event.gender, event_distance, event_strok
DROP FUNCTION IF EXISTS GetEventName(
    event_id_value VARCHAR(10));
CREATE OR REPLACE FUNCTION GetEventName (
    event_id_value VARCHAR(10))
RETURNS TABLE
(
    gender VARCHAR(1),
    distance INT,
    stroke VARCHAR(20)
)
AS $$
    BEGIN
        RETURN QUERY 
        (SELECT DISTINCT
         e.gender, e.distance,
         s.stroke
         FROM Event e
         INNER JOIN StrokeOf s
         ON e.id=s.event_id
         WHERE e.id=event_id_value
        );
    END $$
LANGUAGE plpgsql
STABLE;


--return individual event info
DROP FUNCTION IF EXISTS GetEventInfoInd(
    meet_name_value VARCHAR(20), 
    event_id_value VARCHAR(10));
CREATE OR REPLACE FUNCTION GetEventInfoInd (
    meet_name_value VARCHAR(20), 
    event_id_value VARCHAR(10))
RETURNS TABLE 
(t DECIMAL,
 event_rank bigint,
 heat_id VARCHAR(10),
 participant_id VARCHAR(10),
 swimmer_name VARCHAR(20),
 org_id VARCHAR(10),
 school VARCHAR(20)
 )
AS $$
    BEGIN
        RETURN QUERY (SELECT 
        m.t, m.event_rank,
        m.heat_id, 
        m.participant_id, m.swimmer_name, 
        m.org_id, m.school
        FROM meet_individual_info m 
        WHERE m.meet_name = meet_name_value
        AND m.event_id=event_id_value
        ORDER BY m.t
        );
    END $$
LANGUAGE plpgsql
STABLE;


--return relay event info with individual times
DROP FUNCTION IF EXISTS GetEventInfoGroup(
    meet_name_value VARCHAR(20), 
    event_id_value VARCHAR(10));
CREATE OR REPLACE FUNCTION GetEventInfoGroup (
    meet_name_value VARCHAR(20), 
    event_id_value VARCHAR(10))
RETURNS TABLE 
(group_time DECIMAL,
 group_event_rank bigint ,
 heat_id VARCHAR(10),
 org_id VARCHAR(10),
 school VARCHAR(20),
 leg INT,
 participant_id VARCHAR(10),
 swimmer_name VARCHAR(20), 
 individual_time DECIMAL)
AS $$
    BEGIN
        RETURN QUERY (SELECT 
        m.group_time,
        m.group_event_rank,
        m.heat_id, 
        m.org_id, m.school,
        m.leg,
        m.participant_id, m.swimmer_name, 
        m.individual_time
        FROM meet_group_info m 
        WHERE m.meet_name = meet_name_value
        AND m.event_id=event_id_value
        ORDER BY m.group_time
        );
    END $$
LANGUAGE plpgsql
STABLE;


