
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
    is_univ BOOLEAN,
    PRIMARY KEY (id),
    --one name corresponds to only one org or one university
    CONSTRAINT un_name_is_univ UNIQUE(name, is_univ)
);


CREATE TABLE Meet (
    name VARCHAR(20),
    start_date DATE,
    num_days INT,	--can be unknown
    org_id VARCHAR(10),
    PRIMARY KEY (name),
    FOREIGN KEY (org_id) REFERENCES Org (id),
    CONSTRAINT chk_num_days
    CHECK ((num_days > 0) and num_days is not NULL)
);


CREATE TABLE Participant (
    id VARCHAR(10),
    gender VARCHAR(1) NOT NULL,
    org_id VARCHAR(10) NOT NULL,
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
    PRIMARY KEY (stroke),
    --limit stroke to lower case, so the application should convert string to lowercase
    CONSTRAINT chk_stroke CHECK (stroke IN ('freestyle', 'butterfly', 'breaststroke', 'backstroke', 'medley'))
);

CREATE TABLE Distance (
    distance INT,
    PRIMARY KEY (distance),
    --limit distance to hundreds
    CONSTRAINT chk_distance CHECK (mod(distance,100) = 0)
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
    --a swimmer may start the swimming but fail to finish it for some reasons
    --so it can be null if that happens
    time REAL,	
    PRIMARY KEY (heat_id, event_id, meet_name, participant_id),
    FOREIGN KEY (heat_id, event_id, meet_name) REFERENCES Heat (id, event_id ,meet_name),
    FOREIGN KEY (participant_id) REFERENCES Participant (id),
    FOREIGN KEY (leg) REFERENCES Leg (leg),
    CONSTRAINT chk_time
    CHECK ((time > 0) and time is not NULL)
);









