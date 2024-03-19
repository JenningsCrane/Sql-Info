/*
CREATE DATABASE info1;
*/

CREATE TABLE IF NOT EXISTS Peers(
    Nickname TEXT NOT NULL PRIMARY KEY,
    Birthday DATE NOT NULL,

    UNIQUE(Nickname)
);

CREATE TABLE IF NOT EXISTS Tasks(
    Title       TEXT NOT NULL PRIMARY KEY,
    ParentTask  TEXT NULL     DEFAULT NULL,
    MaxXP       INT  NOT NULL,

    UNIQUE(Title),

    CONSTRAINT fk_ParentTask_to_Tasks FOREIGN KEY(ParentTask) REFERENCES Tasks(Title)
);

CREATE TABLE IF NOT EXISTS Checks(
    ID           SERIAL PRIMARY KEY,
    PeerNickname TEXT   NOT NULL,
    TaskTitle    TEXT   NOT NULL,
    Date         DATE   NOT NULL DEFAULT NOW(),

    CONSTRAINT fk_PeerNickname_to_Peers FOREIGN KEY(PeerNickname) REFERENCES Peers(Nickname),
    CONSTRAINT fk_TaskTitle_to_Tasks FOREIGN KEY(TaskTitle) REFERENCES Tasks(Title)
);

DO $$
BEGIN
CREATE TYPE CheckStatus AS ENUM('Start', 'Success', 'Failure');
EXCEPTION WHEN DUPLICATE_OBJECT THEN RAISE NOTICE 'CheckStatus exists, skipping...';
END
$$;

CREATE TABLE IF NOT EXISTS P2P(
    ID                   SERIAL      PRIMARY KEY,
    CheckID              INT         NOT NULL,
    CheckingPeerNickname TEXT        NOT NULL,
    State                CheckStatus NOT NULL,
    Time                 TIME        NOT NULL DEFAULT NOW(),

    CONSTRAINT fk_CheckID_to_Checks FOREIGN KEY(CheckID) REFERENCES Checks(ID)
);

CREATE TABLE IF NOT EXISTS Verter(
    ID      SERIAL      PRIMARY KEY,
    CheckID INT         NOT NULL,
    State   CheckStatus NOT NULL,
    Time    TIME        NOT NULL DEFAULT NOW(),

    CONSTRAINT fk_CheckID_to_Checks FOREIGN KEY(CheckID) REFERENCES Checks(ID)
);

CREATE TABLE IF NOT EXISTS TransferredPoints(
    Id                   SERIAL PRIMARY KEY,
    CheckingPeerNickname TEXT   NOT NULL,
    CheckedPeerNickname  TEXT   NOT NULL,
    PointsAmount         INT    NOT NULL DEFAULT 0,

    CONSTRAINT fk_CheckingPeerNickname_to_Peers FOREIGN KEY(CheckingPeerNickname) REFERENCES Peers(Nickname),
    CONSTRAINT fk_CheckedPeerNickname_to_Peers FOREIGN KEY(CheckedPeerNickname) REFERENCES Peers(Nickname)
);

CREATE TABLE IF NOT EXISTS Friends(
    ID            SERIAL PRIMARY KEY,
    Peer1Nickname TEXT   NOT NULL,
    Peer2Nickname TEXT   NOT NULL,

    CONSTRAINT fk_Peer1Nickname_to_Peers FOREIGN KEY(Peer1Nickname) REFERENCES Peers(Nickname),
    CONSTRAINT fk_Peer2Nickname_to_Peers FOREIGN KEY(Peer2Nickname) REFERENCES Peers(Nickname)
);

CREATE TABLE IF NOT EXISTS Recommendations(
    ID                      SERIAL PRIMARY KEY,
    PeerNickname            TEXT   NOT NULL,
    RecommendedPeerNickname TEXT   NOT NULL,

    CONSTRAINT fk_PeerNickname_to_Peers FOREIGN KEY(PeerNickname) REFERENCES Peers(Nickname),
    CONSTRAINT fk_RecommendedPeerNickname_to_Peers FOREIGN KEY(PeerNickname) REFERENCES Peers(Nickname)
);

CREATE TABLE IF NOT EXISTS XP(
    ID       SERIAL PRIMARY KEY,
    CheckID  INT    NOT NULL,
    XPAmount INT    NOT NULL,

    CONSTRAINT fk_CheckID_to_Checks FOREIGN KEY(CheckID) REFERENCES Checks(ID)
);

CREATE TABLE IF NOT EXISTS TimeTracking(
    ID           SERIAL   PRIMARY KEY,
    PeerNickname TEXT     NOT NULL,
    Date         DATE     NOT NULL DEFAULT NOW(),
    Time         TIME     NOT NULL DEFAULT NOW(),
    State        SMALLINT NOT NULL
);

/*
DROP TABLE IF EXISTS TimeTracking;
DROP TABLE IF EXISTS XP;
DROP TABLE IF EXISTS Recommendations;
DROP TABLE IF EXISTS Friends;
DROP TABLE IF EXISTS TransferredPoints;
DROP TABLE IF EXISTS Verter;
DROP TABLE IF EXISTS P2P;
DROP TABLE IF EXISTS Checks;
DROP TABLE IF EXISTS Tasks;
DROP TABLE IF EXISTS Peers;
*/

CREATE OR REPLACE PROCEDURE export_Table_as_csv(IN tableName TEXT, IN outFile TEXT, IN delim TEXT default ',') AS
$$
DECLARE
    query TEXT;
BEGIN
    query := 'COPY (SELECT * FROM ' || tableName || ') TO ''' || outFile || ''' DELIMITER ''' || delim || ''' CSV HEADER';
    EXECUTE query;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE export_Peers_as_csv(IN outFile TEXT, IN delim TEXT default ',') AS
$$
CALL export_Table_as_csv('Peers', outFile);
$$ LANGUAGE SQL;

CREATE OR REPLACE PROCEDURE export_Tasks_as_csv(IN outFile TEXT, IN delim TEXT default ',') AS
$$
CALL export_Table_as_csv('Tasks', outFile);
$$ LANGUAGE SQL;

CREATE OR REPLACE PROCEDURE export_Checks_as_csv(IN outFile TEXT, IN delim TEXT default ',') AS
$$
CALL export_Table_as_csv('Checks', outFile);
$$ LANGUAGE SQL;

CREATE OR REPLACE PROCEDURE export_P2P_as_csv(IN outFile TEXT, IN delim TEXT default ',') AS
$$
CALL export_Table_as_csv('P2P', outFile);
$$ LANGUAGE SQL;

CREATE OR REPLACE PROCEDURE export_Verter_as_csv(IN outFile TEXT, IN delim TEXT default ',') AS
$$
CALL export_Table_as_csv('Verter', outFile);
$$ LANGUAGE SQL;

CREATE OR REPLACE PROCEDURE export_transferredpoints_as_csv(IN outFile TEXT, IN delim TEXT default ',') AS
$$
CALL export_Table_as_csv('transferredpoints', outFile);
$$ LANGUAGE SQL;

CREATE OR REPLACE PROCEDURE export_Friends_as_csv(IN outFile TEXT, IN delim TEXT default ',') AS
$$
CALL export_Table_as_csv('Friends', outFile);
$$ LANGUAGE SQL;

CREATE OR REPLACE PROCEDURE export_Recommendations_as_csv(IN outFile TEXT, IN delim TEXT default ',') AS
$$
CALL export_Table_as_csv('Recommendations', outFile);
$$ LANGUAGE SQL;

CREATE OR REPLACE PROCEDURE export_XP_as_csv(IN outFile TEXT, IN delim TEXT default ',') AS
$$
CALL export_Table_as_csv('XP', outFile);
$$ LANGUAGE SQL;

CREATE OR REPLACE PROCEDURE export_TimeTrackings_as_csv(IN outFile TEXT, IN delim TEXT default ',') AS
$$
CALL export_Table_as_csv('TimeTrackings', outFile);
$$ LANGUAGE SQL;

INSERT INTO Peers(Nickname,   Birthday    ) VALUES
                 ('mrGod',    '0001-01-01'),
                 ('clairere', '2000-01-01'),
                 ('jenningc', '2000-01-01'),
                 ('lynseypi', '2000-01-01'),
                 ('lolkek',   '2000-01-01');

INSERT INTO Tasks(Title,                ParentTask,           MaxXP) VALUES
                 ('C0_SimpleBashUtils', NULL,                 250  ),
                 ('C1_s21_string+',     'C0_SimpleBashUtils', 500  ),
                 ('C2_s21_decimal',     'C1_s21_string+',     350  ),
                 ('C3_s21_matrix',      'C2_s21_decimal',     200  ),
                 ('C4_SmartCalc_v1.0',  'C3_s21_matrix',      500  ),
                 ('C5_3DViewer_v1.0',   'C4_SmartCalc_v1.0',  750  );

INSERT INTO Checks(PeerNickname, TaskTitle,             Date        ) VALUES
                  ('lolkek',     'C0_SimpleBashUtils',  '2023-02-01'),
                  ('jenningc',   'C0_SimpleBashUtils',  '2023-04-01'),
                  ('lynseypi',   'C0_SimpleBashUtils',  '2023-05-01'),
                  ('clairere',   'C0_SimpleBashUtils',  '2023-06-01'),
                  ('clairere',   'C1_s21_string+',      '2023-07-01');

INSERT INTO P2P(CheckID, CheckingPeerNickname, State,     Time      ) VALUES
               (1,       'mrGod',             'Start',   '12:00:00'),
               (1,       'mrGod',             'Success', '13:00:00'),
               (2,       'lolkek',            'Start',   '11:00:00'),
               (2,       'lolkek',            'Failure', '12:00:00'),
               (3,       'lolkek',            'Start',   '13:00:00'),
               (3,       'lolkek',            'Success', '14:00:00'),
               (4,       'lynseypi',          'Start',   '15:00:00'),
               (4,       'lynseypi',          'Success', '16:00:00'),
               (5,       'lynseypi',          'Start',   '10:00:00'),
               (5,       'lynseypi',          'Failure', '11:00:00');

INSERT INTO Verter(CheckID, State,    Time      ) VALUES
                  (1,      'Start',   '13:00:00'),
                  (1,      'Success', '13:30:00'),
                  (3,      'Start',   '14:00:00'),
                  (3,      'Failure', '14:30:00'),
                  (4,      'Start',   '16:00:00'),
                  (4,      'Success', '16:30:00');

INSERT INTO TransferredPoints(CheckingPeerNickname, CheckedPeerNickname, PointsAmount) VALUES
                            ('mrGod',              'clairere',           0           ),
                            ('mrGod',              'jenningc',           0           ),
                            ('mrGod',              'lynseypi',           0           ),
                            ('mrGod',              'lolkek',             1           ),
                            ('clairere',           'mrGod',              0           ),
                            ('clairere',           'jenningc',           0           ),
                            ('clairere',           'lynseypi',           0           ),
                            ('clairere',           'lolkek',             0           ),
                            ('jenningc',           'mrGod',              0           ),
                            ('jenningc',           'clairere',           0           ),
                            ('jenningc',           'lynseypi',           0           ),
                            ('jenningc',           'lolkek',             0           ),
                            ('lynseypi',           'mrGod',              0           ),
                            ('lynseypi',           'clairere',           2           ),
                            ('lynseypi',           'jenningc',           0           ),
                            ('lynseypi',           'lolkek',             0           ),
                            ('lolkek',             'mrGod',              0           ),
                            ('lolkek',             'clairere',           0           ),
                            ('lolkek',             'jenningc',           1           ),
                            ('lolkek',             'lynseypi',           1           );

INSERT INTO Friends(Peer1Nickname, Peer2Nickname) VALUES
                   ('mrGod',       'lolkek'     ),
                   ('mrGod',       'lynseypi'   ),
                   ('jenningc',    'lynseypi'   ),
                   ('jenningc',    'clairere'   ),
                   ('clairere',    'lynseypi'   );

INSERT INTO Recommendations(PeerNickname, RecommendedPeerNickname) VALUES
                           ('mrGod',     'lolkek'                ),
                           ('jenningc',  'lynseypi'              ),
                           ('clairere',  'lolkek'                ),
                           ('lolkek',    'jenningc'              ),
                           ('clairere',  'lynseypi'              );

INSERT INTO XP(CheckID, XPAmount) VALUES
              (1,       250     ),
              (4,       250     );

INSERT INTO TimeTracking(PeerNickname, Date,         Time,       State) VALUES
                         ('clairere',   '2023-06-01', '13:37',    1    ),
                         ('clairere',   '2023-06-01', '15:48',    2    ),
                         ('jenningc',   '2023-04-01', '10:21',    1    ),
                         ('jenningc',   '2023-04-01', '16:42',    2    ),
                         ('lynseypi',   '2023-05-01', '12:48',    1    ),
                         ('lynseypi',   '2023-05-02', '02:30',    2    ),
                         ('lolkek',     '2023-06-02', '01:30',    1    );
