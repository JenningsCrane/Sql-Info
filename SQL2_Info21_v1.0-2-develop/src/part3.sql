-- 1) Написать функцию, возвращающую таблицу TransferredPoints в более человекочитаемом виде

DROP FUNCTION IF EXISTS fnc_get_transferred_points CASCADE;

CREATE OR REPLACE FUNCTION fnc_get_transferred_points()
RETURNS TABLE (
    Peer1 TEXT,
    Peer2 TEXT,
    PointsAmount INT
)
AS $fnc_get_transferred_points$
BEGIN
    RETURN QUERY SELECT t1.checkingpeernickname              as Peer1,
                        t1.checkedpeernickname               as Peer2,
                        -1*(t1.pointsamount-t2.pointsamount) as PointsAmount
                   FROM transferredpoints AS t1
                   JOIN transferredpoints AS t2
                       ON t1.checkingpeernickname = t2.checkedpeernickname
                      AND t1.checkedpeernickname  = t2.checkingpeernickname
                      AND t1.id < t2.id;
END;
$fnc_get_transferred_points$ LANGUAGE plpgsql;

-- SELECT * FROM fnc_get_transferred_points();

-- $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$

-- 2) Написать функцию, которая возвращает таблицу вида:
--    ник пользователя, название проверенного задания, кол-во полученного XP

DROP FUNCTION IF EXISTS fnc_get_success_tasks_xp CASCADE;

CREATE OR REPLACE FUNCTION fnc_get_success_tasks_xp()
RETURNS TABLE (
    Peer TEXT,
    Task TEXT,
    XP INT
)
AS $fnc_get_success_tasks_xp$
BEGIN
    RETURN QUERY SELECT ch.peernickname AS Peer, SUBSTRING(ch.tasktitle FROM '^[^_]+') AS Task,  x.xpamount AS XP
                   FROM checks AS ch
                   JOIN p2p p on ch.id = p.checkid
                   JOIN verter v on ch.id = v.checkid
                   JOIN xp x on ch.id = x.checkid
                 WHERE p.state = 'Success' AND v.state = 'Success';
END;
$fnc_get_success_tasks_xp$ LANGUAGE plpgsql;

-- SELECT * FROM fnc_get_success_tasks_xp();

-- $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$

-- 3) Написать функцию, определяющую пиров, которые не выходили из кампуса в течение всего дня

DROP FUNCTION IF EXISTS func_day_tracking CASCADE;

CREATE OR REPLACE FUNCTION func_day_tracking(targetDate DATE)
RETURNS TABLE (peernickname TEXT) AS
$func_day_tracking$
BEGIN
    RETURN QUERY
    SELECT input.peernickname
    FROM (SELECT * FROM timetracking
          WHERE date = targetDate AND state = 1) AS input
    LEFT JOIN (SELECT * FROM timetracking
               WHERE date = targetDate AND state = 2) AS output
    ON input.peernickname = output.peernickname
    WHERE output.peernickname IS NULL;
END;
$func_day_tracking$ LANGUAGE plpgsql;

-- SELECT * FROM func_day_tracking('2023-06-02');

-- $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$

-- 4) Посчитать изменение в количестве пир поинтов каждого пира по таблице TransferredPoints

DROP PROCEDURE IF EXISTS count_change_points_origin CASCADE;

CREATE OR REPLACE PROCEDURE count_change_points_origin(INOUT result_cursor refcursor)
AS $count_change_points_origin$
BEGIN
    -- Открытие курсора и запись туда временной таблицы
    OPEN result_cursor FOR
    WITH tab1 AS ( -- сумма заработанных пирпоинтов для всех пиров
    SELECT checkingpeernickname, SUM(pointsamount) AS sum FROM transferredpoints GROUP BY checkingpeernickname
),
    tab2 AS (  -- сумма отданных пир поинтов для всех пиров
    SELECT checkedpeernickname, SUM(pointsamount) AS dif FROM transferredpoints GROUP BY checkedpeernickname
)
-- выводим пир и сколько он заработал минус сколько потерял
SELECT checkingpeernickname AS peer, tab1.sum-tab2.dif AS PointsChange FROM tab1
JOIN tab2 ON tab1.checkingpeernickname = tab2.checkedpeernickname
    ORDER BY 1,2;
END;
$count_change_points_origin$ LANGUAGE plpgsql;

-- BEGIN;
--     CALL count_change_points_origin('cursor');
--     FETCH ALL FROM cursor;
--     CLOSE cursor;
-- END;

-- $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$

-- 5) Посчитать изменение в количестве пир поинтов каждого пира по таблице, возвращаемой первой функцией из Part 3

DROP PROCEDURE IF EXISTS count_change_points_Part3_1 CASCADE;

CREATE OR REPLACE PROCEDURE count_change_points_Part3_1(INOUT result_cursor refcursor)
AS $count_change_points_Part3_1$
BEGIN
    -- Открытие курсора и запись туда временной таблицы
    OPEN result_cursor FOR
    WITH temp AS ( -- временная таблица для функции из Part 3.1
    SELECT * FROM fnc_get_transferred_points()
    ),
    tab1 AS ( -- сумма заработан0.2ных пирпоинтов для всех пиров
    SELECT peer1, SUM(pointsamount) AS sum FROM temp GROUP BY peer1
),
    tab2 AS (  -- сумма отданных пир поинтов для всех пиров
    SELECT peer2, SUM(pointsamount) AS dif FROM temp GROUP BY peer2
)
    -- выводим пир и сколько он заработал минус сколько потерял
    SELECT trab.peer, coalesce(tab2.dif, 0) - coalesce(tab1.sum, 0) AS pointschange FROM
    (SELECT peer1 AS peer FROM tab1
    UNION
    SELECT peer2 AS peer FROM tab2) AS trab
    FULL JOIN tab1 ON tab1.peer1 = trab.peer
    FULL JOIN tab2 ON tab2.peer2 = trab.peer
    ORDER BY 1, 2;
END;
$count_change_points_Part3_1$ LANGUAGE plpgsql;

-- BEGIN;
-- CALL count_change_points_Part3_1('cursor');
-- FETCH ALL FROM cursor;
-- CLOSE cursor;
-- END;


-- 6) Определить самое часто проверяемое задание за каждый день

DROP PROCEDURE IF EXISTS find_popular_task CASCADE;

CREATE OR REPLACE PROCEDURE find_popular_task(in cursor refcursor)
AS $find_popular_task$
BEGIN
OPEN cursor FOR
    WITH t1 AS (
    SELECT SUBSTRING(ch.tasktitle FROM '^[^_]+') AS tasktitle, Date, COUNT(ch.tasktitle) AS counts FROM checks ch
    GROUP BY Date, ch.tasktitle)

    SELECT Date, n.tasktitle FROM t1 AS n
    WHERE counts = (
       SELECT MAX(counts)
       FROM (SELECT * FROM t1 WHERE n.Date = t1.Date) AS res
    )
    ORDER BY 1 DESC, 2;
END;
$find_popular_task$ LANGUAGE plpgsql;

-- BEGIN;
--     CALL find_popular_task('cursor');
--     FETCH ALL IN "cursor";
--     CLOSE cursor;
-- END;

-- 7) Найти всех пиров, выполнивших весь заданный блок задач и дату завершения последнего задания
DROP PROCEDURE IF EXISTS get_peers_with_closed_branches CASCADE;

CREATE OR REPLACE PROCEDURE get_peers_with_closed_branches(INOUT cursor REFCURSOR,
    branch VARCHAR
) AS $get_peers_with_closed_branches$
BEGIN
    OPEN cursor FOR
    WITH t AS (
    -- Выводим имена пиров и даты сдачи проекта
        SELECT DISTINCT ch.peernickname, ch.Date, ch.tasktitle
        FROM checks ch
        JOIN verter v ON v.checkid = ch.id
        WHERE ch.tasktitle = (
            SELECT Title
            FROM Tasks
            WHERE Title ~ (branch || '[0-9]*')
            ORDER BY Title DESC
        LIMIT 1) AND v.state = 'Success'
        ORDER BY ch.Date
    )
    SELECT *
    FROM t;
END;
$get_peers_with_closed_branches$ LANGUAGE plpgsql;

-- -- Добавление проверки со статусом 'Start'.
--  CALL add_p2p_check('jenningc', 'lynseypi' , 'C5_3DViewer_v1.0', 'Start', '14:00:00');
--
--  -- Завершение проверки пира jenningc пиром lynseypi со статусом 'Success'.
--  CALL add_p2p_check('jenningc', 'lynseypi' , 'C5_3DViewer_v1.0', 'Success', '14:30:00');
--
-- -- Добавляем начало проверки вертером пира jenningc.
-- CALL add_verter_check('jenningc', 'C5_3DViewer_v1.0', 'Start', '14:35:00');
--
-- -- Завершаем проверку вертером пира jenningc.
-- CALL add_verter_check('jenningc', 'C5_3DViewer_v1.0', 'Success', '14:40:00');

-- BEGIN;
--     CALL get_peers_with_closed_branches('cursor', 'C');
--     FETCH ALL IN "cursor";
--     CLOSE cursor;
-- END;


-- 8) Определить, к какому пиру стоит идти на проверку каждому обучающемуся

DROP PROCEDURE IF EXISTS recommendedpeers_from_friends CASCADE;

CREATE OR REPLACE PROCEDURE recommendedpeers_from_friends(INOUT cursor REFCURSOR)
AS $recommendedpeers_from_friends$
BEGIN
    OPEN cursor FOR
    WITH t1 AS (
        SELECT t1.Peer, t1.recommendedpeernickname
        FROM (SELECT DISTINCT ON (Peer)
              p.Peer, r.recommendedpeernickname, count(r.recommendedpeernickname)
        FROM (SELECT peer1nickname AS Peer, peer2nickname AS Friend
            FROM Friends
            UNION ALL
            SELECT peer2nickname AS Peer, peer1nickname AS Friend
        FROM Friends ) AS p
    LEFT JOIN Recommendations r ON r.peernickname = p.friend
    WHERE r.recommendedpeernickname IS NOT NULL AND p.Peer <> r.recommendedpeernickname
    GROUP BY 1,2
    ORDER BY 1,3 DESC)
    t1)
    SELECT * FROM t1;
END;
$recommendedpeers_from_friends$ LANGUAGE plpgsql;

-- BEGIN;
--     CALL recommendedpeers_from_friends('cursor');
--     FETCH ALL IN "cursor";
--     CLOSE cursor;
-- END;

-- Task 9
-- Процедура принимает названия блоков (block1, block2)
-- Выдаёт процент пиров, начавших block1 (StartedBlock1), block2 (StartedBlock2), оба блока (StartedBothBlocks), ни один из блоков (DidntStartAnyBlock)

CREATE OR REPLACE PROCEDURE GetPeersOfTwoBlocks(
    INOUT cursor REFCURSOR,
    IN block1 TEXT,
    IN block2 TEXT
) AS
$$
DECLARE
    peersCount NUMERIC;
BEGIN
    SELECT COUNT(*) INTO peersCount FROM Peers;

    OPEN cursor FOR
    WITH
        block1_Checks AS (
            SELECT DISTINCT PeerNickname FROM Checks WHERE TaskTitle LIKE (block1 || '%')
        ),
        block2_Checks AS (
            SELECT DISTINCT PeerNickname FROM Checks WHERE TaskTitle LIKE (block2 || '%')
        ),
        bothBlocks_Checks AS (
            SELECT * FROM block1_Checks
            UNION
            SELECT * FROM block2_Checks
        ),
        dontMatchBlocks_Checks AS (
            SELECT Nickname FROM Peers
            EXCEPT
            SELECT * FROM bothBlocks_Checks
        )
    SELECT
    (
        SELECT ((COUNT(*)::NUMERIC / peersCount) * 100)::int FROM block1_Checks
    ) as "StartedBlock1",
    (
        SELECT ((COUNT(*)::NUMERIC / peersCount) * 100)::int FROM block2_Checks
    ) as "StartedBlock2",
    (
        SELECT ((COUNT(*)::NUMERIC / peersCount) * 100)::int FROM bothBlocks_Checks
    ) as "StartedBothBlocks",
    (
        SELECT ((COUNT(*)::NUMERIC / peersCount) * 100)::int FROM dontMatchBlocks_Checks
    ) as "DidntStartAnyBlock";
END;
$$ LANGUAGE plpgsql;

/*
BEGIN;
    CALL GetPeersOfTwoBlocks('cursor', 'C', 'A');
    FETCH ALL IN "cursor";
    CLOSE "cursor";
END;
*/

/*
INSERT INTO Tasks(Title, MaxXP) VALUES('A0_something', 1000);
INSERT INTO Checks(PeerNickname, TaskTitle, Date) VALUES('clairere', 'A0_something', '2020-01-01');
INSERT INTO XP(CheckID, XPAmount) VALUES(6, 0);
*/

-- Task 10
-- Процедура выдаёт процент пиров, выполнивших успешно проект в свой день рождения (SuccessfulChecks) и процент заваливших (UnsuccessfulChecks)

CREATE OR REPLACE PROCEDURE GetBirthdayChecksPercentage(
    INOUT cursor REFCURSOR
) AS
$$
DECLARE
    peersCount INT;
BEGIN
    SELECT COUNT(*) INTO peersCount FROM Peers;

    OPEN cursor FOR
    WITH
        birthday_Checks AS (
            SELECT Checks.ID as CheckID FROM Checks
            INNER JOIN Peers ON Peers.Nickname = Checks.PeerNickname
            WHERE date_part('day', Peers.Birthday) = date_part('day', Checks.Date)
              AND date_part('month', Peers.Birthday) = date_part('month', Checks.Date)
        ),
        success_Checks AS (
            SELECT DISTINCT BC.CheckID FROM birthday_Checks as BC
            INNER JOIN p2p on BC.CheckID = p2p.checkid AND p2p.state = 'Success'
            INNER JOIN verter on BC.CheckID = p2p.checkid AND verter.state = 'Success'
        ),
        failed_Checks AS (
            SELECT DISTINCT BC.CheckID FROM birthday_Checks as BC
            INNER JOIN p2p on BC.CheckID = p2p.checkid
            LEFT  JOIN verter on BC.CheckID = verter.checkid
            WHERE p2p.state = 'Failure' OR verter.state = 'Failure'
        )
    SELECT
    (
        SELECT (COUNT(*)::numeric / peersCount * 100)::int FROM success_Checks
    ) as "SuccessfulChecks",
    (
        SELECT (COUNT(*)::numeric / peersCount * 100)::int FROM failed_Checks
    ) as "UnsuccessfulChecks";
END;
$$ LANGUAGE plpgsql;

/*
BEGIN;
    CALL GetBirthdayChecksPercentage('cursor');
    FETCH ALL IN "cursor";
    CLOSE "cursor";
END;
*/

/*
INSERT INTO Checks(PeerNickname, TaskTitle, Date) VALUES('jenningc', 'A0_something', '2020-01-01');
INSERT INTO P2P(CheckID, CheckingPeerNickname, State    ) VALUES
               (11,       'jenningc',          'Start'  ),
               (11,       'jenningc',          'Success');
INSERT INTO Verter(CheckID, State    ) VALUES
               (11,     'Start'   ),
               (11,     'Success' );
*/

-- Task 11
-- Процедура принимает название трёх проектов (task1, task2, task3)
-- Выдаёт пиров, успешно прошедших task1 и task2, но заваливших task3

CREATE OR REPLACE PROCEDURE GetPeersByTasks_1_2_Success_3_Failed(
    INOUT cursor REFCURSOR,
    IN task1 TEXT,
    IN task2 TEXT,
    IN task3 TEXT
) AS
$$
BEGIN
    OPEN cursor FOR
    WITH
        completeTask1_Peers AS (
            SELECT DISTINCT Peers.Nickname FROM Peers
            INNER JOIN Checks ON Checks.PeerNickname = Peers.Nickname
            INNER JOIN p2p on checks.id = p2p.checkid AND p2p.state = 'Success'
            INNER JOIN verter on checks.id = verter.checkid AND verter.state = 'Success'
            WHERE Checks.TaskTitle = task1
        ),
        completeTask2_Peers AS (
            SELECT DISTINCT Peers.Nickname FROM Peers
            INNER JOIN Checks ON Checks.PeerNickname = Peers.Nickname
            INNER JOIN p2p on checks.id = p2p.checkid AND p2p.state = 'Success'
            INNER JOIN verter on checks.id = verter.checkid AND verter.state = 'Success'
            WHERE Checks.TaskTitle = task2
        ),
        failedTask3_Peers AS (
            SELECT DISTINCT Peers.Nickname FROM Peers
            INNER JOIN Checks ON Checks.PeerNickname = Peers.Nickname
            INNER JOIN p2p on checks.id = p2p.checkid
            LEFT  JOIN verter on checks.id = verter.checkid
            WHERE Checks.TaskTitle = task3 AND (p2p.state = 'Failure' OR verter.state = 'Failure')
        )
    SELECT * FROM completeTask1_Peers
    INTERSECT
    SELECT * FROM completeTask2_Peers
    INTERSECT
    SELECT * FROM failedTask3_Peers;
END;
$$ LANGUAGE plpgsql;

/*
BEGIN;
    CALL GetPeersByTasks_1_2_Success_3_Failed('cursor', 'C0_SimpleBashUtils', 'C0_SimpleBashUtils', 'C1_s21_string+');
    FETCH ALL IN "cursor";
    CLOSE "cursor";
END;
*/

/*
INSERT INTO XP(CheckID, XPAmount) VALUES(5, 0);
INSERT INTO Checks(PeerNickname, TaskTitle) VALUES('jenningc', 'C1_s21_string+');
INSERT INTO XP(CheckID, XPAmount) VALUES(2, 250);
INSERT INTO XP(CheckID, XPAmount) VALUES(8, 500);
*/

-- Task 12
-- Процедура выдаёт для каждого проекта кол-во его "родительских" проектов (ParentTask), т.е. сколько нужно сделать до него проектов

CREATE OR REPLACE PROCEDURE GetTasksParentsCount(INOUT cursor REFCURSOR) AS
$$
BEGIN
    OPEN cursor FOR
    WITH RECURSIVE parentTasks AS (
        SELECT Title, 0 as ParentsCount FROM Tasks

        UNION

        SELECT Tasks.Title, (PT.ParentsCount + 1) FROM Tasks
        INNER JOIN parentTasks AS PT ON PT.Title = Tasks.ParentTask
    )
    SELECT Title, MAX(ParentsCount) FROM parentTasks
    GROUP BY Title
    ORDER BY MAX(ParentsCount) DESC;
END;
$$ LANGUAGE plpgsql;

/*
BEGIN;
    CALL GetTasksParentsCount('cursor');
    FETCH ALL IN "cursor";
    CLOSE "cursor";
END;
*/

-- Task 13
-- Процедура принимает необходимое кол-во идущих подряд успешных проверок
-- Выдаёт дни, в которых такое или большее кол-во подряд успешных проверок произошло

CREATE OR REPLACE PROCEDURE GetLuckyDays(
    INOUT cursor REFCURSOR,
    IN minSuccessChecks INT
) AS
$$
BEGIN
    OPEN cursor FOR
    WITH success_Checks AS (
        SELECT Checks.Date, XP.XPAmount, Tasks.MaxXP FROM Checks
        JOIN P2P ON P2P.CheckID = Checks.ID
        JOIN Verter ON Verter.CheckID = Checks.ID
        JOIN Tasks ON Tasks.Title = Checks.TaskTitle
        JOIN XP ON XP.CheckID = Checks.ID
        WHERE P2P.State = 'Success' AND Verter.State = 'Success'
    )
    SELECT Date FROM success_Checks
    WHERE XPAmount >= 0.8 * MaxXP
    GROUP BY Date
    HAVING COUNT(Date) >= minSuccessChecks;
END;
$$ LANGUAGE plpgsql;

/*
BEGIN;
    CALL GetLuckyDays('cursor', 1);
    FETCH ALL IN "cursor";
    CLOSE "cursor";
END;
*/

-- Task 14
-- Выдаёт пира с максимальным кол-вом XP

CREATE OR REPLACE PROCEDURE GetPeerWithMaxXP(INOUT cursor REFCURSOR) AS
$$
BEGIN
    OPEN cursor FOR
    SELECT Checks.PeerNickname as Peer, SUM(XP.XPAmount)
    FROM Checks
    JOIN XP ON XP.CheckID = Checks.ID
    GROUP BY Peer
    ORDER BY SUM(XP.XPAmount) DESC
    LIMIT 1;
END;
$$ LANGUAGE plpgsql;

/*
BEGIN;
    CALL GetPeerWithMaxXP('cursor');
    FETCH ALL IN "cursor";
    CLOSE "cursor";
END;
*/

-- Task 15
-- Принимает время (fromTime), необходимое кол-во посещений (minDaysCount)
-- Выдаёт пиров, приходивших раньше заданного времени не менее minDaysCount раз за всё время

CREATE OR REPLACE PROCEDURE GetPeersCameBefore(
    INOUT cursor REFCURSOR,
    IN fromTime TIME,
    IN minDaysCount INT
) AS
$$
BEGIN
    OPEN cursor FOR
    SELECT PeerNickname FROM TimeTracking
    WHERE Time < fromTime AND State = 1
    GROUP BY PeerNickname
    HAVING COUNT(PeerNickname) >= minDaysCount;
END;
$$ LANGUAGE plpgsql;

/*
BEGIN;
    CALL GetPeersCameBefore('cursor', '12:00', 1);
    FETCH ALL IN "cursor";
    CLOSE "cursor";
END;
*/

-- Task 16
-- Принимает кол-во дней (daysCount) и кол-во посещений (timesCount)
-- Выдаёт пиров, выходивших из кампуса больше timesCount раз за последние daysCount дней

CREATE OR REPLACE PROCEDURE GetPeersExits(
    INOUT cursor REFCURSOR,
    IN daysCount INT,
    IN timesCount INT
) AS
$$
BEGIN
    OPEN cursor FOR
    SELECT PeerNickname FROM TimeTracking
    WHERE State = 2 AND Date > (NOW() - (daysCount || ' days')::INTERVAL)
    GROUP BY PeerNickname
    HAVING COUNT(PeerNickname) >= timesCount;
END;
$$ LANGUAGE plpgsql;

/*
BEGIN;
    CALL GetPeersExits('cursor', 1, 2);
    FETCH ALL IN "cursor";
    CLOSE "cursor";
END;
*/

/*
INSERT INTO TimeTracking(PeerNickname, State) VALUES
                        ('clairere',   1    ),
                        ('clairere',   2    ),
                        ('clairere',   1    ),
                        ('clairere',   2    ),
                        ('jenningc',   1    ),
                        ('jenningc',   2    ),
                        ('lynseypi',   1    ),
                        ('lynseypi',   2    );
*/

-- Task 17
-- Выдаёт отношение ранних (до 12ч) приходов в кампус к общему кол-ву приходов отдельно для каждого месяца

CREATE OR REPLACE PROCEDURE GetMonthEarlyEntries(INOUT cursor REFCURSOR) AS
$$
BEGIN
    OPEN cursor FOR
    WITH
        months AS (
            SELECT to_char(to_date(num::text, 'MM'), 'Month') as Month, num as Num
            FROM generate_series(1, 12) as num
        ),
        totalEntriesMonths AS (
            SELECT date_part('month', Date) as monthNum, COUNT(date_part('month', Date)) as count
            FROM TimeTracking
            WHERE State = 1
            GROUP BY date_part('month', Date)
        ),
        earlyEntriesMonths AS (
            SELECT date_part('month', Date) as monthNum, COUNT(date_part('month', Date)) as count
            FROM TimeTracking
            WHERE Time < '12:00' AND State = 1
            GROUP BY date_part('month', Date)
        )
    SELECT
        months.Month,
        (COALESCE(earlyEM.count::numeric / totalEM.count::numeric, 0) * 100)::int as EarlyEntries
    FROM months
    LEFT JOIN totalEntriesMonths AS totalEM ON totalEM.monthNum = months.Num
    LEFT JOIN earlyEntriesMonths AS earlyEM ON earlyEM.monthNum = months.Num
    ORDER BY months.Num;
END;
$$ LANGUAGE plpgsql;

/*
BEGIN;
    CALL GetMonthEarlyEntries('cursor');
    FETCH ALL IN "cursor";
    CLOSE "cursor";
END;
*/
