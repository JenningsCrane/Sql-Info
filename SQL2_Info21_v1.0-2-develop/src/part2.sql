DROP PROCEDURE IF EXISTS add_p2p_check CASCADE;
CREATE OR REPLACE PROCEDURE add_p2p_check(
    checked_peer TEXT,
    checking_peer TEXT,
    task_name TEXT,
    p2p_check CheckStatus,
    checking_time TIME
) AS $add_p2p_check$
DECLARE
    check_id INT;
    last_status VARCHAR;
BEGIN
    SELECT state INTO last_status
    FROM p2p
    WHERE CheckingPeerNickname = checking_peer
    ORDER BY id DESC
    LIMIT 1;

    IF p2p_check = 'Start' THEN
        IF last_status = 'Start' THEN
            RAISE EXCEPTION 'У % есть незавершенная проверка', checking_peer;
        END IF;

        INSERT INTO Checks (id, PeerNickname, TaskTitle, Date)
        VALUES ((SELECT COALESCE(MAX(id), 0) + 1 FROM Checks), checked_peer, task_name, current_date)
        RETURNING id INTO check_id;

    ELSE
        IF last_status <> 'Start' THEN
            RAISE EXCEPTION 'Нельзя завершить проверку, которая не началась';
        END IF;

        SELECT CheckID INTO check_id
        FROM P2P
        WHERE P2P.CheckingPeerNickname = checking_peer AND state = 'Start'
        ORDER BY 1 DESC
        LIMIT 1;

        IF check_id IS NOT NULL AND check_id <= (SELECT MAX(CheckID) FROM p2p WHERE state <> 'Start') THEN
            RAISE EXCEPTION 'Запись в p2p с таким check_id уже есть';
        END IF;
    END IF;

    INSERT INTO P2P(id, CheckID, CheckingPeerNickname, State, Time)
    VALUES ((SELECT COALESCE(MAX(id), 0) + 1 FROM P2P), check_id, checking_peer, p2p_check, checking_time);
END;
$add_p2p_check$ LANGUAGE plpgsql;

--  -- Добавление проверки пиром lynseypi пира jenningc.
--  CALL add_p2p_check('jenningc', 'lynseypi' , 'C1_s21_string+', 'Start', '14:00:00');

--  -- Добавление проверки со статусом 'Start' невозможно, т.к. проверка пира jenningc еще не закончена.
--  CALL add_p2p_check('jenningc', 'lynseypi' , 'C0_SimpleBashUtils', 'Start', '14:00:00');
--
--  -- Завершение проверки пира jenningc пиром lynseypi со статусом 'Success'.
--  CALL add_p2p_check('jenningc', 'lynseypi' , 'C0_SimpleBashUtils', 'Success', '14:30:00');

--  SELECT * FROM Checks;
--  SELECT * FROM P2P;

--  delete from p2p where checkid = 6;
--  delete from checks where id = 6;

-- $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$

DROP PROCEDURE IF EXISTS add_verter_check CASCADE;
CREATE OR REPLACE PROCEDURE add_verter_check(
    checked_peer TEXT,
    task_name TEXT,
    verter_check CheckStatus,
    checking_time TIME
) AS $add_verter_check$
DECLARE
    check_id INT;
    last_status TEXT;
BEGIN
    IF verter_check = 'Start' THEN
        SELECT ch.id INTO check_id
        FROM p2p
        JOIN checks ch ON p2p.checkid = ch.id
        WHERE p2p.state = 'Success'
        AND ch.peernickname = checked_peer AND ch.tasktitle = task_name
        ORDER BY ch.date DESC
        LIMIT 1;

        IF check_id IS NULL THEN
            RAISE EXCEPTION 'Verter не может осуществить проверку пира, который не сдал проект другому пиру';
        END IF;

        IF check_id IS NOT NULL AND check_id <= (SELECT MAX(CheckID) FROM verter) THEN
            RAISE EXCEPTION 'Проверка verter с id % уже идет', check_id;
        END IF;
    ELSE
        SELECT v.state INTO last_status
        FROM verter v
        ORDER BY v.id DESC
        LIMIT 1;

        IF last_status IS NULL OR last_status <> 'Start' THEN
            RAISE EXCEPTION 'Добавление проверки verter невозможно, т.к. она не была начата.';
        END IF;

        SELECT v.checkid INTO check_id
        FROM verter v
        ORDER BY v.id DESC
        LIMIT 1;
    END IF;

    INSERT INTO verter(id, checkid, state, time)
    VALUES ((SELECT COALESCE(MAX(id), 0) + 1 FROM verter), check_id, verter_check, checking_time);
END;
$add_verter_check$ LANGUAGE plpgsql;

-- -- Добавляем начало проверки вертером пира jenningc.
-- CALL add_verter_check('jenningc', 'C1_s21_string+', 'Start', '14:35:00');
--
-- -- Снова пытаемся добавить начало проверки вертером пира jenningc.
-- CALL add_verter_check('jenningc', 'C1_s21_string+', 'Start', '14:35:00');
--
-- -- Завершаем проверку вертером пира jenningc.
-- CALL add_verter_check('jenningc', 'C1_s21_string+', 'Success', '14:40:00');
--
-- SELECT * FROM verter;
--
-- delete from verter where checkid = 6;


-- $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$

DROP FUNCTION IF EXISTS fnc_trg_p2p_update_transferredpoints CASCADE;

CREATE OR REPLACE FUNCTION fnc_trg_p2p_update_transferredpoints()
RETURNS TRIGGER AS $fnc_trg_p2p_update_transferredpoints$
BEGIN
    UPDATE TransferredPoints
    SET PointsAmount = PointsAmount + 1
    WHERE checkingpeernickname = NEW.checkingpeernickname
    AND checkedpeernickname = (
        SELECT peernickname
        FROM checks
        WHERE id = NEW.checkid
    );
    RETURN NEW;
END;
$fnc_trg_p2p_update_transferredpoints$ LANGUAGE plpgsql;

CREATE TRIGGER trg_p2p_into_transferredpoints
AFTER INSERT
ON p2p
FOR EACH ROW
WHEN ( NEW.state = 'Start' )
EXECUTE FUNCTION fnc_trg_p2p_update_transferredpoints();

-- SELECT * FROM transferredpoints;
-- CALL add_p2p_check('jenningc', 'clairere' , 'C1_s21_string+', 'Start', '14:00:00');
--

-- $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$

DROP FUNCTION IF EXISTS trigger_xp_check CASCADE;

CREATE OR REPLACE FUNCTION trigger_xp_check()
RETURNS TRIGGER AS $trigger_xp_check$
DECLARE
    max_xp INT;
BEGIN
    SELECT tasks.MaxXP INTO max_xp
    FROM Checks
    JOIN Tasks ON Checks.tasktitle = tasks.title
    JOIN P2P ON checks.id = P2P.checkid
    JOIN Verter ON checks.id = Verter.checkid
    WHERE P2P.State = 'Success' AND Verter.State = 'Success' AND NEW.checkid = Checks.id;

    IF max_xp IS NULL THEN
        RAISE EXCEPTION 'Проверка незавершена или провалена';
    END IF;

    IF NEW.XPAmount > max_xp OR NEW.XPAmount <= 0 THEN
        RAISE EXCEPTION 'Необходимо корректное количество XP';
    END IF;

    RETURN NEW;
END;
$trigger_xp_check$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_xp_check
BEFORE INSERT ON XP
FOR EACH ROW
EXECUTE FUNCTION trigger_xp_check();

-- INSERT INTO XP(checkid, XPAmount)
-- VALUES (1, 250);
-- SELECT * FROM XP;
-- delete from xp where id >= 3;