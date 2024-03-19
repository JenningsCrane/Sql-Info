-- ########################################
-- ######         START DATE         ######
-- ########################################
-- task:
--   создать отдельную базу данных, в которой создать таблицы, функции,
--   процедуры и триггеры, необходимые для тестирования процедур.

DROP DATABASE part_four;
CREATE DATABASE part_four;
CREATE TABLE table_name_users
(
    id       SERIAL PRIMARY KEY,
    username VARCHAR(255),
    age      SMALLINT
);


CREATE TABLE users_audit
(
    time        DATE,
    mode        CHAR(1),
    id_username INTEGER,
    username    VARCHAR(255),
    age         SMALLINT
);

CREATE OR REPLACE FUNCTION get_user_count()
  RETURNS INTEGER AS $$
    BEGIN
          RETURN (SELECT COUNT(*)
                    FROM table_name_users);
    END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION get_last_user_id()
RETURNS INTEGER AS $$
    DECLARE
        last_id INTEGER;
    BEGIN
         SELECT MAX(id) INTO last_id
           FROM table_name_users;
         RETURN last_id;
    END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION get_user_id(uname VARCHAR)
RETURNS INTEGER AS $$
    DECLARE
        user_id INTEGER;
    BEGIN
         SELECT id INTO user_id
           FROM table_name_users
          WHERE username = uname;
         RETURN user_id;
    END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION get_user_id(uname VARCHAR) IS 'get_user_id';

CREATE OR REPLACE FUNCTION GetEmployeesByDepartment(department_id INT)
RETURNS TABLE (
    employee_id   INT,
    employee_name TEXT
) AS $$
BEGIN
    RETURN QUERY
    SELECT id,
           username
      FROM table_name_users;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION GetEmployeesByDepartment(department_id INT) IS 'GetEmployeesByDepartment';

CREATE FUNCTION fnc_trg_person_audit()
    RETURNS TRIGGER AS $$
BEGIN
    IF (TG_OP = 'INSERT') THEN
       INSERT INTO users_audit SELECT now(),'I', NEW.id, NEW.username, NEW.age;
    ELSIF (TG_OP = 'UPDATE') THEN
       INSERT INTO users_audit SELECT now(),'U', OLD.id, OLD.username, OLD.age;
    ELSIF (TG_OP = 'DELETE') THEN
       INSERT INTO users_audit SELECT now(),'D', OLD.id, OLD.username, OLD.age;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_person_audit
AFTER INSERT OR UPDATE OR DELETE ON table_name_users
    FOR EACH ROW EXECUTE FUNCTION fnc_trg_person_audit();

CREATE OR REPLACE PROCEDURE insert_user(username VARCHAR, age SMALLINT)
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO table_name_users (username, age) VALUES (username, age);
END;
$$;

COMMENT ON PROCEDURE insert_user(username VARCHAR, age SMALLINT) IS 'not get this inset';


CALL insert_user('IlonMask'::VARCHAR, 43::SMALLINT);
CALL insert_user('MamiLove'::VARCHAR, 111::SMALLINT);
CALL insert_user('Cat'::VARCHAR, 1::SMALLINT);


-- ########################################
-- ######         PART 4 - 1         ######
-- ########################################
-- task:
--   1) Создать хранимую процедуру, которая, не уничтожая базу данных, уничтожает все те таблицы текущей базы данных
--   , имена которых начинаются с фразы 'TableName'.


CREATE OR REPLACE PROCEDURE DropTablesStartingWithTableName(table_name VARCHAR)
AS $$
    DECLARE
        tables_name TEXT;
         drop_table TEXT;
    BEGIN
        FOR tables_name IN (
            SELECT tablename
              FROM pg_tables
             WHERE schemaname = current_schema() -- Соответствует ли запрос текущей схеме (public)
               AND tablename LIKE concat(table_name, '%') -- ILIKE - без учета регистра
        ) LOOP
            drop_table := 'DROP TABLE IF EXISTS ' || tables_name || ' CASCADE;';
            EXECUTE drop_table;
        END LOOP;
    END;
$$ LANGUAGE plpgsql;

-- CALL DropTablesStartingWithTableName('us');
-- CALL DropTablesStartingWithTableName('ta');



-- ########################################
-- ######         PART 4 - 2         ######
-- ########################################
-- task:
--   2) Создать хранимую процедуру с выходным параметром, которая выводит список имен
--   и параметров всех скалярных SQL функций пользователя в текущей базе данных.
--   Имена функций без параметров не выводить. Имена и список параметров должны выводиться в одну строку.
--   Выходной параметр возвращает количество найденных функций.

CREATE OR REPLACE PROCEDURE GetScalarFunctions(OUT function_count INT)
AS $$
    DECLARE
         functions RECORD;
        schema_oid OID;
    BEGIN
            SELECT oid INTO schema_oid
              FROM pg_namespace
             WHERE nspname = current_schema();

        function_count := 0;

        RAISE NOTICE 'Fucntion with args:';

        FOR functions IN (
            SELECT proname::TEXT AS function_name,
                   proargnames::TEXT AS args,
                   pronargs AS input_argument_count, -- содержит количество аргументов на входе функции.
                   proretset AS is_set_return_type -- false, если функция возвращает скалярное значение.
              FROM pg_proc
             WHERE pronamespace = schema_oid
                   AND prokind = 'f' -- Функции скалярного типа
        ) LOOP
            IF functions.args IS NOT NULL THEN
                IF functions.input_argument_count = 1  AND NOT functions.is_set_return_type THEN
                    function_count := function_count + 1;
                    RAISE NOTICE '%: %', function_count ,functions.function_name || functions.args; -- вывод в лог
                END IF;
            END IF;
        END LOOP;
        IF function_count = 0 THEN
            RAISE NOTICE 'No functions found';
        END IF;
    END;
$$ LANGUAGE plpgsql;

DO $$
    DECLARE
        function_count INT;
    BEGIN
        CALL GetScalarFunctions(function_count);
       RAISE NOTICE 'Quantity functions: %', function_count;
    END;
$$ LANGUAGE plpgsql;


-- ########################################
-- ######         PART 4 - 3         ######
-- ########################################
-- task:
--   3) Создать хранимую процедуру с выходным параметром, которая уничтожает все SQL DML триггеры в текущей базе данных.
--   Выходной параметр возвращает количество уничтоженных триггеров

CREATE OR REPLACE PROCEDURE DropAllDMLTriggers(OUT trigger_count INT)
AS $$
DECLARE
    name_trigger TEXT;
    name_table   TEXT;
BEGIN
    trigger_count := 0;

    FOR name_trigger, name_table IN (
        SELECT tgname::TEXT,
               tables.relname::TEXT
        FROM   pg_trigger
        JOIN   (SELECT oid, relname
                FROM   pg_class) AS tables
           ON  pg_trigger.tgrelid = tables.oid
        WHERE  tgname NOT LIKE 'pg_%' -- Исключаем системные триггеры
          AND  tgisinternal = FALSE -- Исключаем триггеры, связанные с ограничениями
    ) LOOP
        EXECUTE 'DROP TRIGGER IF EXISTS ' || quote_ident(name_trigger) || ' ON '|| quote_ident(name_table) || ' CASCADE;';
        trigger_count := trigger_count + 1;
    END LOOP;
END $$ LANGUAGE plpgsql;


-- DO $$
--     DECLARE
--         trigger_count INT;
--     BEGIN
--         CALL DropAllDMLTriggers(trigger_count);
--         RAISE NOTICE 'Deleted triggers: %', trigger_count;
--     END;
-- $$ LANGUAGE plpgsql;
--
-- SELECT tgname
-- FROM pg_trigger;



-- ########################################
-- ######         PART 4 - 4         ######
-- ########################################
-- task:
--   4) Создать хранимую процедуру с входным параметром, которая выводит имена и описания типа объектов
--   (только хранимых процедур и скалярных функций), в тексте которых на языке SQL встречается строка,
--   задаваемая параметром процедуры.

CREATE OR REPLACE PROCEDURE SearchObjectsByStr(string TEXT)
AS $$
    DECLARE
           objects RECORD;
        schema_oid OID;
    BEGIN
            SELECT oid INTO schema_oid
              FROM pg_namespace
             WHERE nspname = current_schema();

        FOR objects IN (
            SELECT proname::TEXT AS name,
                   prosrc::TEXT AS description,
                   t.description AS type

            FROM pg_proc p
            LEFT JOIN pg_description t
            ON p.prorettype = t.objoid
             WHERE ((prokind = 'f' AND pronargs = 1 AND NOT proretset) OR prokind = 'p')
               AND proname NOT LIKE 'pg_%'
               AND pronamespace = schema_oid
               AND ((prosrc LIKE '%' || string || '%') OR (proname LIKE '%' || string || '%' ))
        ) LOOP
            RAISE NOTICE 'Имя объекта: %, Описание типа: %', objects.name, objects.type;
        END LOOP;
    END;
$$ LANGUAGE plpgsql;


CALL SearchObjectsByStr('get');


