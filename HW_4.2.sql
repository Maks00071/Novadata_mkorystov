-- создаем таблицы public.users и public.users_audit

DROP TABLE IF EXISTS public.users;

CREATE TABLE IF NOT EXISTS public.users(
	  id SERIAL primary KEY
	, "name" TEXT
	, email TEXT
	, "role" TEXT
	, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-----
DROP TABLE IF EXISTS public.users_audit;

CREATE TABLE IF NOT EXISTS public.users_audit(
	  id SERIAL PRIMARY KEY
	, user_id INT4
	, changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	, changed_by TEXT
	, field_changed TEXT
	, old_value TEXT
	, new_value TEXT
);

-----
-- создаем функцию логирования

-- IS DISTINCT FROM (аналог <>, но позволяет корректно обрабатывать NULL-значения), 
-- но на больших объемах не эффективно, т.к. не использует индексы!
DROP FUNCTION IF EXISTS ublic.log_user_update CASCADE;

CREATE OR REPLACE FUNCTION public.log_user_update()
RETURNS TRIGGER 
AS $func$ 
	BEGIN 
		-- если изменилось имя (учитываем, что могут быть NULL-значения)
		IF (OLD."name" IS DISTINCT FROM NEW."name") THEN 
			INSERT INTO public.users_audit(user_id, changed_by, field_changed, old_value, new_value)
			VALUES (OLD.id, CURRENT_USER, 'name', OLD."name", NEW."name");
		END IF;

		-- если изменилась почта
		if (OLD.email IS DISTINCT FROM NEW.email) THEN 
			INSERT INTO public.users_audit(user_id, changed_by, field_changed, old_value, new_value)
			VALUES (OLD.id, CURRENT_USER, 'email', OLD.email, NEW.email);
		END IF;

		-- если изменилась роль
		IF (OLD."role" IS DISTINCT FROM NEW."role") THEN
			INSERT INTO public.users_audit(user_id, changed_by, field_changed, old_value, new_value)
			VALUES (OLD.id, CURRENT_USER, 'role', OLD."role", NEW."role");
		END IF;

		-- обновим updated_at у таблицы public.users
		NEW.updated_at = CURRENT_TIMESTAMP;

		RETURN NEW;
		
	END;
$func$ LANGUAGE plpgsql;

-----
-- создаем триггер

CREATE TRIGGER trigger_log_user_changes
BEFORE UPDATE ON public.users
fOR EACH ROW 
EXECUTE FUNCTION public.log_user_update();

-----
-- устанавливаем расширение pg_cron

CREATE EXTENSION IF NOT EXISTS pg_cron; 

-----
-- создаем функцию выгрузки свежих данных и записывать из в образ Docker
DROP FUNCTION IF EXISTS public.export_todays_data CASCADE;

CREATE OR REPLACE FUNCTION public.export_todays_data()
RETURNS VOID 
--SECURITY DEFINER -- лучше установить "выполнение с правами владельца"
AS $func$
	DECLARE
		export_path TEXT;
		export_query TEXT;
	BEGIN

		-- путь к файлу с текущей датой
		export_path := '/tmp/users_audit_export_' || TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') || '.csv';

		-- запрос на извлечение данных за текущую дату
		export_query := $query$ 
			COPY (
				SELECT * 
				FROM public.users_audit
				WHERE changed_at::DATE = CURRENT_DATE
				) TO PROGRAM 'cat > $command$ || QUOTE_LITERAL(export_path) $command$'
			WITH CSV HEADER
		$query$;

		-- выполним экспорт данных
		EXECUTE export_query;

		-- сделаем пометку для пользователя
		RAISE NOTICE 'Данные загружены в файл %', export_path; 

	END;

$func$ LANGUAGE plpgsql;

------
-- установим планировщик pg_cron на 3:00 ночи ежедневно

SELECT cron.schedule(
	'export_daily_data',					-- имя задания
	'0 3 * * *', 							-- время выполнения
	'SELECT public.export_todays_data();'	-- выполняемая команда
);

-----
-- проверим, что задание установилось
select * from cron.job;

-----
















\