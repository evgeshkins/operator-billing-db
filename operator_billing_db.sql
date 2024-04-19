-- PostgreSQL 15

-- CREATE DATABASE operator_billing_db; -- Создаём базу данных

CREATE TABLE subscriber -- сущность "Абонент"
(
	sub_id SERIAL PRIMARY KEY,
	last_name CHARACTER VARYING(30) NOT NULL,
	first_name CHARACTER VARYING(30) NOT NULL,
	middle_name CHARACTER VARYING(30)
);
CREATE TABLE tariff -- сущность "Тариф"
(
	tariff_id SERIAL PRIMARY KEY,
	price REAL NOT NULL, -- стоимость тарифа
	minute_quantity REAL, -- кол-во минут в тарифе
	add_price_per_min REAL -- стоимость поминутной оплаты звонков сверх пакета
);
CREATE TABLE subscriber_tariff -- промежуточная сущность "Абонент-тариф"
(
	st_id SERIAL PRIMARY KEY,
	phone_number CHARACTER VARYING NOT NULL UNIQUE, -- номер телефона абонента
	balance REAL,
	minute_remains REAL, --Остаток
	sub_id INTEGER,
	tariff_id INTEGER,
	FOREIGN KEY (sub_id) REFERENCES subscriber (sub_id), -- внешний ключ на таблицу "Абонент"
	FOREIGN KEY (tariff_id) REFERENCES tariff (tariff_id) -- внешний ключ на таблицу "Тариф"
);
CREATE TABLE calls -- Сущность "Звонки"
(
	call_id SERIAL PRIMARY KEY,
	call_duration TIME, -- продолжительность звонка
	call_fee REAL, -- плата за совершенный звонок (0 - если есть пакет минут, либо расчёт в руб/мин по тарифу, если пакет минут закончился)
	st_id INTEGER,
	FOREIGN KEY (st_id) REFERENCES subscriber_tariff (st_id) -- внешний ключ на таблицу "Абонент-тариф"
);
CREATE TABLE transactions -- сущность "Операции"
(
	transaction_id SERIAL PRIMARY KEY,
	amount REAL, -- сумма, используемая в операции: отрицательная - списание, положительная - зачисление
	phone_number CHARACTER VARYING, -- номер по которому была проведена операция
	transaction_date TIMESTAMP, -- дата и время выполнения операции
	user_name CHARACTER VARYING, -- пользователь, совершивший операцию
	st_id INTEGER,
	FOREIGN KEY (st_id) REFERENCES subscriber_tariff (st_id)
);

-- АРМ Менеджера: работа со справочником тарифов - создание справочника
CREATE OR REPLACE VIEW tariffs_catalog AS
	SELECT * FROM tariff ORDER BY tariff_id;

-- Создадим представления таблиц subscriber и subscriber_tariff, чтобы работать с таблицами не напрямую, а через представления.
-- Заодно делаем сортировку по возрастанию значений поля sub_id и st_id для более красивого вывода после изменений данных.
CREATE OR REPLACE VIEW subscriber_view AS
	SELECT * FROM subscriber ORDER BY sub_id;
	
CREATE OR REPLACE VIEW st_view AS
	SELECT * FROM subscriber_tariff ORDER BY st_id;

-- Телефонная станция: списание за совершенный звонок
-- Сделаем представление списаний за все совершенные звонки (select таблицы всех совершенных звонков
-- с информацией об оплате за каждый звонок)
-- Заодно делаем сортировку по возрастанию значений поля call_id для более красивого вывода после изменений данных.
CREATE OR REPLACE VIEW all_call_fees AS
	SELECT * FROM calls ORDER BY call_id;
-- Это представление можно использовать для того, чтобы выводить списание за определенный звонок (используем SELECT)

-- Мобильное приложение: просмотр истории списаний/зачислений денежных средств
-- Делаем представление таблицы transactions
-- Заодно делаем сортировку по возрастанию значений поля transaction_id для более красивого вывода после изменений данных.
CREATE OR REPLACE VIEW transactions_history AS
	SELECT * FROM transactions ORDER BY transaction_id;

-- Функция для триггера обновлений баланса абонента
CREATE OR REPLACE FUNCTION st_update_trigger_fnc()
RETURNS TRIGGER AS
$$
BEGIN
INSERT INTO transactions(amount, phone_number, transaction_date, user_name, st_id)
VALUES (round(cast(NEW."balance"-OLD."balance" as NUMERIC), 2), NEW."phone_number", now()::timestamp(0), current_user, NEW."st_id");
RETURN NEW;
END;
$$
LANGUAGE plpgsql;

-- Триггер обновлений баланса абонента
CREATE OR REPLACE TRIGGER update_balance_trigger
AFTER UPDATE OF balance ON subscriber_tariff
FOR EACH ROW
EXECUTE FUNCTION st_update_trigger_fnc();

--- Функция для триггера: Триггер на INSERT в таблице "Звонки": уменьшается баланс при звонках сверх пакета,
--- идёт запись в таблицу transactions при изменении баланса, после каждого звонка уменьшается кол-во минут.
CREATE OR REPLACE FUNCTION call_insert_trigger_fnc()
RETURNS TRIGGER AS
$$
BEGIN
		-- если остаток минут ПРИ ЗАВЕРШЕНИИ звонка >0 - плата за звонок 0 мин
		IF ((SELECT minute_remains FROM st_view WHERE st_id = NEW."st_id") - 
		(EXTRACT(EPOCH FROM NEW."call_duration")/60)) > 0 THEN 
			UPDATE all_call_fees SET call_fee = 0 WHERE call_id = NEW."call_id";
		ELSIF (SELECT minute_remains FROM st_view WHERE st_id = NEW."st_id") = 0 THEN
		-- если остаток мин = 0 - плата за звонок рассчитывается руб/мин
			UPDATE all_call_fees SET call_fee = 
				round(cast(((EXTRACT(EPOCH FROM NEW."call_duration")/60)*
				(SELECT add_price_per_min FROM tariffs_catalog WHERE tariff_id = 
					(SELECT tariff_id FROM st_view WHERE st_id = NEW."st_id"))) as NUMERIC), 2) WHERE call_id = NEW."call_id";
		ELSE
			IF (SELECT minute_remains FROM st_view WHERE st_id = NEW."st_id")<
			(EXTRACT(EPOCH FROM NEW."call_duration")/60) THEN
			-- если длительность звонка больше, чем оставшийся пакет минут
				UPDATE all_call_fees SET call_fee = round(cast(((EXTRACT(EPOCH FROM NEW."call_duration")/60) -
				(SELECT minute_remains FROM st_view WHERE st_id = NEW."st_id"))*
				(SELECT add_price_per_min FROM tariffs_catalog WHERE tariff_id =
					(SELECT tariff_id FROM st_view WHERE st_id = NEW."st_id")) as NUMERIC), 2) WHERE call_id = NEW."call_id";
			END IF;
		END IF;
			-- Сразу же меняем баланс: сработает триггер изменения баланса и в таблицу с транзакциями будет сделана запись:			
		IF ((SELECT minute_remains FROM st_view WHERE st_id=NEW."st_id")-
		   (EXTRACT(EPOCH FROM NEW."call_duration")/60))<=0 THEN
			UPDATE st_view SET balance = 
				round(cast(balance - (SELECT call_fee FROM all_call_fees WHERE call_id = NEW."call_id") as NUMERIC), 2)
				WHERE st_id = NEW."st_id";
		END IF;
		-- обновляем остаток минут после звонка
		-- если остаток при вычитании уходит в отрицательное значение, делаем его равным нулю.
		IF ((SELECT minute_remains FROM st_view WHERE st_id=NEW."st_id")-
		   (EXTRACT(EPOCH FROM NEW."call_duration")/60)<=0) THEN
		   	UPDATE subscriber_tariff SET minute_remains = 0 WHERE st_id = NEW."st_id";
		-- иначе обновляем остаток
		ELSE
			UPDATE st_view SET minute_remains = 
				round(cast(minute_remains - (EXTRACT(EPOCH FROM NEW."call_duration")/60) as NUMERIC), 2)
			WHERE st_id = NEW."st_id";
		END IF;
	RETURN NEW;
END;
$$
LANGUAGE plpgsql;

--- Триггер на INSERT в таблице "Звонки": после каждого звонка уменьшается кол-во минут,
--- уменьшается баланс при звонках сверх пакета, идёт запись в таблицу transactions при изменении баланса
CREATE OR REPLACE TRIGGER call_insert_trigger
AFTER INSERT ON calls
FOR EACH ROW
EXECUTE FUNCTION call_insert_trigger_fnc();
 
--- Наполнение таблиц
INSERT INTO subscriber (last_name, first_name, middle_name)
VALUES
('Еремочкин', 'Олег', 'Вячеславович'),
('Абайдуллин', 'Руслан', 'Архипович'),
('Копырин', 'Евгений', 'Александрович'),
('Кошкина', 'Юлия', 'Сергеевна'),
('Цурочкина', 'Ольга', 'Николаевна'),
('Лебединских', 'Ксения', 'Андреевна'),
('Еремочкин', 'Владислав', 'Вячеславович'),
('Кроповских', 'Мария', 'Петровна'),
('Чубайс', 'Ольга', 'Дмитриевна'),
('Лебединский', 'Александр', 'Сергеевич');
INSERT INTO tariff (price, minute_quantity, add_price_per_min)
VALUES
(200, 250, 0.7),
(250, 300, 0.4),
(300, 350, 0.35),
(350, 400, 0.3),
(400, 500, 0.15),
(450, 600, 0.15),
(500, 700, 0.1),
(600, 800, 0.1),
(650, 900, 0.05),
(700, 1000, 0.05);
-- Наполнение таблицы абонент-тариф - один из абонентов (sub_id: 1) имеет два номера,
-- тариф с id: 4 не подключен ни одному из абонентов, а тариф c id: 3 подключен сразу двум абонентам
INSERT INTO subscriber_tariff (phone_number, balance, minute_remains, sub_id, tariff_id)
-- (номер тел, баланс, остаток минут, id абонента, id тарифа)
VALUES
('+7-909-171-11-11', 350, 300, 1, 3),
('+7-909-171-11-21', 200, 382, 2, 5),
('+7-909-171-11-31', 500, 735, 3, 8),
('+7-909-171-11-41', 100, 120, 4, 2),
('+7-909-171-11-51', 22, 200, 5, 1),
('+7-909-171-11-61', 1000, 972, 6, 10),
('+7-909-171-11-71', 282.5, 348, 7, 3),
('+7-909-171-11-81', 102, 172, 8, 6),
('+7-909-171-11-91', 82.32, 222, 9, 7),
('+7-909-171-11-01', 0, 125, 10, 9),
('+7-909-171-11-12', 3.55, 212, 1, 1);
INSERT INTO calls (call_duration, st_id)
-- (продолжительность звонка, плата за звонок (заполняется сама), id договора)
VALUES
('00:01:49', 1),
('00:25:48', 2),
('00:03:56', 3),
('01:00:45', 4),
('00:02:02', 5),
('00:01:47', 6),
('00:00:05', 7),
('02:01:01', 8),
('01:01:49', 9),
('00:31:52', 10),
('00:11:43', 11);
-- Таблица transactions (операции списания/зачисления) будет заполняться с помощью процедур по пополнению баланса,
-- списания за звонок, перевода денег другому абоненту.




-- АРМ Менеджера: работа со справочником тарифов - добавление нового тарифа в справочник
CREATE OR REPLACE PROCEDURE insert_tariff(new_price REAL, minute_quan REAL, price_per_min REAL)
LANGUAGE SQL
AS
$$
--- Делаем insert не напрямую в таблицу, а в представление таблицы
INSERT INTO tariffs_catalog (price, minute_quantity, add_price_per_min) VALUES (new_price, minute_quan, price_per_min);
$$;

-- АРМ Менеджера: работа со справочником тарифов: удаление тарифа по id
CREATE OR REPLACE PROCEDURE delete_tariff(t_id INTEGER)
LANGUAGE SQL
AS
$$
--- Делаем delete не напрямую в таблицу, а в представление таблицы
DELETE FROM tariffs_catalog WHERE tariff_id = t_id; 
$$;

-- АРМ Менеджера: работа со справочником тарифов: изменение тарифа по id
CREATE OR REPLACE PROCEDURE update_tariff(t_id INTEGER, t_price REAL, minute_quan REAL, price_per_min REAL)
LANGUAGE SQL
AS
$$
--- Делаем update не напрямую в таблицу, а в представление таблицы
	UPDATE tariffs_catalog SET price = t_price, minute_quantity = minute_quan, add_price_per_min = price_per_min
	WHERE tariff_id = t_id;
$$;


-- АРМ Менеджера: создание абонента
CREATE OR REPLACE PROCEDURE subscriber_create(l_name CHARACTER VARYING, f_name CHARACTER VARYING, m_name CHARACTER VARYING,
p_number CHARACTER VARYING, t_id INTEGER)
-- (Фамилия, Имя, Отчество, номер телефона, id тарифа)
LANGUAGE SQL
AS
$$
	INSERT INTO subscriber_view (last_name, first_name, middle_name) VALUES (l_name, f_name, m_name);
	-- В договор: номер телефона, баланс (изначально 0 при создании абонента), остаток минут по тарифу, id абонента, id нужного тарифа
	INSERT INTO st_view (phone_number, balance, minute_remains, sub_id, tariff_id)
		VALUES (p_number, 0, (SELECT minute_quantity FROM tariffs_catalog WHERE tariff_id = t_id),
		(SELECT MAX (sub_id) FROM subscriber_view), t_id);
$$;

-- АРМ Менеджера: Процедура: Пополнение баланса абонента
CREATE OR REPLACE PROCEDURE balance_replenishment(amount REAL, ph_number CHARACTER VARYING)
LANGUAGE SQL
AS
$$
UPDATE st_view SET balance = balance + amount WHERE phone_number = ph_number;
$$;

-- Телефонная станция: запрос доступного кол-ва минут
CREATE OR REPLACE FUNCTION select_minute_quantity (p_number CHARACTER VARYING) -- запрос по номеру телефона
RETURNS REAL
AS
$$
	BEGIN
		RETURN (SELECT minute_remains FROM st_view WHERE phone_number = p_number);
	END;
$$
LANGUAGE plpgsql;

-- (Процедура звонка для теста) Сделаем процедуру звонка для удобства проверки списания за каждый звонок
-- Параметры: длительность звонка, id договора
CREATE OR REPLACE PROCEDURE new_call (duration TIME, subt_id INTEGER)
LANGUAGE SQL
AS
$$
	INSERT INTO all_call_fees (call_duration, st_id) VALUES (duration, subt_id);
$$;



-- Мобильное приложение: перевод денег другому абоненту
-- Параметры: номер, с которого переводим; сумма перевода; номер, на который переводим
CREATE OR REPLACE PROCEDURE money_transfer (from_number CHARACTER VARYING, trans_amount REAL, to_number CHARACTER VARYING)
LANGUAGE plpgsql
AS
$$
BEGIN
  IF ((SELECT balance FROM st_view WHERE phone_number = from_number) - trans_amount) >= 0 THEN
    UPDATE st_view SET balance = round(cast(balance - trans_amount as NUMERIC), 2) WHERE phone_number = from_number;
    UPDATE st_view SET balance = round(cast(balance + trans_amount as NUMERIC), 2) WHERE phone_number = to_number;
  END IF;
END;
$$;

--- Делаем роли для каждого клиента БД
--- АРМ Менеджера: создаем роль, устанавливаем на нее простейший пароль
CREATE ROLE manager WITH
LOGIN
CONNECTION LIMIT -1
PASSWORD 'qwerty123456';
GRANT pg_signal_backend TO manager WITH ADMIN OPTION;
--- Телефонная станция: создаем роль, устанавливаем на нее простейший пароль
CREATE ROLE telephone_station WITH
LOGIN
CONNECTION LIMIT -1
PASSWORD '123456qwerty';
GRANT pg_signal_backend TO telephone_station WITH ADMIN OPTION;
--- Мобильное приложение: создаем роль, устанавливаем на нее простейший пароль
CREATE ROLE mobile_app WITH
LOGIN
CONNECTION LIMIT -1
PASSWORD '1qwerty23456';
GRANT pg_signal_backend TO mobile_app WITH ADMIN OPTION;

--- Даём доступы
-- АРМ Менеджера: работа со справочником тарифов - просмотр справочника тарифов
GRANT SELECT, INSERT, UPDATE, DELETE ON tariffs_catalog TO manager;
-- АРМ Менеджера: работа со справочником тарифов - добавление нового тарифа в справочник
GRANT EXECUTE ON PROCEDURE insert_tariff TO manager;
-- АРМ Менеджера: работа со справочником тарифов: удаление тарифа по id
GRANT EXECUTE ON PROCEDURE delete_tariff TO manager;
-- АРМ Менеджера: работа со справочником тарифов: изменение тарифа по id
GRANT EXECUTE ON PROCEDURE update_tariff TO manager;
-- АРМ Менеджера: создание абонента
GRANT SELECT, INSERT ON subscriber_view TO manager;
GRANT SELECT, INSERT, UPDATE ON st_view TO manager;
GRANT EXECUTE ON PROCEDURE subscriber_create TO manager;
-- АРМ Менеджера: Процедура: Пополнение баланса абонента
GRANT EXECUTE ON PROCEDURE balance_replenishment TO manager;

-- Телефонная станция: запрос доступного кол-ва минут
GRANT EXECUTE ON FUNCTION select_minute_quantity TO telephone_station;
GRANT SELECT, UPDATE ON st_view TO telephone_station;
-- Телефонная станция: списание за совершенный звонок
GRANT SELECT, INSERT, UPDATE, DELETE ON all_call_fees TO telephone_station;
-- (Процедура звонка для теста) Сделаем процедуру звонка для удобства проверки списания за каждый звонок
GRANT EXECUTE ON PROCEDURE new_call TO telephone_station;


-- Мобильное приложение: просмотр истории списаний/зачислений денежных средств
GRANT SELECT ON transactions_history TO mobile_app;
-- Мобильное приложение: перевод денег другому абоненту
GRANT EXECUTE ON PROCEDURE money_transfer TO mobile_app;
-- Доступ к представлению таблицы subscriber_tariff для мобильного приложения:
GRANT SELECT, UPDATE ON st_view TO mobile_app;

-- Для того, чтобы делать операции INSERT, DELETE, UPDATE с любой таблицей не напрямую
-- (а через функции/процедуры), нужно в любом случае иметь доступ к операциям для изменений в таблице
-- при использовании роли.
GRANT INSERT, UPDATE, DELETE ON tariff TO manager;
GRANT INSERT ON subscriber TO manager;
GRANT INSERT, UPDATE ON subscriber_tariff TO manager;
GRANT INSERT ON transactions TO manager;
GRANT INSERT, UPDATE ON calls TO telephone_station;
GRANT INSERT ON transactions TO telephone_station;
GRANT UPDATE ON subscriber_tariff TO mobile_app;
GRANT INSERT ON transactions TO mobile_app;
-- Также необходимо выдать права на пользования последовательностями для корректной работы процедур/функций над представлениями:
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO manager, telephone_station, mobile_app;

-- Реализованный подход не даёт доступа к содержимому таблиц напрямую - только через представления.
-- Все написанные функции и процедуры также работают только с представлениями.