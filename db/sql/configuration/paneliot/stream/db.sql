--------------------------------------------------------------------------------
-- STREAM ----------------------------------------------------------------------
--------------------------------------------------------------------------------

CREATE TYPE stream.TLPWAN_PACKAGE AS (
  length        int4,
  version       int2,
  params        bit(8),
  type          int2,
  serial_size   int2,
  serial        text,
  n_command     int2,
  n_package     int2,
  command       bytea,
  crc16         int4
);

CREATE TYPE stream.TLPWAN_COMMAND AS (
  date          timestamp,
  type          int2,
  code          int2,
  data          bytea
);

-- Команда 0x04. Текущие значения
CREATE TYPE stream.TLPWAN_CMD_CURRENT_VALUE AS (
  type          int2,
  size          int2,
  data          bytea
);

--------------------------------------------------------------------------------
-- stream.log ------------------------------------------------------------------
--------------------------------------------------------------------------------

CREATE TABLE stream.log (
    id          numeric PRIMARY KEY DEFAULT NEXTVAL('SEQUENCE_STREAM_LOG'),
    datetime	timestamptz DEFAULT clock_timestamp() NOT NULL,
    username	text NOT NULL DEFAULT session_user,
    protocol    text NOT NULL,
    identity	text NOT NULL,
    request     bytea,
    response	bytea,
    runtime     interval,
    message     text
);

COMMENT ON TABLE stream.log IS 'Лог потоковых данных.';

COMMENT ON COLUMN stream.log.id IS 'Идентификатор';
COMMENT ON COLUMN stream.log.datetime IS 'Дата и время';
COMMENT ON COLUMN stream.log.username IS 'Пользователь СУБД';
COMMENT ON COLUMN stream.log.protocol IS 'Протокол';
COMMENT ON COLUMN stream.log.identity IS 'Идентификатор';
COMMENT ON COLUMN stream.log.request IS 'Запрос';
COMMENT ON COLUMN stream.log.response IS 'Ответ';
COMMENT ON COLUMN stream.log.runtime IS 'Время выполнения запроса';
COMMENT ON COLUMN stream.log.message IS 'Информация об ошибке';

CREATE INDEX ON stream.log (protocol);
CREATE INDEX ON stream.log (identity);
CREATE INDEX ON stream.log (datetime);

--------------------------------------------------------------------------------
-- stream.WriteToLog -----------------------------------------------------------
--------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION stream.WriteToLog (
  pProtocol	text,
  pIdentity	text,
  pRequest	bytea default null,
  pResponse	bytea default null,
  pRunTime	interval default null,
  pMessage	text default null
) RETURNS	numeric
AS $$
DECLARE
  nId		numeric;
BEGIN
  INSERT INTO stream.log (protocol, identity, request, response, runtime, message)
  VALUES (pProtocol, pIdentity, pRequest, pResponse, pRunTime, pMessage)
  RETURNING id INTO nId;

  RETURN nId;
END;
$$ LANGUAGE plpgsql
   SECURITY DEFINER
   SET search_path = kernel, pg_temp;

--------------------------------------------------------------------------------
-- stream.ClearLog -------------------------------------------------------------
--------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION stream.ClearLog (
  pDateTime	timestamptz
) RETURNS	void
AS $$
BEGIN
  DELETE FROM stream.log WHERE datetime < pDateTime;
END;
$$ LANGUAGE plpgsql
   SECURITY DEFINER
   SET search_path = kernel, pg_temp;

--------------------------------------------------------------------------------
-- VIEW streamLog --------------------------------------------------------------
--------------------------------------------------------------------------------

CREATE OR REPLACE VIEW streamLog (Id, DateTime, UserName, Protocol,
  Identity, Request, RequestLength, Response, ResponseLength, RunTime, Message)
AS
  SELECT id, datetime, username, protocol, identity,
         encode(request, 'hex'), octet_length(request),
         encode(response, 'hex'), octet_length(response),
         round(extract(second from runtime)::numeric, 3),
         message
    FROM stream.log;

--------------------------------------------------------------------------------
-- stream.lpwan_package --------------------------------------------------------
--------------------------------------------------------------------------------

CREATE TABLE stream.lpwan_package (
    id              numeric PRIMARY KEY DEFAULT NEXTVAL('SEQUENCE_STREAM_LPWAN'),
    datetime        timestamptz DEFAULT clock_timestamp() NOT NULL,
    length          int4 NOT NULL,
    version         int2 NOT NULL,
    params          bit(8) NOT NULL,
    type            int2 NOT NULL,
    serial_size     int2 NOT NULL,
    serial          text NOT NULL,
    n_command       int2 NOT NULL,
    n_package       int2 NOT NULL,
    command         bytea NOT NULL,
    crc16           int4 NOT NULL
);

COMMENT ON TABLE stream.lpwan_package IS 'Общая структура пакета LPWAN.';

COMMENT ON COLUMN stream.lpwan_package.id IS 'Идентификатор';
COMMENT ON COLUMN stream.lpwan_package.datetime IS 'Дата и время';
COMMENT ON COLUMN stream.lpwan_package.length IS 'Длина всего пакета (все поля кроме поля длины) в байтах';
COMMENT ON COLUMN stream.lpwan_package.version IS 'Версия протокола';
COMMENT ON COLUMN stream.lpwan_package.params IS 'Параметры: 0 бит – начальный пакет команды; 1 бит – конечный пакет команды; 2 бит – пакет к счетчику; 3 бит – ответ на запрос; 4 бит – команда упакована (Zlib); 5 бит – команда зашифрована (AES 128); 6 бит – квитанция';
COMMENT ON COLUMN stream.lpwan_package.type IS 'Тип счетчика: meter_type';
COMMENT ON COLUMN stream.lpwan_package.serial_size IS 'Размер серийного номера';
COMMENT ON COLUMN stream.lpwan_package.serial IS 'Серийный номер';
COMMENT ON COLUMN stream.lpwan_package.n_command IS 'Номер команды: Циклически от 0 до 255. При ответе на запрос подставляется из запроса';
COMMENT ON COLUMN stream.lpwan_package.n_package IS 'Номер пакета: Номер пакета команды от 0 до 255 (для каждой команды от 0)';
COMMENT ON COLUMN stream.lpwan_package.command IS 'Данные пакета (команда)';
COMMENT ON COLUMN stream.lpwan_package.crc16 IS 'Контрольная сумма (CRC16)';

CREATE INDEX ON stream.lpwan_package (datetime);
CREATE INDEX ON stream.lpwan_package (serial);

--------------------------------------------------------------------------------
-- stream.lpwan_command --------------------------------------------------------
--------------------------------------------------------------------------------

CREATE TABLE stream.lpwan_command (
    package         numeric NOT NULL,
    n_command       int2 NOT NULL,
    n_package       int2 NOT NULL,
    date            timestamp,
    type            int2,
    code            int2,
    data            bytea,
    CONSTRAINT pk_lpwan_command PRIMARY KEY(package, n_command, n_package)
);

COMMENT ON TABLE stream.lpwan_command IS 'Общая структура пакета LPWAN.';

COMMENT ON COLUMN stream.lpwan_command.package IS 'Идентификатор пакета данных';
COMMENT ON COLUMN stream.lpwan_command.n_command IS 'Номер команды: Циклически от 0 до 255. При ответе на запрос подставляется из запроса';
COMMENT ON COLUMN stream.lpwan_command.n_package IS 'Номер пакета: Номер пакета команды от 0 до 255 (для каждой команды от 0)';
COMMENT ON COLUMN stream.lpwan_command.date IS 'Метка времени';
COMMENT ON COLUMN stream.lpwan_command.type IS 'Тип команды';
COMMENT ON COLUMN stream.lpwan_command.code IS 'Код ошибки';
COMMENT ON COLUMN stream.lpwan_command.data IS 'Данные команды';

CREATE INDEX ON stream.lpwan_command (date);
CREATE INDEX ON stream.lpwan_command (type);

--------------------------------------------------------------------------------
-- stream.GetCRC16 -------------------------------------------------------------
--------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION stream.GetCRC16 (
  buffer        bytea,
  size          int
) RETURNS       int
AS $$
DECLARE
  crc           int;
BEGIN
  crc := 65535;
  for i in 0..size - 1
  loop
    crc := crc # get_byte(buffer, i);
    for j in 0..7
    loop
      if (crc & 1) = 1 then
        crc := (crc >> 1) # 40961; -- 0xA001
      else
        crc := crc >> 1;
      end if;
    end loop;
  end loop;
  return crc; --(crc & 255) << 8 | (crc >> 8);
END;
$$ LANGUAGE plpgsql
   SECURITY DEFINER
   SET search_path = kernel, pg_temp;

--------------------------------------------------------------------------------
-- stream.GetDataTypeInfo ------------------------------------------------------
--------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION stream.GetDataTypeInfo (
  pType         text
) RETURNS       jsonb
AS $$
BEGIN
  CASE pType
  WHEN 'energy_active' THEN
    RETURN jsonb_build_object('data_type', pType, 'data_label', 'Показание активной энергии, Ватт*час');
  WHEN 'energy_reactive' THEN
    RETURN jsonb_build_object('data_type', pType, 'data_label', 'Показание реактивной энергии, Вар*час');
  WHEN 'energy_apparent' THEN
    RETURN jsonb_build_object('data_type', pType, 'data_label', 'Показание полной энергии, ВА*час');
  WHEN 'energy_period_active' THEN
    RETURN jsonb_build_object('data_type', pType, 'data_label', 'Потребление активной энергии за период, Ватт*час, время значения – это время на конец периода');
  WHEN 'energy_period_reactive' THEN
    RETURN jsonb_build_object('data_type', pType, 'data_label', 'Потребление реактивной энергии за период, Вар*час, время значения – это время на конец периода');
  WHEN 'energy_period_apparent' THEN
    RETURN jsonb_build_object('data_type', pType, 'data_label', 'Потребление полной энергии за период, ВА*час, время значения – это время на конец периода');
  WHEN 'power_active' THEN
    RETURN jsonb_build_object('data_type', pType, 'data_label', 'Активная мощность, Ватт');
  WHEN 'power_reactive' THEN
    RETURN jsonb_build_object('data_type', pType, 'data_label', 'Реактивная мощность, Вар');
  WHEN 'power_apparent' THEN
    RETURN jsonb_build_object('data_type', pType, 'data_label', 'Полная мощность, ВА');
  WHEN 'power_period_active' THEN
    RETURN jsonb_build_object('data_type', pType, 'data_label', 'Максимальная активная мощность за период, Ватт, время значения – это время на конец периода');
  WHEN 'power_period_reactive' THEN
    RETURN jsonb_build_object('data_type', pType, 'data_label', 'Максимальная реактивная мощность за период, Вар, время значения – это время на конец периода');
  WHEN 'power_period_apparent' THEN
    RETURN jsonb_build_object('data_type', pType, 'data_label', 'Максимальная полная мощность за период, ВА, время значения – это время на конец периода');
  WHEN 'curr' THEN
    RETURN jsonb_build_object('data_type', pType, 'data_label', 'Ток, Ампер');
  WHEN 'volt' THEN
    RETURN jsonb_build_object('data_type', pType, 'data_label', 'Напряжение. Вольт');
  WHEN 'cosf' THEN
    RETURN jsonb_build_object('data_type', pType, 'data_label', 'cos Ф');
  WHEN 'freq' THEN
    RETURN jsonb_build_object('data_type', pType, 'data_label', 'Частота. Герц');
  WHEN 'temper' THEN
    RETURN jsonb_build_object('data_type', pType, 'data_label', 'Температура, градусы Цельсия');
  WHEN 'degree' THEN
    RETURN jsonb_build_object('data_type', pType, 'data_label', 'Градусы');
  WHEN 'percent' THEN
    RETURN jsonb_build_object('data_type', pType, 'data_label', 'Проценты');
  WHEN 'loss_line' THEN
    RETURN jsonb_build_object('data_type', pType, 'data_label', 'Удельная энергия потерь в цепях тока, A2*час');
  WHEN 'loss_transform' THEN
    RETURN jsonb_build_object('data_type', pType, 'data_label', 'Удельная энергия потерь в силовых трансформаторах, V2*час');
  WHEN 'load_state' THEN
    RETURN jsonb_build_object('data_type', pType, 'data_label', 'Состояние нагрузки (1 – включена, 2 – выключена)');
  WHEN 'meter_state' THEN
    RETURN jsonb_build_object('data_type', pType, 'data_label', 'Состояние счетчика');
  WHEN 'second' THEN
    RETURN jsonb_build_object('data_type', pType, 'data_label', 'Секунд');
  WHEN 'byte' THEN
    RETURN jsonb_build_object('data_type', pType, 'data_label', 'Байт');
  WHEN 'time' THEN
    RETURN jsonb_build_object('data_type', pType, 'data_label', 'Локальное время');
  WHEN 'time_utc' THEN
    RETURN jsonb_build_object('data_type', pType, 'data_label', 'Всемирное координированное время (UTC)');
  WHEN 'time_day' THEN
    RETURN jsonb_build_object('data_type', pType, 'data_label', 'Время в секундах с начала суток');
  WHEN 'utf8' THEN
    RETURN jsonb_build_object('data_type', pType, 'data_label', 'Строка в кодировке UTF-8');
  END CASE;

  RETURN jsonb_build_object();
END
$$ LANGUAGE plpgsql
   SECURITY DEFINER
   SET search_path = kernel, pg_temp;

--------------------------------------------------------------------------------
-- stream.GetUInt64 ------------------------------------------------------------
--------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION stream.GetUInt64 (
  pData         bytea
) RETURNS       numeric
AS $$
BEGIN
  RETURN (get_byte(pData, 7) << 56) | (get_byte(pData, 6) << 48) | (get_byte(pData, 5) << 40) | (get_byte(pData, 4) << 32) |
         (get_byte(pData, 3) << 24) | (get_byte(pData, 2) << 16) | (get_byte(pData, 1) <<  8) | (get_byte(pData, 0));
END
$$ LANGUAGE plpgsql
   SECURITY DEFINER
   SET search_path = kernel, pg_temp;

--------------------------------------------------------------------------------
-- stream.GetUInt48 ------------------------------------------------------------
--------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION stream.GetUInt48 (
  pData         bytea
) RETURNS       numeric
AS $$
BEGIN
  RETURN (get_byte(pData, 5) << 40) | (get_byte(pData, 4) << 32) | (get_byte(pData, 3) << 24) |
         (get_byte(pData, 2) << 16) | (get_byte(pData, 1) <<  8) | (get_byte(pData, 0));
END
$$ LANGUAGE plpgsql
   SECURITY DEFINER
   SET search_path = kernel, pg_temp;

--------------------------------------------------------------------------------
-- stream.GetUInt32 ------------------------------------------------------------
--------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION stream.GetUInt32 (
  pData         bytea
) RETURNS       numeric
AS $$
BEGIN
  RETURN (get_byte(pData, 3) << 24) | (get_byte(pData, 2) << 16) | (get_byte(pData, 1) << 8) | (get_byte(pData, 0));
END
$$ LANGUAGE plpgsql
   SECURITY DEFINER
   SET search_path = kernel, pg_temp;

--------------------------------------------------------------------------------
-- stream.GetUInt24 ------------------------------------------------------------
--------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION stream.GetUInt24 (
  pData         bytea
) RETURNS       numeric
AS $$
BEGIN
  RETURN (get_byte(pData, 2) << 16) | (get_byte(pData, 1) << 8) | (get_byte(pData, 0));
END
$$ LANGUAGE plpgsql
   SECURITY DEFINER
   SET search_path = kernel, pg_temp;

--------------------------------------------------------------------------------
-- stream.GetUInt16 ------------------------------------------------------------
--------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION stream.GetUInt16 (
  pData         bytea
) RETURNS       numeric
AS $$
BEGIN
  RETURN (get_byte(pData, 1) << 8) | (get_byte(pData, 0));
END
$$ LANGUAGE plpgsql
   SECURITY DEFINER
   SET search_path = kernel, pg_temp;

--------------------------------------------------------------------------------
-- stream.GetUInt8 -------------------------------------------------------------
--------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION stream.GetUInt8 (
  pData         bytea
) RETURNS       numeric
AS $$
BEGIN
  RETURN get_byte(pData, 0);
END
$$ LANGUAGE plpgsql
   SECURITY DEFINER
   SET search_path = kernel, pg_temp;

--------------------------------------------------------------------------------
-- stream.ByTariff48 -----------------------------------------------------------
--------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION stream.ByTariff48 (
  pData         bytea
) RETURNS       numeric[]
AS $$
DECLARE
  Result        numeric[];
  Count         int;
  UnitSize      int DEFAULT 6; -- 6 байт в UInt48
BEGIN
  Count := octet_length(pData) / UnitSize;

  FOR key IN 0..Count - 1
  LOOP
    Result[key] := stream.GetUInt48(substr(pData, key * UnitSize + 1, UnitSize));
  END LOOP;

  RETURN Result;
END
$$ LANGUAGE plpgsql
   SECURITY DEFINER
   SET search_path = kernel, pg_temp;

--------------------------------------------------------------------------------
-- stream.ByTariff32 -----------------------------------------------------------
--------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION stream.ByTariff32 (
  pData         bytea
) RETURNS       numeric[]
AS $$
DECLARE
  Result        numeric[];
  Count         int;
  UnitSize      int DEFAULT 4; -- 4 байта в UInt32
BEGIN
  Count := octet_length(pData) / UnitSize;

  FOR key IN 0..Count - 1
  LOOP
    Result[key] := stream.GetUInt32(substr(pData, key * UnitSize + 1, UnitSize));
  END LOOP;

  RETURN Result;
END
$$ LANGUAGE plpgsql
   SECURITY DEFINER
   SET search_path = kernel, pg_temp;

--------------------------------------------------------------------------------
-- stream.ByTariff24 -----------------------------------------------------------
--------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION stream.ByTariff24 (
  pData         bytea
) RETURNS       numeric[]
AS $$
DECLARE
  Result        numeric[];
  Count         int;
  UnitSize      int DEFAULT 3; -- 3 байта в UInt24
BEGIN
  Count := octet_length(pData) / UnitSize;

  FOR key IN 0..Count - 1
  LOOP
    Result[key] := stream.GetUInt24(substr(pData, key * UnitSize + 1, UnitSize));
  END LOOP;

  RETURN Result;
END
$$ LANGUAGE plpgsql
   SECURITY DEFINER
   SET search_path = kernel, pg_temp;

--------------------------------------------------------------------------------
-- stream.GetCurrentValue ------------------------------------------------------
--------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION stream.GetCurrentValue (
  pValue        stream.TLPWAN_CMD_CURRENT_VALUE
) RETURNS       jsonb
AS $$
BEGIN
  CASE pValue.type
  WHEN 1 THEN
    RETURN jsonb_build_object('value', stream.ByTariff48(pValue.data), 'value_type', pValue.type, 'value_name', 'Энергия, A+') || stream.GetDataTypeInfo('energy_active');
  WHEN 2 THEN
    RETURN jsonb_build_object('value', stream.ByTariff48(pValue.data), 'value_type', pValue.type, 'value_name', 'Энергия, A-') || stream.GetDataTypeInfo('energy_active');
  WHEN 3 THEN
    RETURN jsonb_build_object('value', stream.ByTariff48(pValue.data), 'value_type', pValue.type, 'value_name', 'Энергия, A, Q1') || stream.GetDataTypeInfo('energy_active');
  WHEN 4 THEN
    RETURN jsonb_build_object('value', stream.ByTariff48(pValue.data), 'value_type', pValue.type, 'value_name', 'Энергия, A, Q2') || stream.GetDataTypeInfo('energy_active');
  WHEN 5 THEN
    RETURN jsonb_build_object('value', stream.ByTariff48(pValue.data), 'value_type', pValue.type, 'value_name', 'Энергия, A, Q3') || stream.GetDataTypeInfo('energy_active');
  WHEN 6 THEN
    RETURN jsonb_build_object('value', stream.ByTariff48(pValue.data), 'value_type', pValue.type, 'value_name', 'Энергия, A, Q4') || stream.GetDataTypeInfo('energy_active');
  WHEN 7 THEN
    RETURN jsonb_build_object('value', stream.ByTariff48(pValue.data), 'value_type', pValue.type, 'value_name', 'Энергия, A+, фаза 1') || stream.GetDataTypeInfo('energy_active');
  WHEN 8 THEN
    RETURN jsonb_build_object('value', stream.ByTariff48(pValue.data), 'value_type', pValue.type, 'value_name', 'Энергия, A-, фаза 1') || stream.GetDataTypeInfo('energy_active');
  WHEN 9 THEN
    RETURN jsonb_build_object('value', stream.ByTariff48(pValue.data), 'value_type', pValue.type, 'value_name', 'Энергия, A, Q1, фаза 1') || stream.GetDataTypeInfo('energy_active');
  WHEN 10 THEN
    RETURN jsonb_build_object('value', stream.ByTariff48(pValue.data), 'value_type', pValue.type, 'value_name', 'Энергия, A, Q2, фаза 1') || stream.GetDataTypeInfo('energy_active');
  WHEN 11 THEN
    RETURN jsonb_build_object('value', stream.ByTariff48(pValue.data), 'value_type', pValue.type, 'value_name', 'Энергия, A, Q3, фаза 1') || stream.GetDataTypeInfo('energy_active');
  WHEN 12 THEN
    RETURN jsonb_build_object('value', stream.ByTariff48(pValue.data), 'value_type', pValue.type, 'value_name', 'Энергия, A, Q4, фаза 1') || stream.GetDataTypeInfo('energy_active');
  WHEN 13 THEN
    RETURN jsonb_build_object('value', stream.ByTariff48(pValue.data), 'value_type', pValue.type, 'value_name', 'Энергия, A+, фаза 2') || stream.GetDataTypeInfo('energy_active');
  WHEN 14 THEN
    RETURN jsonb_build_object('value', stream.ByTariff48(pValue.data), 'value_type', pValue.type, 'value_name', 'Энергия, A-, фаза 2') || stream.GetDataTypeInfo('energy_active');
  WHEN 15 THEN
    RETURN jsonb_build_object('value', stream.ByTariff48(pValue.data), 'value_type', pValue.type, 'value_name', 'Энергия, A, Q1, фаза 2') || stream.GetDataTypeInfo('energy_active');
  WHEN 16 THEN
    RETURN jsonb_build_object('value', stream.ByTariff48(pValue.data), 'value_type', pValue.type, 'value_name', 'Энергия, A, Q2, фаза 2') || stream.GetDataTypeInfo('energy_active');
  WHEN 17 THEN
    RETURN jsonb_build_object('value', stream.ByTariff48(pValue.data), 'value_type', pValue.type, 'value_name', 'Энергия, A, Q3, фаза 2') || stream.GetDataTypeInfo('energy_active');
  WHEN 18 THEN
    RETURN jsonb_build_object('value', stream.ByTariff48(pValue.data), 'value_type', pValue.type, 'value_name', 'Энергия, A, Q4, фаза 2') || stream.GetDataTypeInfo('energy_active');
  WHEN 19 THEN
    RETURN jsonb_build_object('value', stream.ByTariff48(pValue.data), 'value_type', pValue.type, 'value_name', 'Энергия, A+, фаза 3') || stream.GetDataTypeInfo('energy_active');
  WHEN 20 THEN
    RETURN jsonb_build_object('value', stream.ByTariff48(pValue.data), 'value_type', pValue.type, 'value_name', 'Энергия, A-, фаза 3') || stream.GetDataTypeInfo('energy_active');
  WHEN 21 THEN
    RETURN jsonb_build_object('value', stream.ByTariff48(pValue.data), 'value_type', pValue.type, 'value_name', 'Энергия, A, Q1, фаза 3') || stream.GetDataTypeInfo('energy_active');
  WHEN 22 THEN
    RETURN jsonb_build_object('value', stream.ByTariff48(pValue.data), 'value_type', pValue.type, 'value_name', 'Энергия, A, Q2, фаза 3') || stream.GetDataTypeInfo('energy_active');
  WHEN 23 THEN
    RETURN jsonb_build_object('value', stream.ByTariff48(pValue.data), 'value_type', pValue.type, 'value_name', 'Энергия, A, Q3, фаза 3') || stream.GetDataTypeInfo('energy_active');
  WHEN 24 THEN
    RETURN jsonb_build_object('value', stream.ByTariff48(pValue.data), 'value_type', pValue.type, 'value_name', 'Энергия, A, Q4, фаза 3') || stream.GetDataTypeInfo('energy_active');
  WHEN 25 THEN
    RETURN jsonb_build_object('value', stream.ByTariff48(pValue.data), 'value_type', pValue.type, 'value_name', 'Энергия, R+') || stream.GetDataTypeInfo('energy_reactive');
  WHEN 26 THEN
    RETURN jsonb_build_object('value', stream.ByTariff48(pValue.data), 'value_type', pValue.type, 'value_name', 'Энергия, R-') || stream.GetDataTypeInfo('energy_reactive');
  WHEN 27 THEN
    RETURN jsonb_build_object('value', stream.ByTariff48(pValue.data), 'value_type', pValue.type, 'value_name', 'Энергия, R, Q1') || stream.GetDataTypeInfo('energy_reactive');
  WHEN 28 THEN
    RETURN jsonb_build_object('value', stream.ByTariff48(pValue.data), 'value_type', pValue.type, 'value_name', 'Энергия, R, Q2') || stream.GetDataTypeInfo('energy_reactive');
  WHEN 29 THEN
    RETURN jsonb_build_object('value', stream.ByTariff48(pValue.data), 'value_type', pValue.type, 'value_name', 'Энергия, R, Q3') || stream.GetDataTypeInfo('energy_reactive');
  WHEN 30 THEN
    RETURN jsonb_build_object('value', stream.ByTariff48(pValue.data), 'value_type', pValue.type, 'value_name', 'Энергия, R, Q4') || stream.GetDataTypeInfo('energy_reactive');
  WHEN 31 THEN
    RETURN jsonb_build_object('value', stream.ByTariff48(pValue.data), 'value_type', pValue.type, 'value_name', 'Энергия, R+, фаза 1') || stream.GetDataTypeInfo('energy_reactive');
  WHEN 32 THEN
    RETURN jsonb_build_object('value', stream.ByTariff48(pValue.data), 'value_type', pValue.type, 'value_name', 'Энергия, R-, фаза 1') || stream.GetDataTypeInfo('energy_reactive');
  WHEN 33 THEN
    RETURN jsonb_build_object('value', stream.ByTariff48(pValue.data), 'value_type', pValue.type, 'value_name', 'Энергия, R, Q1, фаза 1') || stream.GetDataTypeInfo('energy_reactive');
  WHEN 34 THEN
    RETURN jsonb_build_object('value', stream.ByTariff48(pValue.data), 'value_type', pValue.type, 'value_name', 'Энергия, R, Q2, фаза 1') || stream.GetDataTypeInfo('energy_reactive');
  WHEN 35 THEN
    RETURN jsonb_build_object('value', stream.ByTariff48(pValue.data), 'value_type', pValue.type, 'value_name', 'Энергия, R, Q3, фаза 1') || stream.GetDataTypeInfo('energy_reactive');
  WHEN 36 THEN
    RETURN jsonb_build_object('value', stream.ByTariff48(pValue.data), 'value_type', pValue.type, 'value_name', 'Энергия, R, Q4, фаза 1') || stream.GetDataTypeInfo('energy_reactive');
  WHEN 37 THEN
    RETURN jsonb_build_object('value', stream.ByTariff48(pValue.data), 'value_type', pValue.type, 'value_name', 'Энергия, R+, фаза 2') || stream.GetDataTypeInfo('energy_reactive');
  WHEN 38 THEN
    RETURN jsonb_build_object('value', stream.ByTariff48(pValue.data), 'value_type', pValue.type, 'value_name', 'Энергия, R-, фаза 2') || stream.GetDataTypeInfo('energy_reactive');
  WHEN 39 THEN
    RETURN jsonb_build_object('value', stream.ByTariff48(pValue.data), 'value_type', pValue.type, 'value_name', 'Энергия, R, Q1, фаза 2') || stream.GetDataTypeInfo('energy_reactive');
  WHEN 40 THEN
    RETURN jsonb_build_object('value', stream.ByTariff48(pValue.data), 'value_type', pValue.type, 'value_name', 'Энергия, R, Q2, фаза 2') || stream.GetDataTypeInfo('energy_reactive');
  WHEN 41 THEN
    RETURN jsonb_build_object('value', stream.ByTariff48(pValue.data), 'value_type', pValue.type, 'value_name', 'Энергия, R, Q3, фаза 2') || stream.GetDataTypeInfo('energy_reactive');
  WHEN 43 THEN
    RETURN jsonb_build_object('value', stream.ByTariff48(pValue.data), 'value_type', pValue.type, 'value_name', 'Энергия, R, Q4, фаза 2') || stream.GetDataTypeInfo('energy_reactive');
  WHEN 43 THEN
    RETURN jsonb_build_object('value', stream.ByTariff48(pValue.data), 'value_type', pValue.type, 'value_name', 'Энергия, R+, фаза 3') || stream.GetDataTypeInfo('energy_reactive');
  WHEN 44 THEN
    RETURN jsonb_build_object('value', stream.ByTariff48(pValue.data), 'value_type', pValue.type, 'value_name', 'Энергия, R-, фаза 3') || stream.GetDataTypeInfo('energy_reactive');
  WHEN 45 THEN
    RETURN jsonb_build_object('value', stream.ByTariff48(pValue.data), 'value_type', pValue.type, 'value_name', 'Энергия, R, Q1, фаза 3') || stream.GetDataTypeInfo('energy_reactive');
  WHEN 46 THEN
    RETURN jsonb_build_object('value', stream.ByTariff48(pValue.data), 'value_type', pValue.type, 'value_name', 'Энергия, R, Q2, фаза 3') || stream.GetDataTypeInfo('energy_reactive');
  WHEN 47 THEN
    RETURN jsonb_build_object('value', stream.ByTariff48(pValue.data), 'value_type', pValue.type, 'value_name', 'Энергия, R, Q3, фаза 3') || stream.GetDataTypeInfo('energy_reactive');
  WHEN 48 THEN
    RETURN jsonb_build_object('value', stream.ByTariff48(pValue.data), 'value_type', pValue.type, 'value_name', 'Энергия, R, Q4, фаза 3') || stream.GetDataTypeInfo('energy_reactive');
  WHEN 49 THEN
    RETURN jsonb_build_object('value', stream.ByTariff48(pValue.data), 'value_type', pValue.type, 'value_name', 'Энергия, S+') || stream.GetDataTypeInfo('energy_apparent');
  WHEN 50 THEN
    RETURN jsonb_build_object('value', stream.ByTariff48(pValue.data), 'value_type', pValue.type, 'value_name', 'Энергия, S-') || stream.GetDataTypeInfo('energy_apparent');
  WHEN 51 THEN
    RETURN jsonb_build_object('value', stream.ByTariff48(pValue.data), 'value_type', pValue.type, 'value_name', 'Энергия, S, Q1') || stream.GetDataTypeInfo('energy_apparent');
  WHEN 52 THEN
    RETURN jsonb_build_object('value', stream.ByTariff48(pValue.data), 'value_type', pValue.type, 'value_name', 'Энергия, S, Q2') || stream.GetDataTypeInfo('energy_apparent');
  WHEN 53 THEN
    RETURN jsonb_build_object('value', stream.ByTariff48(pValue.data), 'value_type', pValue.type, 'value_name', 'Энергия, S, Q3') || stream.GetDataTypeInfo('energy_apparent');
  WHEN 54 THEN
    RETURN jsonb_build_object('value', stream.ByTariff48(pValue.data), 'value_type', pValue.type, 'value_name', 'Энергия, S, Q4') || stream.GetDataTypeInfo('energy_apparent');
  WHEN 55 THEN
    RETURN jsonb_build_object('value', stream.ByTariff48(pValue.data), 'value_type', pValue.type, 'value_name', 'Энергия, S+, фаза 1') || stream.GetDataTypeInfo('energy_apparent');
  WHEN 56 THEN
    RETURN jsonb_build_object('value', stream.ByTariff48(pValue.data), 'value_type', pValue.type, 'value_name', 'Энергия, S-, фаза 1') || stream.GetDataTypeInfo('energy_apparent');
  WHEN 57 THEN
    RETURN jsonb_build_object('value', stream.ByTariff48(pValue.data), 'value_type', pValue.type, 'value_name', 'Энергия, S, Q1, фаза 1') || stream.GetDataTypeInfo('energy_apparent');
  WHEN 58 THEN
    RETURN jsonb_build_object('value', stream.ByTariff48(pValue.data), 'value_type', pValue.type, 'value_name', 'Энергия, S, Q2, фаза 1') || stream.GetDataTypeInfo('energy_apparent');
  WHEN 59 THEN
    RETURN jsonb_build_object('value', stream.ByTariff48(pValue.data), 'value_type', pValue.type, 'value_name', 'Энергия, S, Q3, фаза 1') || stream.GetDataTypeInfo('energy_apparent');
  WHEN 60 THEN
    RETURN jsonb_build_object('value', stream.ByTariff48(pValue.data), 'value_type', pValue.type, 'value_name', 'Энергия, S, Q4, фаза 1') || stream.GetDataTypeInfo('energy_apparent');
  WHEN 61 THEN
    RETURN jsonb_build_object('value', stream.ByTariff48(pValue.data), 'value_type', pValue.type, 'value_name', 'Энергия, S+, фаза 2') || stream.GetDataTypeInfo('energy_apparent');
  WHEN 62 THEN
    RETURN jsonb_build_object('value', stream.ByTariff48(pValue.data), 'value_type', pValue.type, 'value_name', 'Энергия, S-, фаза 2') || stream.GetDataTypeInfo('energy_apparent');
  WHEN 63 THEN
    RETURN jsonb_build_object('value', stream.ByTariff48(pValue.data), 'value_type', pValue.type, 'value_name', 'Энергия, S, Q1, фаза 2') || stream.GetDataTypeInfo('energy_apparent');
  WHEN 64 THEN
    RETURN jsonb_build_object('value', stream.ByTariff48(pValue.data), 'value_type', pValue.type, 'value_name', 'Энергия, S, Q2, фаза 2') || stream.GetDataTypeInfo('energy_apparent');
  WHEN 65 THEN
    RETURN jsonb_build_object('value', stream.ByTariff48(pValue.data), 'value_type', pValue.type, 'value_name', 'Энергия, S, Q3, фаза 2') || stream.GetDataTypeInfo('energy_apparent');
  WHEN 66 THEN
    RETURN jsonb_build_object('value', stream.ByTariff48(pValue.data), 'value_type', pValue.type, 'value_name', 'Энергия, S, Q4, фаза 2') || stream.GetDataTypeInfo('energy_apparent');
  WHEN 67 THEN
    RETURN jsonb_build_object('value', stream.ByTariff48(pValue.data), 'value_type', pValue.type, 'value_name', 'Энергия, S+, фаза 3') || stream.GetDataTypeInfo('energy_apparent');
  WHEN 68 THEN
    RETURN jsonb_build_object('value', stream.ByTariff48(pValue.data), 'value_type', pValue.type, 'value_name', 'Энергия, S-, фаза 3') || stream.GetDataTypeInfo('energy_apparent');
  WHEN 69 THEN
    RETURN jsonb_build_object('value', stream.ByTariff48(pValue.data), 'value_type', pValue.type, 'value_name', 'Энергия, S, Q1, фаза 3') || stream.GetDataTypeInfo('energy_apparent');
  WHEN 70 THEN
    RETURN jsonb_build_object('value', stream.ByTariff48(pValue.data), 'value_type', pValue.type, 'value_name', 'Энергия, S, Q2, фаза 3') || stream.GetDataTypeInfo('energy_apparent');
  WHEN 71 THEN
    RETURN jsonb_build_object('value', stream.ByTariff48(pValue.data), 'value_type', pValue.type, 'value_name', 'Энергия, S, Q3, фаза 3') || stream.GetDataTypeInfo('energy_apparent');
  WHEN 72 THEN
    RETURN jsonb_build_object('value', stream.ByTariff48(pValue.data), 'value_type', pValue.type, 'value_name', 'Энергия, S, Q4, фаза 3') || stream.GetDataTypeInfo('energy_apparent');
  WHEN 73 THEN
    RETURN jsonb_build_object('value', stream.GetUInt24(pValue.data), 'value_type', pValue.type, 'value_name', 'Энергия за период, A+') || stream.GetDataTypeInfo('energy_period_active');
  WHEN 74 THEN
    RETURN jsonb_build_object('value', stream.GetUInt24(pValue.data), 'value_type', pValue.type, 'value_name', 'Энергия за период, A-') || stream.GetDataTypeInfo('energy_period_active');
  WHEN 75 THEN
    RETURN jsonb_build_object('value', stream.GetUInt24(pValue.data), 'value_type', pValue.type, 'value_name', 'Энергия за период, R+') || stream.GetDataTypeInfo('energy_period_reactive');
  WHEN 76 THEN
    RETURN jsonb_build_object('value', stream.GetUInt24(pValue.data), 'value_type', pValue.type, 'value_name', 'Энергия за период, R-') || stream.GetDataTypeInfo('energy_period_reactive');
  WHEN 77 THEN
    RETURN jsonb_build_object('value', stream.GetUInt24(pValue.data), 'value_type', pValue.type, 'value_name', 'Энергия за период, S+') || stream.GetDataTypeInfo('energy_period_apparent');
  WHEN 78 THEN
    RETURN jsonb_build_object('value', stream.GetUInt24(pValue.data), 'value_type', pValue.type, 'value_name', 'Энергия за период, S-') || stream.GetDataTypeInfo('energy_period_apparent');
  WHEN 79 THEN
    RETURN jsonb_build_object('value', stream.GetUInt32(pValue.data), 'value_type', pValue.type, 'value_name', 'Мощность, A+') || stream.GetDataTypeInfo('power_active');
  WHEN 80 THEN
    RETURN jsonb_build_object('value', stream.GetUInt32(pValue.data), 'value_type', pValue.type, 'value_name', 'Мощность, A-') || stream.GetDataTypeInfo('power_active');
  WHEN 81 THEN
    RETURN jsonb_build_object('value', stream.GetUInt32(pValue.data), 'value_type', pValue.type, 'value_name', 'Мощность, A, Q1') || stream.GetDataTypeInfo('power_active');
  WHEN 82 THEN
    RETURN jsonb_build_object('value', stream.GetUInt32(pValue.data), 'value_type', pValue.type, 'value_name', 'Мощность, A, Q2') || stream.GetDataTypeInfo('power_active');
  WHEN 83 THEN
    RETURN jsonb_build_object('value', stream.GetUInt32(pValue.data), 'value_type', pValue.type, 'value_name', 'Мощность, A, Q3') || stream.GetDataTypeInfo('power_active');
  WHEN 84 THEN
    RETURN jsonb_build_object('value', stream.GetUInt32(pValue.data), 'value_type', pValue.type, 'value_name', 'Мощность, A, Q4') || stream.GetDataTypeInfo('power_active');
  WHEN 85 THEN
    RETURN jsonb_build_object('value', stream.GetUInt32(pValue.data), 'value_type', pValue.type, 'value_name', 'Мощность, A+, фаза 1') || stream.GetDataTypeInfo('power_active');
  WHEN 86 THEN
    RETURN jsonb_build_object('value', stream.GetUInt32(pValue.data), 'value_type', pValue.type, 'value_name', 'Мощность, A-, фаза 1') || stream.GetDataTypeInfo('power_active');
  WHEN 87 THEN
    RETURN jsonb_build_object('value', stream.GetUInt32(pValue.data), 'value_type', pValue.type, 'value_name', 'Мощность, A, Q1, фаза 1') || stream.GetDataTypeInfo('power_active');
  WHEN 88 THEN
    RETURN jsonb_build_object('value', stream.GetUInt32(pValue.data), 'value_type', pValue.type, 'value_name', 'Мощность, A, Q2, фаза 1') || stream.GetDataTypeInfo('power_active');
  WHEN 89 THEN
    RETURN jsonb_build_object('value', stream.GetUInt32(pValue.data), 'value_type', pValue.type, 'value_name', 'Мощность, A, Q3, фаза 1') || stream.GetDataTypeInfo('power_active');
  WHEN 90 THEN
    RETURN jsonb_build_object('value', stream.GetUInt32(pValue.data), 'value_type', pValue.type, 'value_name', 'Мощность, A, Q4, фаза 1') || stream.GetDataTypeInfo('power_active');
  WHEN 91 THEN
    RETURN jsonb_build_object('value', stream.GetUInt32(pValue.data), 'value_type', pValue.type, 'value_name', 'Мощность, A+, фаза 2') || stream.GetDataTypeInfo('power_active');
  WHEN 92 THEN
    RETURN jsonb_build_object('value', stream.GetUInt32(pValue.data), 'value_type', pValue.type, 'value_name', 'Мощность, A-, фаза 2') || stream.GetDataTypeInfo('power_active');
  WHEN 93 THEN
    RETURN jsonb_build_object('value', stream.GetUInt32(pValue.data), 'value_type', pValue.type, 'value_name', 'Мощность, A, Q1, фаза 2') || stream.GetDataTypeInfo('power_active');
  WHEN 94 THEN
    RETURN jsonb_build_object('value', stream.GetUInt32(pValue.data), 'value_type', pValue.type, 'value_name', 'Мощность, A, Q2, фаза 2') || stream.GetDataTypeInfo('power_active');
  WHEN 95 THEN
    RETURN jsonb_build_object('value', stream.GetUInt32(pValue.data), 'value_type', pValue.type, 'value_name', 'Мощность, A, Q3, фаза 2') || stream.GetDataTypeInfo('power_active');
  WHEN 96 THEN
    RETURN jsonb_build_object('value', stream.GetUInt32(pValue.data), 'value_type', pValue.type, 'value_name', 'Мощность, A, Q4, фаза 2') || stream.GetDataTypeInfo('power_active');
  WHEN 97 THEN
    RETURN jsonb_build_object('value', stream.GetUInt32(pValue.data), 'value_type', pValue.type, 'value_name', 'Мощность, A+, фаза 3') || stream.GetDataTypeInfo('power_active');
  WHEN 98 THEN
    RETURN jsonb_build_object('value', stream.GetUInt32(pValue.data), 'value_type', pValue.type, 'value_name', 'Мощность, A-, фаза 3') || stream.GetDataTypeInfo('power_active');
  WHEN 99 THEN
    RETURN jsonb_build_object('value', stream.GetUInt32(pValue.data), 'value_type', pValue.type, 'value_name', 'Мощность, A, Q1, фаза 3') || stream.GetDataTypeInfo('power_active');
  WHEN 100 THEN
    RETURN jsonb_build_object('value', stream.GetUInt32(pValue.data), 'value_type', pValue.type, 'value_name', 'Мощность, A, Q2, фаза 3') || stream.GetDataTypeInfo('power_active');
  WHEN 101 THEN
    RETURN jsonb_build_object('value', stream.GetUInt32(pValue.data), 'value_type', pValue.type, 'value_name', 'Мощность, A, Q3, фаза 3') || stream.GetDataTypeInfo('power_active');
  WHEN 102 THEN
    RETURN jsonb_build_object('value', stream.GetUInt32(pValue.data), 'value_type', pValue.type, 'value_name', 'Мощность, A, Q4, фаза 3') || stream.GetDataTypeInfo('power_active');
  WHEN 103 THEN
    RETURN jsonb_build_object('value', stream.GetUInt32(pValue.data), 'value_type', pValue.type, 'value_name', 'Мощность, R+') || stream.GetDataTypeInfo('power_reactive');
  WHEN 104 THEN
    RETURN jsonb_build_object('value', stream.GetUInt32(pValue.data), 'value_type', pValue.type, 'value_name', 'Мощность, R-') || stream.GetDataTypeInfo('power_reactive');
  WHEN 105 THEN
    RETURN jsonb_build_object('value', stream.GetUInt32(pValue.data), 'value_type', pValue.type, 'value_name', 'Мощность, R, Q1') || stream.GetDataTypeInfo('power_reactive');
  WHEN 106 THEN
    RETURN jsonb_build_object('value', stream.GetUInt32(pValue.data), 'value_type', pValue.type, 'value_name', 'Мощность, R, Q2') || stream.GetDataTypeInfo('power_reactive');
  WHEN 107 THEN
    RETURN jsonb_build_object('value', stream.GetUInt32(pValue.data), 'value_type', pValue.type, 'value_name', 'Мощность, R, Q3') || stream.GetDataTypeInfo('power_reactive');
  WHEN 108 THEN
    RETURN jsonb_build_object('value', stream.GetUInt32(pValue.data), 'value_type', pValue.type, 'value_name', 'Мощность, R, Q4') || stream.GetDataTypeInfo('power_reactive');
  WHEN 109 THEN
    RETURN jsonb_build_object('value', stream.GetUInt32(pValue.data), 'value_type', pValue.type, 'value_name', 'Мощность, R+, фаза 1') || stream.GetDataTypeInfo('power_reactive');
  WHEN 110 THEN
    RETURN jsonb_build_object('value', stream.GetUInt32(pValue.data), 'value_type', pValue.type, 'value_name', 'Мощность, R-, фаза 1') || stream.GetDataTypeInfo('power_reactive');
  WHEN 111 THEN
    RETURN jsonb_build_object('value', stream.GetUInt32(pValue.data), 'value_type', pValue.type, 'value_name', 'Мощность, R, Q1, фаза 1') || stream.GetDataTypeInfo('power_reactive');
  WHEN 112 THEN
    RETURN jsonb_build_object('value', stream.GetUInt32(pValue.data), 'value_type', pValue.type, 'value_name', 'Мощность, R, Q2, фаза 1') || stream.GetDataTypeInfo('power_reactive');
  WHEN 113 THEN
    RETURN jsonb_build_object('value', stream.GetUInt32(pValue.data), 'value_type', pValue.type, 'value_name', 'Мощность, R, Q3, фаза 1') || stream.GetDataTypeInfo('power_reactive');
  WHEN 114 THEN
    RETURN jsonb_build_object('value', stream.GetUInt32(pValue.data), 'value_type', pValue.type, 'value_name', 'Мощность, R, Q4, фаза 1') || stream.GetDataTypeInfo('power_reactive');
  WHEN 115 THEN
    RETURN jsonb_build_object('value', stream.GetUInt32(pValue.data), 'value_type', pValue.type, 'value_name', 'Мощность, R+, фаза 2') || stream.GetDataTypeInfo('power_reactive');
  WHEN 116 THEN
    RETURN jsonb_build_object('value', stream.GetUInt32(pValue.data), 'value_type', pValue.type, 'value_name', 'Мощность, R-, фаза 2') || stream.GetDataTypeInfo('power_reactive');
  WHEN 117 THEN
    RETURN jsonb_build_object('value', stream.GetUInt32(pValue.data), 'value_type', pValue.type, 'value_name', 'Мощность, R, Q1, фаза 2') || stream.GetDataTypeInfo('power_reactive');
  WHEN 118 THEN
    RETURN jsonb_build_object('value', stream.GetUInt32(pValue.data), 'value_type', pValue.type, 'value_name', 'Мощность, R, Q2, фаза 2') || stream.GetDataTypeInfo('power_reactive');
  WHEN 119 THEN
    RETURN jsonb_build_object('value', stream.GetUInt32(pValue.data), 'value_type', pValue.type, 'value_name', 'Мощность, R, Q3, фаза 2') || stream.GetDataTypeInfo('power_reactive');
  WHEN 120 THEN
    RETURN jsonb_build_object('value', stream.GetUInt32(pValue.data), 'value_type', pValue.type, 'value_name', 'Мощность, R, Q4, фаза 2') || stream.GetDataTypeInfo('power_reactive');
  WHEN 121 THEN
    RETURN jsonb_build_object('value', stream.GetUInt32(pValue.data), 'value_type', pValue.type, 'value_name', 'Мощность, R+, фаза 3') || stream.GetDataTypeInfo('power_reactive');
  WHEN 122 THEN
    RETURN jsonb_build_object('value', stream.GetUInt32(pValue.data), 'value_type', pValue.type, 'value_name', 'Мощность, R-, фаза 3') || stream.GetDataTypeInfo('power_reactive');
  WHEN 123 THEN
    RETURN jsonb_build_object('value', stream.GetUInt32(pValue.data), 'value_type', pValue.type, 'value_name', 'Мощность, R, Q1, фаза 3') || stream.GetDataTypeInfo('power_reactive');
  WHEN 124 THEN
    RETURN jsonb_build_object('value', stream.GetUInt32(pValue.data), 'value_type', pValue.type, 'value_name', 'Мощность, R, Q2, фаза 3') || stream.GetDataTypeInfo('power_reactive');
  WHEN 125 THEN
    RETURN jsonb_build_object('value', stream.GetUInt32(pValue.data), 'value_type', pValue.type, 'value_name', 'Мощность, R, Q3, фаза 3') || stream.GetDataTypeInfo('power_reactive');
  WHEN 126 THEN
    RETURN jsonb_build_object('value', stream.GetUInt32(pValue.data), 'value_type', pValue.type, 'value_name', 'Мощность, R, Q4, фаза 3') || stream.GetDataTypeInfo('power_reactive');
  WHEN 127 THEN
    RETURN jsonb_build_object('value', stream.GetUInt32(pValue.data), 'value_type', pValue.type, 'value_name', 'Мощность, S+') || stream.GetDataTypeInfo('power_apparent');
  WHEN 128 THEN
    RETURN jsonb_build_object('value', stream.GetUInt32(pValue.data), 'value_type', pValue.type, 'value_name', 'Мощность, S-') || stream.GetDataTypeInfo('power_apparent');
  WHEN 129 THEN
    RETURN jsonb_build_object('value', stream.GetUInt32(pValue.data), 'value_type', pValue.type, 'value_name', 'Мощность, S, Q1') || stream.GetDataTypeInfo('power_apparent');
  WHEN 130 THEN
    RETURN jsonb_build_object('value', stream.GetUInt32(pValue.data), 'value_type', pValue.type, 'value_name', 'Мощность, S, Q2') || stream.GetDataTypeInfo('power_apparent');
  WHEN 131 THEN
    RETURN jsonb_build_object('value', stream.GetUInt32(pValue.data), 'value_type', pValue.type, 'value_name', 'Мощность, S, Q3') || stream.GetDataTypeInfo('power_apparent');
  WHEN 132 THEN
    RETURN jsonb_build_object('value', stream.GetUInt32(pValue.data), 'value_type', pValue.type, 'value_name', 'Мощность, S, Q4') || stream.GetDataTypeInfo('power_apparent');
  WHEN 133 THEN
    RETURN jsonb_build_object('value', stream.GetUInt32(pValue.data), 'value_type', pValue.type, 'value_name', 'Мощность, S+, фаза 1') || stream.GetDataTypeInfo('power_apparent');
  WHEN 134 THEN
    RETURN jsonb_build_object('value', stream.GetUInt32(pValue.data), 'value_type', pValue.type, 'value_name', 'Мощность, S-, фаза 1') || stream.GetDataTypeInfo('power_apparent');
  WHEN 135 THEN
    RETURN jsonb_build_object('value', stream.GetUInt32(pValue.data), 'value_type', pValue.type, 'value_name', 'Мощность, S, Q1, фаза 1') || stream.GetDataTypeInfo('power_apparent');
  WHEN 136 THEN
    RETURN jsonb_build_object('value', stream.GetUInt32(pValue.data), 'value_type', pValue.type, 'value_name', 'Мощность, S, Q2, фаза 1') || stream.GetDataTypeInfo('power_apparent');
  WHEN 137 THEN
    RETURN jsonb_build_object('value', stream.GetUInt32(pValue.data), 'value_type', pValue.type, 'value_name', 'Мощность, S, Q3, фаза 1') || stream.GetDataTypeInfo('power_apparent');
  WHEN 138 THEN
    RETURN jsonb_build_object('value', stream.GetUInt32(pValue.data), 'value_type', pValue.type, 'value_name', 'Мощность, S, Q4, фаза 1') || stream.GetDataTypeInfo('power_apparent');
  WHEN 139 THEN
    RETURN jsonb_build_object('value', stream.GetUInt32(pValue.data), 'value_type', pValue.type, 'value_name', 'Мощность, S+, фаза 2') || stream.GetDataTypeInfo('power_apparent');
  WHEN 140 THEN
    RETURN jsonb_build_object('value', stream.GetUInt32(pValue.data), 'value_type', pValue.type, 'value_name', 'Мощность, S-, фаза 2') || stream.GetDataTypeInfo('power_apparent');
  WHEN 141 THEN
    RETURN jsonb_build_object('value', stream.GetUInt32(pValue.data), 'value_type', pValue.type, 'value_name', 'Мощность, S, Q1, фаза 2') || stream.GetDataTypeInfo('power_apparent');
  WHEN 142 THEN
    RETURN jsonb_build_object('value', stream.GetUInt32(pValue.data), 'value_type', pValue.type, 'value_name', 'Мощность, S, Q2, фаза 2') || stream.GetDataTypeInfo('power_apparent');
  WHEN 143 THEN
    RETURN jsonb_build_object('value', stream.GetUInt32(pValue.data), 'value_type', pValue.type, 'value_name', 'Мощность, S, Q3, фаза 2') || stream.GetDataTypeInfo('power_apparent');
  WHEN 144 THEN
    RETURN jsonb_build_object('value', stream.GetUInt32(pValue.data), 'value_type', pValue.type, 'value_name', 'Мощность, S, Q4, фаза 2') || stream.GetDataTypeInfo('power_apparent');
  WHEN 145 THEN
    RETURN jsonb_build_object('value', stream.GetUInt32(pValue.data), 'value_type', pValue.type, 'value_name', 'Мощность, S+, фаза 3') || stream.GetDataTypeInfo('power_apparent');
  WHEN 146 THEN
    RETURN jsonb_build_object('value', stream.GetUInt32(pValue.data), 'value_type', pValue.type, 'value_name', 'Мощность, S-, фаза 3') || stream.GetDataTypeInfo('power_apparent');
  WHEN 147 THEN
    RETURN jsonb_build_object('value', stream.GetUInt32(pValue.data), 'value_type', pValue.type, 'value_name', 'Мощность, S, Q1, фаза 3') || stream.GetDataTypeInfo('power_apparent');
  WHEN 148 THEN
    RETURN jsonb_build_object('value', stream.GetUInt32(pValue.data), 'value_type', pValue.type, 'value_name', 'Мощность, S, Q2, фаза 3') || stream.GetDataTypeInfo('power_apparent');
  WHEN 149 THEN
    RETURN jsonb_build_object('value', stream.GetUInt32(pValue.data), 'value_type', pValue.type, 'value_name', 'Мощность, S, Q3, фаза 3') || stream.GetDataTypeInfo('power_apparent');
  WHEN 150 THEN
    RETURN jsonb_build_object('value', stream.GetUInt32(pValue.data), 'value_type', pValue.type, 'value_name', 'Мощность, S, Q4, фаза 3') || stream.GetDataTypeInfo('power_apparent');
  WHEN 151 THEN
    RETURN jsonb_build_object('value', stream.GetUInt32(pValue.data), 'value_type', pValue.type, 'value_name', 'Мощность за период, средняя, A+') || stream.GetDataTypeInfo('power_period_active');
  WHEN 152 THEN
    RETURN jsonb_build_object('value', stream.GetUInt32(pValue.data), 'value_type', pValue.type, 'value_name', 'Мощность за период, средняя, A-') || stream.GetDataTypeInfo('power_period_active');
  WHEN 153 THEN
    RETURN jsonb_build_object('value', stream.GetUInt32(pValue.data), 'value_type', pValue.type, 'value_name', 'Мощность за период, средняя, R+') || stream.GetDataTypeInfo('power_period_reactive');
  WHEN 154 THEN
    RETURN jsonb_build_object('value', stream.GetUInt32(pValue.data), 'value_type', pValue.type, 'value_name', 'Мощность за период, средняя, R-') || stream.GetDataTypeInfo('power_period_reactive');
  WHEN 155 THEN
    RETURN jsonb_build_object('value', stream.GetUInt32(pValue.data), 'value_type', pValue.type, 'value_name', 'Мощность за период, средняя, S+') || stream.GetDataTypeInfo('power_period_apparent');
  WHEN 156 THEN
    RETURN jsonb_build_object('value', stream.GetUInt32(pValue.data), 'value_type', pValue.type, 'value_name', 'Мощность за период, средняя, S-') || stream.GetDataTypeInfo('power_period_apparent');
  WHEN 157 THEN
    RETURN jsonb_build_object('value', stream.GetUInt32(pValue.data), 'value_type', pValue.type, 'value_name', 'Мощность за период, максимальная, A+') || stream.GetDataTypeInfo('power_period_active');
  WHEN 158 THEN
    RETURN jsonb_build_object('value', stream.GetUInt32(pValue.data), 'value_type', pValue.type, 'value_name', 'Мощность за период, максимальная, A-') || stream.GetDataTypeInfo('power_period_active');
  WHEN 159 THEN
    RETURN jsonb_build_object('value', stream.GetUInt32(pValue.data), 'value_type', pValue.type, 'value_name', 'Мощность за период, максимальная, R+') || stream.GetDataTypeInfo('power_period_reactive');
  WHEN 160 THEN
    RETURN jsonb_build_object('value', stream.GetUInt32(pValue.data), 'value_type', pValue.type, 'value_name', 'Мощность за период, максимальная, R-') || stream.GetDataTypeInfo('power_period_reactive');
  WHEN 161 THEN
    RETURN jsonb_build_object('value', stream.GetUInt32(pValue.data), 'value_type', pValue.type, 'value_name', 'Мощность за период, максимальная, S+') || stream.GetDataTypeInfo('power_period_apparent');
  WHEN 162 THEN
    RETURN jsonb_build_object('value', stream.GetUInt32(pValue.data), 'value_type', pValue.type, 'value_name', 'Мощность за период, максимальная, S-') || stream.GetDataTypeInfo('power_period_apparent');
  WHEN 163 THEN
    RETURN jsonb_build_object('value', round(stream.GetUInt24(pValue.data) / 1000, 4), 'value_type', pValue.type, 'value_name', 'Ток, I, фаза 1') || stream.GetDataTypeInfo('curr');
  WHEN 164 THEN
    RETURN jsonb_build_object('value', round(stream.GetUInt24(pValue.data) / 1000, 4), 'value_type', pValue.type, 'value_name', 'Ток, I, фаза 2') || stream.GetDataTypeInfo('curr');
  WHEN 165 THEN
    RETURN jsonb_build_object('value', round(stream.GetUInt24(pValue.data) / 1000, 4), 'value_type', pValue.type, 'value_name', 'Ток, I, фаза 3') || stream.GetDataTypeInfo('curr');
  WHEN 166 THEN
    RETURN jsonb_build_object('value', round(stream.GetUInt24(pValue.data) / 1000, 4), 'value_type', pValue.type, 'value_name', 'Ток, I, ноль') || stream.GetDataTypeInfo('curr');
  WHEN 167 THEN
    RETURN jsonb_build_object('value', round(stream.GetUInt24(pValue.data) / 100, 3), 'value_type', pValue.type, 'value_name', 'Напряжение, U, фаза 1') || stream.GetDataTypeInfo('volt');
  WHEN 168 THEN
    RETURN jsonb_build_object('value', round(stream.GetUInt24(pValue.data) / 100, 3), 'value_type', pValue.type, 'value_name', 'Напряжение, U, фаза 2') || stream.GetDataTypeInfo('volt');
  WHEN 169 THEN
    RETURN jsonb_build_object('value', round(stream.GetUInt24(pValue.data) / 100, 3), 'value_type', pValue.type, 'value_name', 'Напряжение, U, фаза 3') || stream.GetDataTypeInfo('volt');
  WHEN 170 THEN
    RETURN jsonb_build_object('value', round(stream.GetUInt24(pValue.data) / 100, 3), 'value_type', pValue.type, 'value_name', 'Напряжение, U, ноль') || stream.GetDataTypeInfo('volt');
  WHEN 171 THEN
    RETURN jsonb_build_object('value', round(stream.GetUInt16(pValue.data) / 1000, 4), 'value_type', pValue.type, 'value_name', 'Коэффициент мощности, cosФ') || stream.GetDataTypeInfo('cosf');
  WHEN 172 THEN
    RETURN jsonb_build_object('value', round(stream.GetUInt16(pValue.data) / 1000, 4), 'value_type', pValue.type, 'value_name', 'Коэффициент мощности, cosФ, фаза 1') || stream.GetDataTypeInfo('cosf');
  WHEN 173 THEN
    RETURN jsonb_build_object('value', round(stream.GetUInt16(pValue.data) / 1000, 4), 'value_type', pValue.type, 'value_name', 'Коэффициент мощности, cosФ, фаза 2') || stream.GetDataTypeInfo('cosf');
  WHEN 174 THEN
    RETURN jsonb_build_object('value', round(stream.GetUInt16(pValue.data) / 1000, 4), 'value_type', pValue.type, 'value_name', 'Коэффициент мощности, cosФ, фаза 3') || stream.GetDataTypeInfo('cosf');
  WHEN 175 THEN
    RETURN jsonb_build_object('value', round(stream.GetUInt24(pValue.data) / 100, 3), 'value_type', pValue.type, 'value_name', 'Напряжение, U, межфазное 1-2') || stream.GetDataTypeInfo('volt');
  WHEN 176 THEN
    RETURN jsonb_build_object('value', round(stream.GetUInt24(pValue.data) / 100, 3), 'value_type', pValue.type, 'value_name', 'Напряжение, U, межфазное 2-3') || stream.GetDataTypeInfo('volt');
  WHEN 177 THEN
    RETURN jsonb_build_object('value', round(stream.GetUInt24(pValue.data) / 100, 3), 'value_type', pValue.type, 'value_name', 'Напряжение, U, межфазное 1-3') || stream.GetDataTypeInfo('volt');
  WHEN 178 THEN
    RETURN jsonb_build_object('value', round(stream.GetUInt16(pValue.data) / 100, 3), 'value_type', pValue.type, 'value_name', 'Частота, F') || stream.GetDataTypeInfo('freq');
  WHEN 179 THEN
    RETURN jsonb_build_object('value', stream.GetUInt8(pValue.data), 'value_type', pValue.type, 'value_name', 'Температура, T') || stream.GetDataTypeInfo('temper');
  WHEN 180 THEN
    RETURN jsonb_build_object('value', stream.GetUInt16(pValue.data), 'value_type', pValue.type, 'value_name', 'Угол, I фаза 1 – U фаза 1') || stream.GetDataTypeInfo('degree');
  WHEN 181 THEN
    RETURN jsonb_build_object('value', stream.GetUInt16(pValue.data), 'value_type', pValue.type, 'value_name', 'Угол, I фаза 2 – U фаза 2') || stream.GetDataTypeInfo('degree');
  WHEN 182 THEN
    RETURN jsonb_build_object('value', stream.GetUInt16(pValue.data), 'value_type', pValue.type, 'value_name', 'Угол, I фаза 3 – U фаза 3') || stream.GetDataTypeInfo('degree');
  WHEN 183 THEN
    RETURN jsonb_build_object('value', stream.GetUInt16(pValue.data), 'value_type', pValue.type, 'value_name', 'Угол, I фаза 1 – I фаза 2') || stream.GetDataTypeInfo('degree');
  WHEN 184 THEN
    RETURN jsonb_build_object('value', stream.GetUInt16(pValue.data), 'value_type', pValue.type, 'value_name', 'Угол, I фаза 2 – I фаза 3') || stream.GetDataTypeInfo('degree');
  WHEN 185 THEN
    RETURN jsonb_build_object('value', stream.GetUInt16(pValue.data), 'value_type', pValue.type, 'value_name', 'Угол, I фаза 1 – I фаза 3') || stream.GetDataTypeInfo('degree');
  WHEN 186 THEN
    RETURN jsonb_build_object('value', stream.GetUInt16(pValue.data), 'value_type', pValue.type, 'value_name', 'Угол, U фаза 1 – U фаза 2') || stream.GetDataTypeInfo('degree');
  WHEN 187 THEN
    RETURN jsonb_build_object('value', stream.GetUInt16(pValue.data), 'value_type', pValue.type, 'value_name', 'Угол, U фаза 2 – U фаза 3') || stream.GetDataTypeInfo('degree');
  WHEN 188 THEN
    RETURN jsonb_build_object('value', stream.GetUInt16(pValue.data), 'value_type', pValue.type, 'value_name', 'Угол, U фаза 1 – U фаза 3') || stream.GetDataTypeInfo('degree');
  WHEN 189 THEN
    RETURN jsonb_build_object('value', stream.GetUInt16(pValue.data), 'value_type', pValue.type, 'value_name', 'Угол, I, сдвиг фазы, фаза 1') || stream.GetDataTypeInfo('degree');
  WHEN 190 THEN
    RETURN jsonb_build_object('value', stream.GetUInt16(pValue.data), 'value_type', pValue.type, 'value_name', 'Угол, I, сдвиг фазы, фаза 2') || stream.GetDataTypeInfo('degree');
  WHEN 191 THEN
    RETURN jsonb_build_object('value', stream.GetUInt16(pValue.data), 'value_type', pValue.type, 'value_name', 'Угол, I, сдвиг фазы, фаза 3') || stream.GetDataTypeInfo('degree');
  WHEN 192 THEN
    RETURN jsonb_build_object('value', stream.GetUInt16(pValue.data), 'value_type', pValue.type, 'value_name', 'Угол, U, сдвиг фазы, фаза 1') || stream.GetDataTypeInfo('degree');
  WHEN 193 THEN
    RETURN jsonb_build_object('value', stream.GetUInt16(pValue.data), 'value_type', pValue.type, 'value_name', 'Угол, U, сдвиг фазы, фаза 2') || stream.GetDataTypeInfo('degree');
  WHEN 194 THEN
    RETURN jsonb_build_object('value', stream.GetUInt16(pValue.data), 'value_type', pValue.type, 'value_name', 'Угол, U, сдвиг фазы, фаза 3') || stream.GetDataTypeInfo('degree');
  WHEN 195 THEN
    RETURN jsonb_build_object('value', round(stream.GetUInt24(pValue.data) / 1000, 4), 'value_type', pValue.type, 'value_name', 'Ток, I, дифференциальный, фаза 1') || stream.GetDataTypeInfo('curr');
  WHEN 196 THEN
    RETURN jsonb_build_object('value', round(stream.GetUInt24(pValue.data) / 1000, 4), 'value_type', pValue.type, 'value_name', 'Ток, I, дифференциальный, фаза 2') || stream.GetDataTypeInfo('curr');
  WHEN 197 THEN
    RETURN jsonb_build_object('value', round(stream.GetUInt24(pValue.data) / 1000, 4), 'value_type', pValue.type, 'value_name', 'Ток, I, дифференциальный, фаза 3') || stream.GetDataTypeInfo('curr');
  WHEN 198 THEN
    RETURN jsonb_build_object('value', round(stream.GetUInt16(pValue.data) / 100, 3), 'value_type', pValue.type, 'value_name', 'Искажение напряжения, фаза 1') || stream.GetDataTypeInfo('percent');
  WHEN 199 THEN
    RETURN jsonb_build_object('value', round(stream.GetUInt16(pValue.data) / 100, 3), 'value_type', pValue.type, 'value_name', 'Искажение напряжения, фаза 2') || stream.GetDataTypeInfo('percent');
  WHEN 200 THEN
    RETURN jsonb_build_object('value', round(stream.GetUInt16(pValue.data) / 100, 3), 'value_type', pValue.type, 'value_name', 'Искажение напряжения, фаза 3') || stream.GetDataTypeInfo('percent');
  WHEN 201 THEN
    RETURN jsonb_build_object('value', stream.GetUInt48(pValue.data), 'value_type', pValue.type, 'value_name', 'Удельная энергия потерь в цепях тока') || stream.GetDataTypeInfo('loss_line');
  WHEN 202 THEN
    RETURN jsonb_build_object('value', stream.GetUInt48(pValue.data), 'value_type', pValue.type, 'value_name', 'Удельная энергия потерь в силовых трансформаторах') || stream.GetDataTypeInfo('loss_transform');
  WHEN 203 THEN
    RETURN jsonb_build_object('value', stream.GetUInt8(pValue.data), 'value_type', pValue.type, 'value_name', 'Состояние нагрузки') || stream.GetDataTypeInfo('load_state');
  WHEN 204 THEN
    RETURN jsonb_build_object('value', stream.GetUInt16(pValue.data), 'value_type', pValue.type, 'value_name', 'Состояние счетчика') || stream.GetDataTypeInfo('meter_state');
  WHEN 205 THEN
    RETURN jsonb_build_object('value', stream.GetUInt32(pValue.data), 'value_type', pValue.type, 'value_name', 'Лимит мощности, A+') || stream.GetDataTypeInfo('power_active');
  WHEN 206 THEN
    RETURN jsonb_build_object('value', stream.GetUInt16(pValue.data), 'value_type', pValue.type, 'value_name', 'Продолжительность превышения лимита мощности, A+') || stream.GetDataTypeInfo('second');
  WHEN 207 THEN
    RETURN jsonb_build_object('value', stream.GetUInt32(pValue.data), 'value_type', pValue.type, 'value_name', 'Лимит мощности, A-') || stream.GetDataTypeInfo('power_active');
  WHEN 208 THEN
    RETURN jsonb_build_object('value', stream.GetUInt16(pValue.data), 'value_type', pValue.type, 'value_name', 'Продолжительность превышения лимита мощности, A-') || stream.GetDataTypeInfo('second');
  WHEN 209 THEN
    RETURN jsonb_build_object('value', stream.GetUInt32(pValue.data), 'value_type', pValue.type, 'value_name', 'Лимит мощности, R+') || stream.GetDataTypeInfo('power_active');
  WHEN 210 THEN
    RETURN jsonb_build_object('value', stream.GetUInt16(pValue.data), 'value_type', pValue.type, 'value_name', 'Продолжительность превышения лимита мощности, R+') || stream.GetDataTypeInfo('second');
  WHEN 211 THEN
    RETURN jsonb_build_object('value', stream.GetUInt32(pValue.data), 'value_type', pValue.type, 'value_name', 'Лимит мощности, R-') || stream.GetDataTypeInfo('power_active');
  WHEN 212 THEN
    RETURN jsonb_build_object('value', stream.GetUInt16(pValue.data), 'value_type', pValue.type, 'value_name', 'Продолжительность превышения лимита мощности, R-') || stream.GetDataTypeInfo('second');
  WHEN 213 THEN
    RETURN jsonb_build_object('value', stream.GetUInt8(pValue.data), 'value_type', pValue.type, 'value_name', 'Период переключения параметров индикации') || stream.GetDataTypeInfo('second');
  WHEN 214 THEN
    RETURN jsonb_build_object('value', stream.GetUInt32(pValue.data), 'value_type', pValue.type, 'value_name', 'Суточный трафик') || stream.GetDataTypeInfo('byte');
  WHEN 215 THEN
    RETURN jsonb_build_object('value', to_timestamp(stream.GetUInt32(pValue.data)), 'value_type', pValue.type, 'value_name', 'Дата') || stream.GetDataTypeInfo('time');
  WHEN 216 THEN
    RETURN jsonb_build_object('value', stream.GetUInt24(pValue.data), 'value_type', pValue.type, 'value_name', 'Время') || stream.GetDataTypeInfo('time_day');
  WHEN 217 THEN
    RETURN jsonb_build_object('value', stream.GetUInt8(pValue.data), 'value_type', pValue.type, 'value_name', 'Номер активного канала передачи данных');
  WHEN 218 THEN
    RETURN jsonb_build_object('value', null, 'value_type', pValue.type, 'value_name', 'Резерв');
  ELSE
    RETURN jsonb_build_object();
  END CASE;
END
$$ LANGUAGE plpgsql
   SECURITY DEFINER
   SET search_path = kernel, pg_temp;

--------------------------------------------------------------------------------
-- stream.ParseLPWANCommandData ------------------------------------------------
--------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION stream.ParseLPWANCommandData (
  pId           numeric,
  pCommand      int2,
  pPackage      int2
) RETURNS       void
AS $$
DECLARE
  pak           stream.lpwan_package%rowtype;
  cmd           stream.lpwan_command%rowtype;

  nDevice       numeric;
  nModel        numeric;

  vIdentity     text;

  cmd04         stream.TLPWAN_CMD_CURRENT_VALUE;

  pos           int DEFAULT 0;
  count         int2;
BEGIN
  SELECT * INTO cmd
    FROM stream.lpwan_command
   WHERE package = pId
     AND n_command = pCommand
     AND n_package = pPackage;

  IF not found THEN
    RETURN;
  END IF;

  IF cmd.code <> 0 THEN
    RETURN;
  END IF;

  SELECT * INTO pak
    FROM stream.lpwan_package
   WHERE id = cmd.package;

  SELECT id INTO nDevice FROM db.device WHERE serial = pak.serial;

  IF not found THEN
    CASE pak.type
    WHEN 1 THEN
      nModel := GetModel('mercury_200');
      vIdentity := 'M200-' || pak.serial;
    WHEN 2 THEN
      nModel := GetModel('mercury_201');
      vIdentity := 'M201-' || pak.serial;
    WHEN 3 THEN
      nModel := GetModel('mercury_202');
      vIdentity := 'M202-' || pak.serial;
    WHEN 4 THEN
      nModel := GetModel('mercury_203');
      vIdentity := 'M203-' || pak.serial;
    WHEN 5 THEN
      nModel := GetModel('mercury_206');
      vIdentity := 'M206-' || pak.serial;
    WHEN 6 THEN
      nModel := GetModel('mercury_208');
      vIdentity := 'M208-' || pak.serial;
    WHEN 7 THEN
      nModel := GetModel('mercury_230');
      vIdentity := 'M230-' || pak.serial;
    WHEN 8 THEN
      nModel := GetModel('mercury_231');
      vIdentity := 'M231-' || pak.serial;
    WHEN 9 THEN
      nModel := GetModel('mercury_234');
      vIdentity := 'M234-' || pak.serial;
    WHEN 10 THEN
      nModel := GetModel('mercury_236');
      vIdentity := 'M236-' || pak.serial;
    WHEN 11 THEN
      nModel := GetModel('mercury_238');
      vIdentity := 'M238-' || pak.serial;
    ELSE
      RETURN;
    END CASE;

    nDevice := CreateDevice(null, GetType('meter.device'), nModel, null, vIdentity, null, pak.serial);
  END IF;

  PERFORM AddStatusNotification(nDevice, 0, cmd.type::text, cmd.code::text, encode(cmd.data, 'hex'), pak.type::text, cmd.date);

  -- Команда 0x03. Архивы
  IF cmd.type = 3 THEN
    RETURN;
  END IF;

  -- Команда 0x04. Текущие значения
  IF cmd.type = 4 THEN
    -- Количество значений
    count := get_byte(cmd.data, 0);
    pos := pos + 1;
    -- Для каждого значения:
    FOR i IN 0..count - 1
    LOOP
      -- Тип значения
      cmd04.type := get_byte(cmd.data, pos);
      pos := pos + 1;
      -- Размер значения
      cmd04.size := get_byte(cmd.data, pos);
      pos := pos + 1;
      -- Текущее значение
      cmd04.data := substr(cmd.data, pos + 1, cmd04.size);
      pos := pos + cmd04.size;

      PERFORM AddMeterValue(nDevice, cmd04.type, null, stream.GetCurrentValue(cmd04)::json, cmd.date);
    END LOOP;

    RETURN;
  END IF;

END
$$ LANGUAGE plpgsql
   SECURITY DEFINER
   SET search_path = kernel, pg_temp;

--------------------------------------------------------------------------------
-- stream.ParseLPWANPackage ----------------------------------------------------
--------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION stream.ParseLPWANPackage (
  pData         bytea
) RETURNS       stream.TLPWAN_PACKAGE
AS $$
DECLARE
  Pos           int;
  Size          int;
  crc16         int;
  CRC           bytea;
  Result        stream.TLPWAN_PACKAGE;
BEGIN
  Pos := 0;
  Size := octet_length(pData);
  CRC := substr(pData, Size - 1);

  crc16 := stream.GetCRC16(pData, Size - 2);
  Result.crc16 := get_byte(CRC, 1) << 8 | get_byte(CRC, 0);

  IF (crc16 <> Result.crc16) THEN
    RAISE EXCEPTION 'Invalid CRC';
  END IF;

  Result.length := get_byte(pData, Pos);
  Pos := Pos + 1;

  -- length – длина данных (1 или 2 байта).
  -- 1 байт: 0-6 бит – младшие биты длины, 7 бит – длина данных 2 байта).
  -- 2 байт: присутствует если установлен 7 бит первого байта, 0-7 бит – старшие биты длины.
  IF Result.length & 128 = 128 THEN
    Result.length = set_bit(Result.length::bit(8), 0, 0)::int;
    Result.length = get_byte(pData, Pos) << 8 | Result.length;
    Pos := Pos + 1;
  END IF;

  Result.version := get_byte(pData, Pos);
  Pos := Pos + 1;

  Result.params := get_byte(pData, Pos)::bit(8);
  Pos := Pos + 1;

  Result.type := get_byte(pData, Pos);
  Pos := Pos + 1;

  Result.serial_size := get_byte(pData, Pos);
  Pos := Pos + 1;

  Result.serial := encode(substr(pData, Pos + 1, Result.serial_size), 'escape');
  Pos := Pos + Result.serial_size;

  Result.n_command := get_byte(pData, Pos);
  Pos := Pos + 1;

  Result.n_package := get_byte(pData, Pos);
  Pos := Pos + 1;

  Result.command := substr(pData, Pos + 1, Size - Pos - 2);

  RETURN Result;
END
$$ LANGUAGE plpgsql
   SECURITY DEFINER
   SET search_path = kernel, pg_temp;

--------------------------------------------------------------------------------
-- stream.ParseLPWANCommand ----------------------------------------------------
--------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION stream.ParseLPWANCommand (
  pData         bytea
) RETURNS       stream.TLPWAN_COMMAND
AS $$
DECLARE
  Pos           int;
  Size          int;
  temp          double precision;
  Result        stream.TLPWAN_COMMAND;
BEGIN
  Pos := 0;
  Size := octet_length(pData);

  temp := stream.GetUInt32(pData);
  Pos := Pos + 4;

  -- Текущее время счетчика. 0 – неопределенное время, 1 – ошибка времени
  IF temp > 1 THEN
    Result.date := to_timestamp(temp);
  END IF;

  Result.type := get_byte(pData, Pos);
  Pos := Pos + 1;

  Result.code := get_byte(pData, Pos);
  Pos := Pos + 1;

  -- Если есть ошибка, то поле “Данные” отсутствует
  IF Result.code = 0 THEN
    Result.data := substr(pData, Pos + 1, Size - Pos);
  END IF;

  RETURN Result;
END
$$ LANGUAGE plpgsql
   SECURITY DEFINER
   SET search_path = kernel, pg_temp;

--------------------------------------------------------------------------------
-- stream.ParseLPWAN -----------------------------------------------------------
--------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION stream.ParseLPWAN (
  pData         bytea
) RETURNS       bytea
AS $$
DECLARE
  nId           numeric;
  nCommand      int2;
  nPackage      int2;
  Data          bytea;
BEGIN
  INSERT INTO stream.lpwan_package SELECT NEXTVAL('SEQUENCE_STREAM_LPWAN'), Now(), p.* FROM stream.ParseLPWANPackage(pData) AS p
  RETURNING id, n_command, n_package, command INTO nId, nCommand, nPackage, Data;

  INSERT INTO stream.lpwan_command SELECT nId, nCommand, nPackage, c.* FROM stream.ParseLPWANCommand(Data) AS c;

  PERFORM stream.ParseLPWANCommandData(nId, nCommand, nPackage);

  RETURN null;
END
$$ LANGUAGE plpgsql
   SECURITY DEFINER
   SET search_path = kernel, pg_temp;

--------------------------------------------------------------------------------
-- stream.Parse ----------------------------------------------------------------
--------------------------------------------------------------------------------
/**
 * Разбор пакета.
 * @param {text} pProtocol - Протокол (формат данных)
 * @param {text} pIdentity - Идентификатор (host:port)
 * @param {text} pBase64 - Данные в формате BASE64
 * @return {text} - Ответ в формате BASE64
 */
CREATE OR REPLACE FUNCTION stream.Parse (
  pProtocol     text,
  pIdentity     text,
  pBase64       text
) RETURNS       text
AS $$
DECLARE
  tsBegin       timestamp;

  vSession      text;

  vMessage      text;
  vContext      text;

  bRequest      bytea;
  bResponse     bytea;
BEGIN
  vSession := stream.SetSession('admin', 'default');

  tsBegin := clock_timestamp();

  bRequest = decode(pBase64, 'base64');

  CASE pProtocol
  WHEN 'LPWAN' THEN

    bResponse := stream.ParseLPWAN(bRequest);

  ELSE
    PERFORM UnknownProtocol(pProtocol);
  END CASE;

  PERFORM stream.WriteTolog(pProtocol, coalesce(pIdentity, 'null'), bRequest, bResponse, age(clock_timestamp(), tsBegin));

  RETURN encode(bResponse, 'base64');
EXCEPTION
WHEN others THEN
  GET STACKED DIAGNOSTICS vMessage = MESSAGE_TEXT, vContext = PG_EXCEPTION_CONTEXT;

  RAISE NOTICE '%', vContext;

  PERFORM SetErrorMessage(vMessage);

  bRequest = decode(pBase64, 'base64');

  PERFORM stream.WriteTolog(pProtocol, coalesce(pIdentity, 'null'), bRequest, null, age(clock_timestamp(), tsBegin), vMessage);

  RETURN null;
END
$$ LANGUAGE plpgsql
   SECURITY DEFINER
   SET search_path = kernel, pg_temp;

--------------------------------------------------------------------------------
-- stream.SetSession -----------------------------------------------------------
--------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION stream.SetSession (
  pUserName     text,
  pArea         text
) RETURNS       text
AS $$
DECLARE
  vSession      text;
BEGIN
  vSession := GetSession(GetUser(pUserName));

  PERFORM SetArea(GetArea(pArea));

  RETURN vSession;
END
$$ LANGUAGE plpgsql
   SECURITY DEFINER
   SET search_path = kernel, pg_temp;
