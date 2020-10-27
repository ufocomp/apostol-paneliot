SELECT SignIn(CreateSystemOAuth2(), 'admin', 'admin');

SELECT GetErrorMessage();

SELECT SetDefaultArea(GetArea('default'));
SELECT SetArea(GetArea('default'));

SELECT RegSetValueEx(RegCreateKey('CURRENT_CONFIG', 'CONFIG\CurrentProject'), 'Name', 3, pString => 'Panel IoT');
SELECT RegSetValueEx(RegCreateKey('CURRENT_CONFIG', 'CONFIG\CurrentProject'), 'Host', 3, pString => 'http://paneliot.ru');
SELECT RegSetValueEx(RegCreateKey('CURRENT_CONFIG', 'CONFIG\CurrentProject'), 'Domain', 3, pString => 'paneliot.ru');

SELECT CreateClassTree();
SELECT CreateObjectType();
SELECT KernelInit();

SELECT FillCalendar(CreateCalendar(null, GetType('workday.calendar'), 'default.calendar', 'Календарь рабочих дней', 5, ARRAY[6,7], ARRAY[[1,1], [1,7], [2,23], [3,8], [5,1], [5,9], [6,12], [11,4]], '9 hour', '9 hour', '13 hour', '1 hour', 'Календарь рабочих дней.'), '2020/01/01', '2020/12/31');

SELECT CreateVendor(null, GetType('service.vendor'), 'system.vendor', 'Система', 'Системные услуги.');
SELECT CreateVendor(null, GetType('service.vendor'), 'mts.vendor', 'МТС', 'ПАО "МТС" (Мобитьные ТелеСистемы).');
SELECT CreateVendor(null, GetType('service.vendor'), 'google.vendor', 'Google', 'Google.');

SELECT CreateVendor(null, GetType('device.vendor'), 'incotex.vendor', 'Инкотекс', 'Группа компаний ИНКОТЕКС');

SELECT CreateAgent(null, GetType('system.agent'), 'system.agent', 'System', GetVendor('system.vendor'), 'Агент для обработки системных сообщений.');
SELECT CreateAgent(null, GetType('system.agent'), 'event.agent', 'Event', GetVendor('system.vendor'), 'Агент для обработки системных событий.');
SELECT CreateAgent(null, GetType('email.agent'), 'smtp.agent', 'SMTP', GetVendor('system.vendor'), 'Агент для передачи электронной почты по протоколу SMTP.');
SELECT CreateAgent(null, GetType('email.agent'), 'pop3.agent', 'POP3', GetVendor('system.vendor'), 'Агент для получения электронной почты по протоколу POP3.');
SELECT CreateAgent(null, GetType('email.agent'), 'imap.agent', 'IMAP', GetVendor('system.vendor'), 'Агент для получения электронной почты по протоколу IMAP.');
SELECT CreateAgent(null, GetType('stream.agent'), 'udp.agent', 'UDP', GetVendor('system.vendor'), 'Агент для обработки данных по протоколу UDP.');
SELECT CreateAgent(null, GetType('sms.agent'), 'm2m.agent', 'M2M', GetVendor('mts.vendor'), 'Агент для прёма и передачи коротких сообщений через МТС Коммуникатор.');
SELECT CreateAgent(null, GetType('push.agent'), 'fcm.agent', 'FCM', GetVendor('google.vendor'), 'Агент для передачи push-уведомлений через Google Firebase Cloud Messaging.');

SELECT CreateModel(null, GetType('phase1.model'), 'mercury_200.model', 'Меркурий 200', GetVendor('incotex.vendor'), 'Однофазный счётчик ватт-часов активной энергии переменного тока.');
SELECT CreateModel(null, GetType('phase1.model'), 'mercury_201.model', 'Меркурий 201', GetVendor('incotex.vendor'), 'Однофазный счётчик ватт-часов активной энергии переменного тока.');
SELECT CreateModel(null, GetType('phase1.model'), 'mercury_202.model', 'Меркурий 202', GetVendor('incotex.vendor'), 'Однофазный счётчик ватт-часов активной энергии переменного тока.');
SELECT CreateModel(null, GetType('phase1.model'), 'mercury_203.model', 'Меркурий 203', GetVendor('incotex.vendor'), 'Однофазный счётчик ватт-часов активной энергии переменного тока.');
SELECT CreateModel(null, GetType('phase1.model'), 'mercury_206.model', 'Меркурий 206', GetVendor('incotex.vendor'), 'Однофазный счётчик ватт-часов активной энергии переменного тока.');
SELECT CreateModel(null, GetType('phase1.model'), 'mercury_208.model', 'Меркурий 208', GetVendor('incotex.vendor'), 'Однофазный счётчик ватт-часов активной энергии переменного тока.');

SELECT CreateModel(null, GetType('phase3.model'), 'mercury_230.model', 'Меркурий 230', GetVendor('incotex.vendor'), 'Трехфазный счётчик ватт-часов активной энергии переменного тока.');
SELECT CreateModel(null, GetType('phase3.model'), 'mercury_231.model', 'Меркурий 231', GetVendor('incotex.vendor'), 'Трехфазный счётчик ватт-часов активной энергии переменного тока.');
SELECT CreateModel(null, GetType('phase3.model'), 'mercury_234.model', 'Меркурий 234', GetVendor('incotex.vendor'), 'Трехфазный счётчик ватт-часов активной энергии переменного тока.');
SELECT CreateModel(null, GetType('phase3.model'), 'mercury_236.model', 'Меркурий 236', GetVendor('incotex.vendor'), 'Трехфазный счётчик ватт-часов активной энергии переменного тока.');
SELECT CreateModel(null, GetType('phase3.model'), 'mercury_238.model', 'Меркурий 238', GetVendor('incotex.vendor'), 'Трехфазный счётчик ватт-часов активной энергии переменного тока.');

SELECT SignOut();