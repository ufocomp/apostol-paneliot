# PanelIoT Web Service

**Апостол Веб Сервис** - это инструмент (framework) для создания автоматизированных систем (CRM-систем), исходные коды.

Построен на базе [Апостол](https://github.com/ufocomp/apostol).

СТРУКТУРА КАТАЛОГОВ
-
    auto/                       содержит файлы со скриптами
    cmake-modules/              содержит файлы с модулями CMake
    conf/                       содержит файлы с настройками
    db/                         содержит файлы со скриптами базы данных
    doc/                        содержит файлы с документацией
    src/                        содержит файлы с исходным кодом
    ├─app/                      содержит файлы с исходным кодом: Apostol Web Service
    ├─core/                     содержит файлы с исходным кодом: Apostol Core
    ├─lib/                      содержит файлы с исходным кодом библиотек
    | └─delphi/                 содержит файлы с исходным кодом библиотеки*: Delphi classes for C++
    └─modules/                  содержит файлы с исходным кодом дополнений (модулей)
    www/                        содержит файлы с Веб-сайтом

ОПИСАНИЕ
-

**PanelIoT Web Service**, далее по тексту **_система_**, состоит из готовых технических решений собранных воедино для создания **бэкэнд** (backend) части:
- Автоматизированных систем (CRM-систем);
- Веб-приложений;
- Мобильных приложений.

[Подробное описание доступно по этой ссылке.](./doc/REST-API-ru.md)

API
-

Доступ к API **системы** предоставляется с помощью встроенного [сервера приложений](https://github.com/ufocomp/module-AppServer) (REST API) по адресу: [localhost:8080](http://localhost:8080)

СБОРКА И УСТАНОВКА
-
Для установки **системы** Вам потребуется:

Для сборки проекта Вам потребуется:

1. Компилятор C++;
1. [CMake](https://cmake.org) или интегрированная среда разработки (IDE) с поддержкой [CMake](https://cmake.org);
1. Библиотека [libpq-dev](https://www.postgresql.org/download) (libraries and headers for C language frontend development);
1. Библиотека [postgresql-server-dev-12](https://www.postgresql.org/download) (libraries and headers for C language backend development).

### Linux (Debian/Ubuntu)

Для того чтобы установить компилятор C++ и необходимые библиотеки на Ubuntu выполните:
~~~
$ sudo apt-get install build-essential libssl-dev libcurl4-openssl-dev make cmake gcc g++
~~~

###### Подробное описание установки C++, CMake, IDE и иных компонентов необходимых для сборки проекта не входит в данное руководство. 

#### PostgreSQL

Для того чтобы установить PostgreSQL воспользуйтесь инструкцией по [этой](https://www.postgresql.org/download/) ссылке.

#### База данных `paneliot`

Для того чтобы установить базу данных необходимо выполнить:

1. Прописать наименование базы данных в файле db/sql/sets.conf (по умолчанию: paneliot)
1. Прописать пароли для пользователей СУБД [libpq-pgpass](https://postgrespro.ru/docs/postgrespro/11/libpq-pgpass):
   ~~~
   $ sudo -iu postgres -H vim .pgpass
   ~~~
   ~~~
   *:*:*:kernel:kernel
   *:*:*:admin:admin
   *:*:*:daemon:daemon
   ~~~
1. Указать в файле настроек /etc/postgresql/<version>/main/postgresql.conf:
   Пути поиска схемы kernel:
   ~~~
   search_path = '"$user", kernel, public'	# schema names
   ~~~
1. Указать в файле настроек /etc/postgresql/<version>/main/pg_hba.conf:
   ~~~
   # TYPE  DATABASE        USER            ADDRESS                 METHOD
   local	all		kernel					md5
   local	all		admin					md5
   local	all		daemon					md5
    
   host	all		kernel		127.0.0.1/32		md5
   host	all		admin		127.0.0.1/32		md5
   host	all		daemon		127.0.0.1/32		md5   
   ~~~
1. Выполнить:
   ~~~
   $ cd db/
   $ ./install.sh --make
   ~~~

###### Параметр `--make` необходим для установки базы данных в первый раз. Далее установочный скрипт можно запускать или без параметров или с параметром `--install`.

Для установки **системы** (без Git) необходимо:

1. Скачать **Апостол Веб Сервис** по [ссылке](https://github.com/ufocomp/apostol-paneliot/archive/master.zip);
1. Распаковать;
1. Настроить `CMakeLists.txt` (по необходимости);
1. Собрать и скомпилировать (см. ниже).

Для установки **системы** с помощью Git выполните:
~~~
$ git clone https://github.com/ufocomp/apostol-paneliot.git
~~~

###### Сборка:
~~~
$ cd apostol-paneliot
$ ./configure
~~~

###### Компиляция и установка:
~~~
$ cd cmake-build-release
$ make
$ sudo make install
~~~

По умолчанию бинарный файл `paneliot` будет установлен в:
~~~
/usr/sbin
~~~

Файл конфигурации и необходимые для работы файлы, в зависимости от варианта установки, будут расположены в: 
~~~
/etc/paneliot
или
~/paneliot
~~~

ЗАПУСК 
-
###### Если `INSTALL_AS_ROOT` установлено в `ON`.

**`paneliot`** - это системная служба (демон) Linux. 
Для управления **`paneliot`** используйте стандартные команды управления службами.

Для запуска `paneliot` выполните:
~~~
$ sudo service paneliot start
~~~

Для проверки статуса выполните:
~~~
$ sudo service paneliot status
~~~

Результат должен быть **примерно** таким:
~~~
● paneliot.service - LSB: starts the apostol web service
   Loaded: loaded (/etc/init.d/paneliot; generated; vendor preset: enabled)
   Active: active (running) since Tue 2020-08-25 23:04:53 UTC; 4 days ago
     Docs: man:systemd-sysv-generator(8)
  Process: 6310 ExecStop=/etc/init.d/paneliot stop (code=exited, status=0/SUCCESS)
  Process: 6987 ExecStart=/etc/init.d/paneliot start (code=exited, status=0/SUCCESS)
    Tasks: 3 (limit: 4915)
   CGroup: /system.slice/paneliot.service
           ├─6999 paneliot: master process /usr/sbin/paneliot
           ├─7000 paneliot: worker process ("web socket api", "application server", "authorization server", "web server")
           └─7001 paneliot: helper process ("certificate downloader")
~~~

### **Управление**.

Управлять **`paneliot`** можно с помощью сигналов.
Номер главного процесса по умолчанию записывается в файл `/run/paneliot.pid`. 
Изменить имя этого файла можно при конфигурации сборки или же в `paneliot.conf` секция `[daemon]` ключ `pid`. 

Главный процесс поддерживает следующие сигналы:

|Сигнал   |Действие          |
|---------|------------------|
|TERM, INT|быстрое завершение|
|QUIT     |плавное завершение|
|HUP	  |изменение конфигурации, запуск новых рабочих процессов с новой конфигурацией, плавное завершение старых рабочих процессов|
|WINCH    |плавное завершение рабочих процессов|	

Управлять рабочими процессами по отдельности не нужно. Тем не менее, они тоже поддерживают некоторые сигналы:

|Сигнал   |Действие          |
|---------|------------------|
|TERM, INT|быстрое завершение|
|QUIT	  |плавное завершение|
