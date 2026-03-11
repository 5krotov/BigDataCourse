# Автоматизация

Сделана с помощью bash.
Все команды необходимо выполнять на name node.

Автоматизированное развёртывание Apache Hive с поддержкой одновременного
доступа нескольких клиентов (non-embedded режим) и автоматизированной
загрузкой данных в партиционированную таблицу.

**Ключевые решения:**
- HiveServer2 + Metastore — два отдельных процесса (не embedded)
- Multi-client через Thrift (порт 10000)
- MR выполняется в режиме `local` — без YARN, в JVM HiveServer2
- Metastore хранится в Derby (`/home/hadoop/hive4_metastore_db`)

## Скрипты

### setup_hive4.sh — полная установка

Фазы:
1. Остановка старых Hive-процессов
2. Загрузка и распаковка дистрибутива
3. Генерация `hive-site.xml`
4. Патч `mapred-site.xml`
5. Патч `core-site.xml` (proxyuser)
6. Симлинк `commons-collections-3.x` → `mapreduce/lib/`
7. Создание HDFS-директорий
8. Инициализация схемы Derby (`schematool -initSchema`)
9. Создание `start-hive.sh`

### start-hive.sh — управление сервисами

```bash
~/start-hive.sh start           # запуск Metastore + HiveServer2
~/start-hive.sh stop            # остановка
~/start-hive.sh restart         # рестарт
~/start-hive.sh status          # проверить порты и PID
~/start-hive.sh logs hs2        # tail -f логов HiveServer2
~/start-hive.sh logs metastore  # tail -f логов Metastore
```

verify_hive4.sh — автоматическая проверка
```bash
~/verify_hive4.sh
```
Ключевые конфигурации
hive-site.xml

| Параметр                              | Значение                | Назначение                     |
| ------------------------------------- | ----------------------- | ------------------------------ |
| hive.metastore.uris                   | thrift://localhost:9083 | Standalone metastore           |
| hive.server2.thrift.port              | 10000                   | Порт HiveServer2               |
| hive.server2.enable.doAs              | false                   | Без impersonation              |
| hive.exec.dynamic.partition.mode      | nonstrict               | Динамическое партиционирование |
| hive.exec.submit.local.task.via.child | false                   | FIX: tasks в JVM HiveServer2   |
| mapreduce.job.classloader             | false                   | FIX: единый classloader        |
| hive.stats.autogather                 | false                   | Без лишних MR job              |

mapred-site.xml

| Параметр                        | Значение      | Назначение                 |
| ------------------------------- | ------------- | -------------------------- |
| mapreduce.framework.name        | local         | In-process MR без YARN     |
| mapreduce.task.io.sort.mb       | 32            | FIX: OOM в MapOutputBuffer |
| mapreduce.application.classpath | ...hive/lib/* | FIX: Hive jar'ы в MR task  |

Пример работы на таблице
```bash
hadoop@team-06-nn:~$ beeline -u "jdbc:hive2://localhost:10000"   -e "USE team06; SELECT year, month, COUNT(*) AS cnt FROM employees GROUP BY year, month ORDER BY year, month;"  
Connecting to jdbc:hive2://localhost:10000
Connected to: Apache Hive (version 4.0.0-alpha-2)
Driver: Hive JDBC (version 4.0.0-alpha-2)
Transaction isolation: TRANSACTION_REPEATABLE_READ
INFO  : Compiling command(queryId=hadoop_20260311225010_aeba3fa6-00f7-4c1f-83ce-1327c1fcf8de): USE team06
INFO  : Semantic Analysis Completed (retrial = false)
INFO  : Created Hive schema: Schema(fieldSchemas:null, properties:null)
INFO  : Completed compiling command(queryId=hadoop_20260311225010_aeba3fa6-00f7-4c1f-83ce-1327c1fcf8de); Time taken: 0.022 seconds
INFO  : Concurrency mode is disabled, not creating a lock manager
INFO  : Executing command(queryId=hadoop_20260311225010_aeba3fa6-00f7-4c1f-83ce-1327c1fcf8de): USE team06
INFO  : Starting task [Stage-0:DDL] in serial mode
INFO  : Completed executing command(queryId=hadoop_20260311225010_aeba3fa6-00f7-4c1f-83ce-1327c1fcf8de); Time taken: 0.01 seconds
No rows affected (0.089 seconds)
INFO  : Compiling command(queryId=hadoop_20260311225010_e496d9ef-eb2f-4114-81aa-21683d8269d7): SELECT year, month, COUNT(*) AS cnt FROM employees GROUP BY year, month ORDER BY year, month
INFO  : Semantic Analysis Completed (retrial = false)
INFO  : Created Hive schema: Schema(fieldSchemas:[FieldSchema(name:year, type:int, comment:null), FieldSchema(name:month, type:int, comment:null), FieldSchema(name:cnt, type:bigint, comment:null)], properties:null)
INFO  : Completed compiling command(queryId=hadoop_20260311225010_e496d9ef-eb2f-4114-81aa-21683d8269d7); Time taken: 0.141 seconds
INFO  : Concurrency mode is disabled, not creating a lock manager
INFO  : Executing command(queryId=hadoop_20260311225010_e496d9ef-eb2f-4114-81aa-21683d8269d7): SELECT year, month, COUNT(*) AS cnt FROM employees GROUP BY year, month ORDER BY year, month
WARN  : Hive-on-MR is deprecated in Hive 2 and may not be available in the future versions. Consider using a different execution engine (i.e. tez) or using Hive 1.X releases.
INFO  : Query ID = hadoop_20260311225010_e496d9ef-eb2f-4114-81aa-21683d8269d7
INFO  : Total jobs = 2
INFO  : Launching Job 1 out of 2
INFO  : Starting task [Stage-1:MAPRED] in serial mode
INFO  : Number of reduce tasks not specified. Estimated from input data size: 1
INFO  : In order to change the average load for a reducer (in bytes):
INFO  :   set hive.exec.reducers.bytes.per.reducer=<number>
INFO  : In order to limit the maximum number of reducers:
INFO  :   set hive.exec.reducers.max=<number>
INFO  : In order to set a constant number of reducers:
INFO  :   set mapreduce.job.reduces=<number>
INFO  : number of splits:1
INFO  : Submitting tokens for job: job_local483064035_0003
INFO  : Executing with tokens: []
INFO  : The url to track the job: http://localhost:8080/
INFO  : Job running in-process (local Hadoop)
INFO  : 2026-03-11 22:50:12,290 Stage-1 map = 100%,  reduce = 100%, Cumulative CPU 0.23 sec
INFO  : MapReduce Total cumulative CPU time: 230 msec
INFO  : Ended Job = job_local483064035_0003
INFO  : Launching Job 2 out of 2
INFO  : Starting task [Stage-2:MAPRED] in serial mode
INFO  : Number of reduce tasks determined at compile time: 1
INFO  : In order to change the average load for a reducer (in bytes):
INFO  :   set hive.exec.reducers.bytes.per.reducer=<number>
INFO  : In order to limit the maximum number of reducers:
INFO  :   set hive.exec.reducers.max=<number>
INFO  : In order to set a constant number of reducers:
INFO  :   set mapreduce.job.reduces=<number>
INFO  : number of splits:1
INFO  : Submitting tokens for job: job_local819551342_0004
INFO  : Executing with tokens: []
INFO  : The url to track the job: http://localhost:8080/
INFO  : Job running in-process (local Hadoop)
INFO  : 2026-03-11 22:50:13,525 Stage-2 map = 100%,  reduce = 100%, Cumulative CPU 0.17 sec
INFO  : MapReduce Total cumulative CPU time: 170 msec
INFO  : Ended Job = job_local819551342_0004
INFO  : MapReduce Jobs Launched: 
INFO  : Stage-Stage-1:  Cumulative CPU: 0.23 sec   HDFS Read: 10238 HDFS Write: 1702 HDFS EC Read: 0 SUCCESS
INFO  : Stage-Stage-2:  Cumulative CPU: 0.17 sec   HDFS Read: 10238 HDFS Write: 2553 HDFS EC Read: 0 SUCCESS
INFO  : Total MapReduce CPU Time Spent: 400 msec
INFO  : Completed executing command(queryId=hadoop_20260311225010_e496d9ef-eb2f-4114-81aa-21683d8269d7); Time taken: 2.564 seconds
+-------+--------+------+
| year  | month  | cnt  |
+-------+--------+------+
| 2022  | 1      | 6    |
| 2022  | 2      | 1    |
| 2022  | 3      | 2    |
| 2022  | 4      | 3    |
| 2022  | 5      | 2    |
| 2022  | 6      | 4    |
| 2022  | 7      | 2    |
| 2022  | 8      | 4    |
| 2022  | 9      | 4    |
| 2022  | 10     | 4    |
| 2022  | 11     | 5    |
| 2022  | 12     | 2    |
| 2023  | 1      | 2    |
| 2023  | 2      | 3    |
| 2023  | 3      | 1    |
| 2023  | 4      | 2    |
| 2023  | 5      | 2    |
| 2023  | 6      | 6    |
| 2023  | 7      | 1    |
| 2023  | 8      | 3    |
| 2023  | 9      | 3    |
| 2023  | 10     | 5    |
| 2023  | 11     | 4    |
| 2023  | 12     | 2    |
| 2024  | 1      | 1    |
| 2024  | 2      | 3    |
| 2024  | 3      | 3    |
| 2024  | 4      | 2    |
| 2024  | 5      | 2    |
| 2024  | 6      | 1    |
| 2024  | 7      | 2    |
| 2024  | 8      | 3    |
| 2024  | 9      | 4    |
| 2024  | 11     | 4    |
| 2024  | 12     | 2    |
| 2025  | 1      | 3    |
+-------+--------+------+
36 rows selected (2.74 seconds)
Beeline version 4.0.0-alpha-2 by Apache Hive
Closing: 0: jdbc:hive2://localhost:10000
hadoop@team-06-nn:~$ 
```