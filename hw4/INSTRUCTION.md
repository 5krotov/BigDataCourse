Манипуляции мы производим на nn, поскольку именно там у нас установлен hive (на
Derby, не PostgresQL).

## HDFS

Паркет возьмём из открытого репозитория https://github.com/kaysush/sample-parquet-files.

```bash
hadoop@team-06-nn:~$ ls -la part*
-rw-rw-r-- 1 hadoop hadoop 69287 Mar 19 00:56 part-00000-a9e77425-5fb4-456f-ba52-f821123bd193-c000.snappy.parquet
hadoop@team-06-nn:~$ mv part-00000-a9e77425-5fb4-456f-ba52-f821123bd193-c000.snappy.parquet test.snappy.parquet
```

```bash
hadoop@team-06-nn:~$ hdfs dfs -mkdir /raw
hadoop@team-06-nn:~$ hdfs dfs -put test.snappy.parquet /raw
hadoop@team-06-nn:~$ hdfs dfs -ls /raw
Found 1 items
-rw-r--r--   3 hadoop supergroup      69287 2026-03-19 01:48 /raw/test.snappy.parquet
```

## Hive

```bash
hadoop@team-06-nn:~$ cd $HIVE_HOME
hadoop@team-06-nn:~$ ./bin/beeline -u jdbc:hive2://localhost:10000
0: jdbc:hive2://localhost:10000> SHOW DATABASES;
...
+----------------+
| database_name  |
+----------------+
| default        |
| team06         |
+----------------+
2 rows selected (0.015 seconds)
```

Схема файла `test.snappy.parquet` следующая:

```
Column	    Type
-------------------
id	        INTEGER	
first_name	VARCHAR	
last_name	VARCHAR	
email	    VARCHAR	
gender	    VARCHAR	
ip_address	VARCHAR	
cc	        VARCHAR	
country	    VARCHAR	
birthdate	VARCHAR	
salary	    DOUBLE	
title	    VARCHAR	
comments	VARCHAR	
```

Сделаем на её основе SQL-запрос:

```sql
CREATE TABLE test (
  id INT,
  first_name STRING,
  last_name STRING,
  email STRING,
  gender STRING,
  ip_address STRING,
  cc STRING,
  country STRING,
  birthdate STRING,
  salary DOUBLE,
  title STRING,
  comments STRING
)
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY');
```

```sql
LOAD DATA INPATH '/raw/test.snappy.parquet' INTO TABLE test;
```

```sql
0: jdbc:hive2://localhost:10000> SELECT test.id, test.email, test.country FROM test LIMIT 10;
...
+----------+---------------------------+-------------------------+
| test.id  |        test.email         |      test.country       |
+----------+---------------------------+-------------------------+
| 1        | ajordan0@com.com          | Indonesia               |
| 2        | afreeman1@is.gd           | Canada                  |
| 3        | emorgan2@altervista.org   | Russia                  |
| 4        | driley3@gmpg.org          | China                   |
| 5        | cburns4@miitbeian.gov.cn  | South Africa            |
| 6        | kwhite5@google.com        | Indonesia               |
| 7        | sholmes6@foxnews.com      | Portugal                |
| 8        | hhowell7@eepurl.com       | Bosnia and Herzegovina  |
| 9        | jfoster8@yelp.com         | South Korea             |
| 10       | estewart9@opensource.org  | Nigeria                 |
+----------+---------------------------+-------------------------+
10 rows selected (0.071 seconds)
```

## Make it partitioned

```sql
CREATE TABLE test_partitioned (
  id INT,
  first_name STRING,
  last_name STRING,
  email STRING,
  gender STRING,
  ip_address STRING,
  cc STRING,
  birthdate STRING,
  salary DOUBLE,
  title STRING,
  comments STRING
)
PARTITIONED BY (country STRING)
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY');
```

```sql
INSERT INTO test_partitioned SELECT * FROM test;
```

## Metastore

```bash
hadoop@team-06-nn:~$ hive --hiveconf hive.server2.enable.doAs=false --hiveconf hive.security.authorization.enabled=false --service metastore 1>> /tmp/hms.log 2>> /tmp/hms.log &
[1] 395384
hadoop@team-06-nn:~$ jps
225571 DataNode
247474 RunJar
393399 RunJar
156481 JobHistoryServer
395461 Jps
225383 NameNode
246006 NodeManager
245864 ResourceManager
47660 SecondaryNameNode
247325 RunJar
```

## Spark

```bash
hadoop@team-06-nn:~$ wget https://archive.apache.org/dist/spark/spark-3.5.3/spark-3.5.3-bin-hadoop3.tgz
hadoop@team-06-nn:~$ tar -xvf spark-3.5.3-bin-hadoop3.tgz
```

```bash
echo > ~/.profile <<EOF
export SPARK_DIST_CLASSPATH="/home/hadoop/spark-3.5.3-bin-hadoop3/jars/*:/home/hadoop/hadoop-3.4.0/etc/hadoop:/home/hadoop/hadoop-3.4.0/share/hadoop/common/lib/*:/home/hadoop/hadoop-3.4.0/share/hadoop/common/*:/home/hadoop/hadoop-3.4.0/share/hadoop/hdfs:/home/hadoop/hadoop-3.4.0/share/hadoop/hdfs/*:/home/hadoop/hadoop-3.4.0/share/hadoop/mapreduce/*:/home/hadoop/hadoop-3.4.0/share/hadoop/yarn:/home/hadoop/hadoop-3.4.0/share/hadoop/yarn/lib/*:/home/hadoop/hadoop-3.4.0/share/hadoop/yarn/*:/home/hadoop/apache-hive-4.0.0-alpha-2-bin/*:/home/hadoop/apache-hive-4.0.0-alpha-2-bin/lib/*"
EOF
```

## Python

```bash
ubuntu@team-06-nn:~$ sudo apt install python3-pip
```

```bash
hadoop@team-06-nn:~$ python3 -m venv .venv
hadoop@team-06-nn:~$ source ~/.venv/bin/activate
(.venv) hadoop@team-06-nn:~$ pip install -U pip
(.venv) hadoop@team-06-nn:~$ pip install onetl ipython pyspark==3.5.3
```

```ipython
In [1]: from onetl.connection import Hive
/home/hadoop/.venv/lib/python3.12/site-packages/etl_entities/process/process_stack_manager.py:27: UserWarning: Deprecated in v2.0, will be removed in v3.0
  default: ClassVar[Process] = Process()  # noqa: WPS462

In [2]: from pyspark.sql import SparkSession

In [3]: from onetl.db import DBWriter, DBReader

In [4]: spark = (
   ...: SparkSession.builder.master("yarn")
   ...: .appName("spark_check_yarn")
   ...: .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
   ...: .config("spark.hive.metastore.uris", "thrift://team-06-nn:9083")
   ...: .enableHiveSupport()
   ...: .getOrCreate()
   ...: )

In [5]: hive = Hive(spark=spark, cluster="x")

In [6]: hive.check()

In [7]: reader = DBReader(connection=hive, table="default.test")

In [8]: df = reader.run()

In [9]: df.count()

In [10]: df.show(10)

In [11]: df.rdd.getNumPartitions()

In [12]: df = df.limit(1000000).select("country", "email", "gender")

In [13]: writer = DBWriter(connection=hive, target="default.test_spark_partitioned")

In [14]: writer.run(df.repartition("country"))

In [15]: spark.stop()
```
