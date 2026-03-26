## Подготовка данных и среды
В рамках домашней работы был настроен ETL-пайплайн на кластере Hadoop с использованием Apache Spark, Hive и Apache Airflow. Все сделано было на name node.

Как исходные данные использовал файл - `/raw/test.snappy.parquet`
Заранее загрузил Spark JAR-файлы в HDFS для ускорения запуска
```bash
# Создаём директорию в HDFS
/home/hadoop/hadoop-3.4.3/bin/hdfs dfs -mkdir -p /spark-jars

# Загружаем все JAR-файлы Spark
/home/hadoop/hadoop-3.4.3/bin/hdfs dfs -put \
    /home/hadoop/spark-3.5.3-bin-hadoop3/jars/* \
    /spark-jars/

# Проверяем загрузку
/home/hadoop/hadoop-3.4.3/bin/hdfs dfs -ls /spark-jars/ | wc -l
```
Написал скрипт `/home/hadoop/spark_pipeline_job.py`, выполняющий следующие шаги:

1. **Чтение данных** из HDFS (`/user/hive/warehouse/test/`) в формате Parquet    
2. **Очистка и трансформация:**
    - Фильтрация строк с пустыми `country` и `salary`
    - Приведение типа `salary` к `DoubleType` с округлением до 2 знаков
    - Добавление колонки `salary_tier` (Low / Mid / High) на основе размера зарплаты
    - Нормализация колонки `gender` к верхнему регистру
    - Удаление лишних колонок (`ip_address`, `cc`, `comments`)
3. **Агрегация** по `country`, `gender`, `salary_tier` с вычислением `employee_count`, `avg_salary`, `max_salary`, `min_salary`
4. **Сохранение результата** в Hive-таблицу `team06.salary_stats_by_country` в формате Parquet с компрессией Snappy и партиционированием по `country`
5. Протестировал скрипт вручную через `spark-submit`:
```bash
/home/hadoop/spark-3.5.3-bin-hadoop3/bin/spark-submit \
  --master yarn \
  --deploy-mode client \
  --conf "spark.yarn.jars=hdfs://team-06-nn:9000/spark-jars/*" \
  --conf "spark.hive.metastore.uris=thrift://team-06-nn:9083" \
  --conf "spark.sql.warehouse.dir=/user/hive/warehouse" \
  --conf "spark.executor.memory=1g" \
  --conf "spark.executor.instances=2" \
  /home/hadoop/spark_pipeline_job.py
```

## Настройка Apache Airflow
После успешного ручного запуска пайплайн был перенесён в Apache Airflow.
```bash
source ~/.venv/bin/activate

AIRFLOW_VERSION=2.9.3
PYTHON_VERSION="$(python3 -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

pip install \
    "apache-airflow==${AIRFLOW_VERSION}" \
    "apache-airflow-providers-apache-spark" \
    --constraint "${CONSTRAINT_URL}"

export AIRFLOW_HOME=~/airflow

airflow db migrate

airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin123

# Проверка
airflow users list
```
```bash
# Удаляем предыдущие попытки
airflow connections delete spark_yarn

# Создаём правильный connection
airflow connections add spark_yarn \
    --conn-type spark \
    --conn-host yarn \
    --conn-extra \
    '{"spark-binary": "spark-submit", "deploy-mode": "client", "queue": "default"}'

# Проверяем
airflow connections get spark_yarn
```

Был создан DAG spark_hive_dag с пятью задачами:
Файл лежит рядом
Отдельно была джоба в которой происходили действия с таблицей.
```text
check_hdfs ──┐
             ├──→ check_hive_services ──→ run_spark_job ──→ verify_hive_table
check_yarn ──┘
```

![[Pasted image 20260326223218.png]]

В UI появился пайплайн, логи пайпланы в отдельном файле.
## Проблемы при запуске через Airflow
В процессе настройки были последовательно решены несколько проблем:
**Проблема 1:** `spark-binary` в connection указан как полный путь.
- **Причина:** Airflow принимает только имя бинарника (`spark-submit`), не полный путь.
- **Решение:** Пересоздан connection с `"spark-binary": "spark-submit"`, путь добавлен в `PATH`.

**Проблема 2:** `spark-home` extra не поддерживается в новой версии провайдера.
- **Причина:** В `apache-airflow-providers-apache-spark` >=4.x параметр `spark-home` удалён.
- **Решение:** Убран `spark-home` из connection, Spark-бинарник добавлен в `PATH` окружения scheduler.