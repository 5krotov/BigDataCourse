from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

SPARK_HOME   = "/home/hadoop/spark-3.5.3-bin-hadoop3"
HADOOP_HOME  = "/home/hadoop/hadoop-3.4.3"
HIVE_HOME    = "/home/hadoop/apache-hive-4.0.0-alpha-2-bin"
VENV_PYTHON  = "/home/hadoop/.venv/bin/python3"

default_args = {
    "owner": "hadoop",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "start_date": datetime(2026, 3, 26),
}

with DAG(
    dag_id="spark_hive_pipeline",
    default_args=default_args,
    description="Spark YARN → HDFS → transform → Hive table",
    schedule_interval=None,          # запускаем вручную
    catchup=False,
    tags=["spark", "hive", "hdfs"],
) as dag:

    # Task 1: проверка HDFS
    check_hdfs = BashOperator(
        task_id="check_hdfs",
        bash_command=(
            f"{HADOOP_HOME}/bin/hdfs dfs -ls /raw && "
            f"echo 'HDFS OK'"
        ),
    )

    # Task 2: проверка Hive-сервисов
    check_hive = BashOperator(
        task_id="check_hive_services",
        bash_command=(
            "nc -z localhost 9083 && echo 'Metastore UP' || "
            f"(nohup {HIVE_HOME}/bin/hive --service metastore "
            ">> /tmp/hadoop/hive-metastore.log 2>&1 & sleep 30 && "
            "nc -z localhost 9083 && echo 'Metastore started') && "
            "nc -z localhost 10000 && echo 'HiveServer2 UP' || "
            f"(nohup {HIVE_HOME}/bin/hive --service hiveserver2 "
            ">> /tmp/hadoop/hive-server2.log 2>&1 & sleep 40 && "
            "echo 'HiveServer2 started')"
        ),
    )

    # Task 3: проверка YARN
    check_yarn = BashOperator(
        task_id="check_yarn",
        bash_command=(
            f"{HADOOP_HOME}/bin/yarn node -list 2>&1 | grep 'Total Nodes' && "
            f"echo 'YARN OK'"
        ),
    )

    # Task 4: Spark job
    run_spark = SparkSubmitOperator(
        task_id="run_spark_job",
        conn_id="spark_yarn",          # создадим в шаге 6
        application="/home/hadoop/spark_pipeline_job.py",
        name="airflow_spark_pipeline",
        deploy_mode="client",
        executor_memory="1g",
        executor_cores=1,
        num_executors=2,
        conf={
            "spark.hive.metastore.uris": "thrift://team-06-nn:9083",
            "spark.sql.warehouse.dir": "/user/hive/warehouse",
            "spark.yarn.appMasterEnv.PYSPARK_PYTHON": VENV_PYTHON,
            "spark.executorEnv.PYSPARK_PYTHON": VENV_PYTHON,
        },
        env_vars={
            "HADOOP_CONF_DIR": f"{HADOOP_HOME}/etc/hadoop",
            "JAVA_HOME": "/usr/lib/jvm/java-8-openjdk-amd64",
            "PYSPARK_PYTHON": VENV_PYTHON,
        },
    )

    # Task 5: верификация таблицы в Hive
    verify_table = BashOperator(
        task_id="verify_hive_table",
        bash_command=(
            f"{HIVE_HOME}/bin/beeline -u jdbc:hive2://localhost:10000 "
            "--silent=true "
            "-e \"SELECT country, gender, employee_count, avg_salary "
            "FROM team06.salary_stats_by_country "
            "ORDER BY avg_salary DESC LIMIT 10;\""
        ),
    )

    # Граф зависимостей
    [check_hdfs, check_yarn] >> check_hive >> run_spark >> verify_table