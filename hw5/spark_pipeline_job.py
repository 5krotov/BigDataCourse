from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

spark = (
    SparkSession.builder
    .appName("airflow_spark_pipeline")
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
    .config("spark.hive.metastore.uris", "thrift://team-06-nn:9083")
    .config("spark.executor.memory", "1g")
    .config("spark.executor.instances", "2")
    .config("spark.executor.cores", "1")
    .enableHiveSupport()
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# 1. Чтение из HDFS — файл лежит в Hive warehouse
df = spark.read.parquet("hdfs://team-06-nn:9000/user/hive/warehouse/test/")
print(f"[INFO] Прочитано строк: {df.count()}")
df.printSchema()

# 2. Трансформации
df_clean = (
    df
    .filter(F.col("country").isNotNull() & F.col("salary").isNotNull())
    .withColumn("salary", F.round(F.col("salary").cast(DoubleType()), 2))
    .withColumn(
        "salary_tier",
        F.when(F.col("salary") < 50000, "Low")
         .when(F.col("salary") < 100000, "Mid")
         .otherwise("High")
    )
    .withColumn("gender", F.upper(F.col("gender")))
    .drop("ip_address", "cc", "comments")
)

# 3. Агрегация по стране и гендеру
df_agg = (
    df_clean
    .groupBy("country", "gender", "salary_tier")
    .agg(
        F.count("*").alias("employee_count"),
        F.round(F.avg("salary"), 2).alias("avg_salary"),
        F.round(F.max("salary"), 2).alias("max_salary"),
        F.round(F.min("salary"), 2).alias("min_salary"),
    )
    .orderBy("country", "gender")
)

print(f"[INFO] Агрегировано строк: {df_agg.count()}")
df_agg.show(20, truncate=False)

# 4. Сохранение в Hive с партиционированием по country
spark.sql("CREATE DATABASE IF NOT EXISTS team06")
df_agg.write \
    .mode("overwrite") \
    .format("parquet") \
    .option("compression", "snappy") \
    .partitionBy("country") \
    .saveAsTable("team06.salary_stats_by_country")

print("[INFO] team06.salary_stats_by_country — сохранена ✓")
spark.stop()