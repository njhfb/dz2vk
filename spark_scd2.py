"""
SCD Type 2 по CDC-потоку из Kafka (Debezium) в Iceberg.
Читаем топик pg.public.accounts микробатчами и в каждом батче делаем MERGE INTO.
"""
from pyspark.sql import SparkSession, Window, functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, LongType

spark = (
    SparkSession.builder.appName("accounts_scd2")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.lake", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lake.type", "rest")
    .config("spark.sql.catalog.lake.uri", "http://iceberg-rest:8181")
    .config("spark.sql.catalog.lake.warehouse", "s3://warehouse/")
    .config("spark.sql.catalog.lake.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .config("spark.sql.catalog.lake.s3.endpoint", "http://minio:9000")
    .config("spark.sql.catalog.lake.s3.path-style-access", "true")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.inventory")
spark.sql("""
    CREATE TABLE IF NOT EXISTS lake.inventory.accounts_scd2 (
        id         INT,
        owner      STRING,
        status     STRING,
        valid_from TIMESTAMP,
        valid_to   TIMESTAMP,
        is_current BOOLEAN
    ) USING iceberg
""")

# Формат события Debezium (schemas.enable=false в конфиге коннектора)
row = StructType([
    StructField("id", IntegerType()),
    StructField("owner", StringType()),
    StructField("status", StringType()),
])
event_schema = StructType([
    StructField("before", row),
    StructField("after", row),
    StructField("source", StructType([StructField("lsn", LongType())])),
    StructField("op", StringType()),      # r - снапшот, c - insert, u - update, d - delete
    StructField("ts_ms", LongType()),     # время изменения в источнике
])


def process_batch(df, batch_id):
    if df.isEmpty():
        return

    ev = (
        df.select(F.from_json(F.col("value").cast("string"), event_schema).alias("e"))
        .select("e.*")
        .where(F.col("op").isNotNull())
        .select(
            F.coalesce(F.col("after.id"), F.col("before.id")).alias("id"),
            F.col("after.owner").alias("owner"),
            F.col("after.status").alias("status"),
            F.col("op"),
            F.col("ts_ms"),
            F.col("source.lsn").alias("lsn"),
            (F.col("ts_ms") / 1000).cast("timestamp").alias("event_ts"),
        )
        # 1. дедупликация: одно и то же событие могло прийти дважды
        .dropDuplicates(["id", "ts_ms", "lsn", "op"])
    )

    # 2. порядок событий по каждой записи и время следующего изменения
    w = Window.partitionBy("id").orderBy("ts_ms", "lsn")
    ev = (
        ev.withColumn("next_ts", F.lead("event_ts").over(w))
          .withColumn("first_ts", F.min("event_ts").over(Window.partitionBy("id")))
    )

    # 3. новые версии (для delete новой версии нет, он только закрывает старую)
    new_versions = (
        ev.where(F.col("op") != "d")
        .select(
            F.lit(None).cast("int").alias("merge_key"),
            "id", "owner", "status",
            F.col("event_ts").alias("valid_from"),
            F.col("next_ts").alias("valid_to"),
            F.col("next_ts").isNull().alias("is_current"),
            F.lit(None).cast("timestamp").alias("close_ts"),
        )
    )

    # 4. строки для закрытия текущей версии в таблице (время первого события в батче)
    to_close = (
        ev.select("id", "first_ts").distinct()
        .select(
            F.col("id").alias("merge_key"),
            F.col("id"),
            F.lit(None).cast("string").alias("owner"),
            F.lit(None).cast("string").alias("status"),
            F.lit(None).cast("timestamp").alias("valid_from"),
            F.lit(None).cast("timestamp").alias("valid_to"),
            F.lit(False).alias("is_current"),
            F.col("first_ts").alias("close_ts"),
        )
    )

    new_versions.unionByName(to_close).createOrReplaceTempView("staged_changes")

    # 5. MERGE: у строк с merge_key закрываем текущую версию, строки без merge_key вставляем
    spark.sql("""
        MERGE INTO lake.inventory.accounts_scd2 t
        USING staged_changes s
        ON t.id = s.merge_key AND t.is_current = true
        WHEN MATCHED THEN UPDATE SET valid_to = s.close_ts, is_current = false
        WHEN NOT MATCHED AND s.merge_key IS NULL THEN
            INSERT (id, owner, status, valid_from, valid_to, is_current)
            VALUES (s.id, s.owner, s.status, s.valid_from, s.valid_to, s.is_current)
    """)
    print(f"batch {batch_id}: обработано событий {ev.count()}")


raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", "pg.public.accounts")
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
)

query = (
    raw.writeStream.foreachBatch(process_batch)
    .option("checkpointLocation", "/checkpoint/accounts_scd2")
    .trigger(processingTime="10 seconds")
    .start()
)
query.awaitTermination()
