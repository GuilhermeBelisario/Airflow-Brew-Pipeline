from pyspark.sql import SparkSession
import pyspark.sql.functions as F


def transformar_dados(
    spark, container_silver: str, container_bronze: str, storage_account_name: str
):

    if not all([container_silver, container_bronze]):
        raise ValueError("Parâmetros de conexão não podem ser nulos")

    try:
        df = spark.read.format("parquet").load(
            f"abfss://{container_bronze}@{storage_account_name}.dfs.core.windows.net/*.parquet"
        )

        df = (
            df.withColumnRenamed("stat_province", "state")
            .withColumnRenamed("postal_code", "zip_code")
            .withColumn(
                "fixed_phone",
                F.concat(
                    F.lit("+1 "),
                    F.substring(F.regexp_replace(F.col("phone"), "[^0-9]", ""), 1, 3),
                    F.lit("-"),
                    F.substring(F.regexp_replace(F.col("phone"), "[^0-9]", ""), 4, 3),
                    F.lit("-"),
                    F.substring(F.regexp_replace(F.col("phone"), "[^0-9]", ""), 7, 4),
                ),
            )
            .withColumn("created_at", F.to_date(F.col("created_at"), "yyyy-MM-dd"))
            .withColumn("updated_at", F.to_date(F.col("updated_at"), "yyyy-MM-dd"))
            .withColumn("latitude", F.col("latitude").cast("float"))
            .withColumn("longitude", F.col("longitude").cast("float"))
            .withColumn(
                "has_location",
                F.when(
                    F.col("longitude").isNull() | F.col("latitude").isNull(), F.lit(0)
                ).otherwise(F.lit(1)),
            )
            .withColumn(
                "full_address",
                F.concat_ws(
                    ", ",
                    F.lit("St."),
                    F.col("address_1"),
                    F.col("city"),
                    F.col("state"),
                    F.col("zip_code"),
                    F.col("country"),
                ),
            )
            .withColumn(
                "is_phone_missing",
                F.when(F.col("phone").isNull(), F.lit(1)).otherwise(F.lit(0)),
            )
            .withColumn("created_year", F.year(F.col("data_de_processamento")))
            .withColumn("created_month", F.month(F.col("data_de_processamento")))
        )

        df.write.format("delta").mode("overwrite").partitionBy(
            "data_de_processamento"
        ).save(
            f"abfss://{container_silver}@{storage_account_name}.dfs.core.windows.net/brewery-silver-extracted"
        )
        return print("Bronze Layer para Silver Layer finalizada!")

    except:
        return print("Falha ao tentar salvar o arquivo")
