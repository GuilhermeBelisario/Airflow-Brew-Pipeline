from pyspark.sql import SparkSession
from pyspark.sql.functions import concat, lit, regexp_replace, substring, col, to_date, when,concat_ws,year,month


def transformar_dados(spark ,container_silver: str, container_bronze: str, storage_account_name: str):
    
    if not all([container_silver, container_bronze]):
        raise ValueError("Parâmetros de conexão não podem ser nulos")

    try:    
        df = spark.read \
                    .format('delta')\
                    .load(f'abfss://{container_bronze}@{storage_account_name}.dfs.core.windows.net/*.parquet')

        df = df.withColumnRenamed('stat_province', 'state') \
            .withColumnRenamed('postal_code', 'zip_code') \
            .withColumn(
                "fixed_phone",
                concat(
                    lit("+1 "),  
                    substring(regexp_replace(col("phone"), "[^0-9]", ""), 1, 3),  
                    lit("-"),
                    substring(regexp_replace(col("phone"), "[^0-9]", ""), 4, 3), 
                    lit("-"),
                    substring(regexp_replace(col("phone"), "[^0-9]", ""), 7, 4)
                )
            ) \
            .withColumn('created_at', to_date(col('created_at'), 'yyyy-MM-dd')) \
            .withColumn('updated_at', to_date(col('updated_at'), 'yyyy-MM-dd')) \
            .withColumn('latitude', col('latitude').cast('float')) \
            .withColumn('longitude', col('longitude').cast('float')) \
            .withColumn(
                'has_location',
                when(col('longitude').isNull() | col('latitude').isNull(), lit(0)).otherwise(lit(1))
            ) \
            .withColumn(
                'full_address',
                concat_ws(', ',  
                    lit('St.'),
                    col('address_1'),
                    col('city'),
                    col('state'),
                    col('zip_code'),
                    col('country')
                )
            ) \
            .withColumn(
                'is_phone_missing',
                when(col('phone').isNull(), lit(1)).otherwise(lit(0))
            ) \
            .withColumn('created_year', year(col('data_de_processamento'))) \
            .withColumn('created_month', month(col('data_de_processamento')))
        
        
        
        df.write \
                .format("delta") \
                .mode("overwrite") \
                .partitionBy("data_de_processamento") \
                .save(f"abfss://{container_silver}@{storage_account_name}.dfs.core.windows.net/brewery-silver-extracted")
        return print("Bronze Layer para Silver Layer finalizada!")
    
    except:
        return print('Falha ao tentar salvar o arquivo')