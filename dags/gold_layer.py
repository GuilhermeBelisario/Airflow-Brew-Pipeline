from pyspark.sql import SparkSession

def criar_tabelas_para_consumidores(container_silver: str,spark: SparkSession,storage_account_name):

    if not all([container_silver]):
        raise ValueError("Parâmetros de conexão não podem ser nulos")
    
    df = spark \
            .read \
            .parquet(f'abfss://{container_silver}@{storage_account_name}.dfs.core.windows.net/*.parquet')

    df_dim_brewery_type_table = df.select(
                "brewery_type"
                ).distinct()

    df_dim_brewery_table = df.select(
                "name",
                "phone",
                "is_phone_missing",
                "fixed_phone",
                "website_url"
                ).distinct()

    df_dim_location_table = df.select(
                "full_address",
                "address_1",
                "city",
                "state",
                "zip_code",
                "country",
                "longitude",
                "latitude",
                "has_location",
                ).distinct()

    df_fact_brewery_operations_table = df.select( 
                "metric_date",
                "data_de_processamento",
                "created_year",
                "created_month",
                "origem_do_dado",
                "formato_na_origem",
                "pipeline_vinculado",
                "nome_do_arquivo_original"
                )
    
    df_dict = {'dim_brewery_type': df_dim_brewery_type_table,
            'dim_brewery': df_dim_brewery_table,
            'dim_location': df_dim_location_table,
            'fact_brewery_operations': df_fact_brewery_operations_table}
    
    return df_dict
    

def gravar_tabelas_no_banco(df, table_name, jdbc_url, connection_properties):
    
    (df.write
      .format("jdbc")
      .option("url", jdbc_url)
      .option("dbtable", table_name)
      .option("user", connection_properties["user"])
      .option("password", connection_properties["password"])
      .option("driver", connection_properties["driver"])
      .mode("append") 
      .save())