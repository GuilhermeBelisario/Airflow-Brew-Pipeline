from pyspark.sql import SparkSession

def criar_tabelas_para_consumidores(container_silver: str,spark: SparkSession, container_gold: str):
    
    if not all([container_silver,container_gold]):
        raise ValueError("Parâmetros de conexão não podem ser nulos")
    
    df = spark \
            .read \
            .parquet(f'abfss://{container_silver}@lofrey.dfs.core.windows.net/*.parquet')

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
    
    df_list = [df_dim_brewery_type_table,df_dim_brewery_table,df_dim_location_table,df_fact_brewery_operations_table]

    return df_list
    

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