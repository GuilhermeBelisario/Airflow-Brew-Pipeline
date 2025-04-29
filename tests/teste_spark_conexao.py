from pyspark.sql import SparkSession

try:
    spark_test = SparkSession.builder.master("local[1]").getOrCreate()
    print("Spark funcionando corretamente!")
    spark_test.stop()
except Exception as e:
    print(f"Erro: {str(e)}")