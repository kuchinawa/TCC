from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("TESTE_TCC") \
    .master("local[*]") \
    .config("spark.driver.memory", "4g")\
    .config("spark.executor.memory", "4g")\
    .getOrCreate()

data = [("Alice", 34), ("Bob", 45), ("Cathy", 29)]
columns = ["Name", "Age"]
df = spark.createDataFrame(data, columns)
df.show()

input("Pressione Enter para parar o programa...\n")

spark.stop()



