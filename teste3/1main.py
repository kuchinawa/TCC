'''from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, sum
from sparkmeasure import StageMetrics, TaskMetrics

spark = SparkSession.builder \
    .appName("ETL_TCC") \
    .master("local[*]") \
    .config("spark.driver.memory", "4g")\
    .config("spark.executor.memory", "4g")\
    .getOrCreate()


stage_metrics = StageMetrics(spark)
task_metrics = TaskMetrics(spark)
stage_metrics.begin()
task_metrics.begin()


df = spark.read.parquet("../dados/INFLUD21.parquet")

# 2. Seleção das colunas úteis
colunas_uteis = [
    "SEM_NOT", "SG_UF_NOT", "NU_IDADE_N", "CS_SEXO", "CS_GESTANT",
    "EVOLUCAO", "NU_NOTIFIC",
    # Exemplo de colunas de sintomas
    "FEBRE", "TOSSE", "GARGANTA", "DISPNEIA", "DIARREIA", "VOMITO", "DOR_ABD", "FADIGA", "PERD_OLFT", "PERD_PALA",
    # Exemplo de colunas de comorbidades
    "CARDIOPATI", "HEMATOLOGI", "SIND_DOWN", "HEPATICA", "ASMA", "DIABETES", "NEUROLOGIC", "PNEUMOPATI", "IMUNODEPRE", "RENAL", "OBESIDADE"
]
df = df.select(*colunas_uteis)

# 3. Filtrar idades válida
df = df.filter((col("NU_IDADE_N") >= 0) & (col("NU_IDADE_N") <= 120))

# 4. Remover registros com colunas essenciais nulas e duplicados
df = df.na.drop(subset=["EVOLUCAO", "SG_UF_NOT", "SEM_NOT"])
df = df.dropDuplicates(["NU_NOTIFIC", "SEM_NOT"])

# 5. Criar faixa etária (bebês idade 0 entram em "menor")
df = df.withColumn(
    "FAIXA_ETARIA",
    when((col("NU_IDADE_N") >= 0) & (col("NU_IDADE_N") < 18), "menor")
    .when((col("NU_IDADE_N") >= 18) & (col("NU_IDADE_N") < 60), "adulto")
    .otherwise("idoso")
)

# 6. Listas das colunas de sintomas e comorbidades
colunas_sintomas = ["FEBRE", "TOSSE", "GARGANTA", "DISPNEIA", "DIARREIA", "VOMITO", "DOR_ABD", "FADIGA", "PERD_OLFT", "PERD_PALA"]
colunas_comorbidades = ["CARDIOPATI", "HEMATOLOGI", "SIND_DOWN", "HEPATICA", "ASMA", "DIABETES", "NEUROLOGIC", "PNEUMOPATI", "IMUNODEPRE", "RENAL", "OBESIDADE"]

# 7. Calcular quantidade de sintomas
df = df.withColumn(
    "QTD_SINTOMAS",
    when(col("FEBRE") == 1, 1).otherwise(0) +
    when(col("TOSSE") == 1, 1).otherwise(0) +
    when(col("GARGANTA") == 1, 1).otherwise(0) +
    when(col("DISPNEIA") == 1, 1).otherwise(0) +
    when(col("DIARREIA") == 1, 1).otherwise(0) +
    when(col("VOMITO") == 1, 1).otherwise(0) +
    when(col("DOR_ABD") == 1, 1).otherwise(0) +
    when(col("FADIGA") == 1, 1).otherwise(0) +
    when(col("PERD_OLFT") == 1, 1).otherwise(0) +
    when(col("PERD_PALA") == 1, 1).otherwise(0)
)

# 8. Calcular quantidade de comorbidades
df = df.withColumn(
    "QTD_COMORBIDADES",
    when(col("CARDIOPATI") == 1, 1).otherwise(0) +
    when(col("HEMATOLOGI") == 1, 1).otherwise(0) +
    when(col("SIND_DOWN") == 1, 1).otherwise(0) +
    when(col("HEPATICA") == 1, 1).otherwise(0) +
    when(col("ASMA") == 1, 1).otherwise(0) +
    when(col("DIABETES") == 1, 1).otherwise(0) +
    when(col("NEUROLOGIC") == 1, 1).otherwise(0) +
    when(col("PNEUMOPATI") == 1, 1).otherwise(0) +
    when(col("IMUNODEPRE") == 1, 1).otherwise(0) +
    when(col("RENAL") == 1, 1).otherwise(0) +
    when(col("OBESIDADE") == 1, 1).otherwise(0)
)

# 9. Agrupar e agregar os dados
df_grouped = df.groupBy("SEM_NOT", "SG_UF_NOT", "FAIXA_ETARIA").agg(
    sum(when(col(   "EVOLUCAO").isin(1, 9), 1).otherwise(0)).alias("casos"),
    sum(when(col("EVOLUCAO").isin(2, 3), 1).otherwise(0)).alias("obitos"),
    sum("QTD_SINTOMAS").alias("total_sintomas"),
    sum("QTD_COMORBIDADES").alias("total_comorbidades")
)

# 10. Ordenar pelo campo SEM_NOT
df_grouped = df_grouped.orderBy("SEM_NOT")

# 11. Mostrar resultado
df_grouped.show(truncate=False)

#12 salva em csv
df_grouped.coalesce(1).write.mode("overwrite").option("header", "true").csv("../dados/ETL_TCC_Output.csv")

# 13. Finalizar Spark
stage_metrics.end()
task_metrics.end()

stage_metrics.print_report()
task_metrics.print_report()
spark.stop()
'''
