from pyspark.sql import SparkSession
from pyspark.sql.functions import col, substring

import time

inicio = time.time()

# Inicia Spark
spark = (SparkSession
         .builder
         .appName("Casos")
         .master("local[2]")
         .getOrCreate())
df_municip = spark.read.option("header", "true").option("delimiter", ",").csv(
    "file:///home/kuchi/PycharmProjects/TCC/dados/dicionario_municipios/dicionario_municipios.csv"
)

df_chik = spark.read.option("header", "true").option("delimiter", ",").csv([
    "file:///home/kuchi/PycharmProjects/TCC/dados/Chikungunya/CHIKBR21.csv",
    "file:///home/kuchi/PycharmProjects/TCC/dados/Chikungunya/CHIKBR22.csv",
    "file:///home/kuchi/PycharmProjects/TCC/dados/Chikungunya/CHIKBR23.csv",
    "file:///home/kuchi/PycharmProjects/TCC/dados/Chikungunya/CHIKBR24.csv",
    "file:///home/kuchi/PycharmProjects/TCC/dados/Chikungunya/CHIKBR25.csv"
])

df_dengue = spark.read.option("header", "true").option("delimiter", ",").csv([
    "file:///home/kuchi/PycharmProjects/TCC/dados/Dengue/DENGBR21.csv",
    "file:///home/kuchi/PycharmProjects/TCC/dados/Dengue/DENGBR22.csv",
    "file:///home/kuchi/PycharmProjects/TCC/dados/Dengue/DENGBR23.csv",
    "file:///home/kuchi/PycharmProjects/TCC/dados/Dengue/DENGBR24.csv",
    "file:///home/kuchi/PycharmProjects/TCC/dados/Dengue/DENGBR25.csv"
])

df_zika = spark.read.option("header", "true").option("delimiter", ",").csv([
    "file:///home/kuchi/PycharmProjects/TCC/dados/Zika/ZIKABR21.csv",
    "file:///home/kuchi/PycharmProjects/TCC/dados/Zika/ZIKABR22.csv",
    "file:///home/kuchi/PycharmProjects/TCC/dados/Zika/ZIKABR23.csv",
    "file:///home/kuchi/PycharmProjects/TCC/dados/Zika/ZIKABR24.csv",
    "file:///home/kuchi/PycharmProjects/TCC/dados/Zika/ZIKABR25.csv"
])

df_srag = spark.read.option("header", "true").option("delimiter", ",").csv([
    "file:///home/kuchi/PycharmProjects/TCC/dados/SRAG/INFLUD21.csv",
    "file:///home/kuchi/PycharmProjects/TCC/dados/SRAG/INFLUD22.csv",
    "file:///home/kuchi/PycharmProjects/TCC/dados/SRAG/INFLUD23.csv",
    "file:///home/kuchi/PycharmProjects/TCC/dados/SRAG/INFLUD24.csv",
    "file:///home/kuchi/PycharmProjects/TCC/dados/SRAG/INFLUD25.csv"
])




fim = time.time()
print(f"Tempo de execução: {fim - inicio:.2f} segundos")


# Supondo que o dicionário de municípios está em df_municipios
df_municipios = df_municip.select(
    col("ID_MUNICIP").alias("ID_MUNICIP_DICT"),
    col("ID_SG_UF_NOT").alias("SG_UF_NOT_DICT"),
    col("NOME_MUNICIP"),
    col("Nome_UF")
)

# Join com ID_MUNICIP para pegar Nome_Município
df_zika = df_zika.join(
    df_municipios.select("ID_MUNICIP_DICT", "NOME_MUNICIP"),
    df_zika["ID_MUNICIP"] == col("ID_MUNICIP_DICT"),
    "left"
).drop("ID_MUNICIP").withColumnRenamed("NOME_MUNICIP", "ID_MUNICIP")

# Join com SG_UF_NOT para pegar Nome_UF
df_zika = df_zika.join(
    df_municipios.select("SG_UF_NOT_DICT", "Nome_UF"),
    df_zika["SG_UF_NOT"] == col("SG_UF_NOT_DICT"),
    "left"
).drop("SG_UF_NOT").withColumnRenamed("Nome_UF", "SG_UF_NOT")

df_zika.show()
print("Spark UI ativo. Acesse http://localhost:4040")
input("Pressione Enter para encerrar...")
