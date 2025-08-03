from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, udf
from pyspark.sql.types import StringType

# Inicia sessão Spark
spark = SparkSession.builder.appName("DicionarioMunicipios").getOrCreate()

# 1. Lê o CSV original
df_raw = spark.read.option("header", "true").option("delimiter", ",").csv(
    "file:///home/kuchi/PycharmProjects/TCC/dados/RELATORIO_DTB_BRASIL_2024_MUNICIPIOS.csv"
)

df_raw.show()

uf_dict = {
    11: "RO", 12: "AC", 13: "AM", 14: "RR", 15: "PA", 16: "AP", 17: "TO",
    21: "MA", 22: "PI", 23: "CE", 24: "RN", 25: "PB", 26: "PE", 27: "AL", 28: "SE", 29: "BA",
    31: "MG", 32: "ES", 33: "RJ", 35: "SP",
    41: "PR", 42: "SC", 43: "RS",
    50: "MS", 51: "MT", 52: "GO", 53: "DF"
}


df = df_raw.withColumn("ID_SG_UF_NOT", col("UF"))

def map_uf_sigla(codigo):
    try:
        return uf_dict.get(int(codigo), None)
    except:
        return None

map_uf_udf = udf(map_uf_sigla, StringType())
df = df.withColumn("SG_UF_NOT", map_uf_udf(col("UF")))


df_final = df.select(
    col("ID_SG_UF_NOT"),
    col("SG_UF_NOT"),
    col("Nome_UF").alias("NOME_UF"),
    col("Código Município Completo").alias("ID_MUNICIP"),
    col("Nome_Município").alias("NOME_MUNICIP")
)


df_final = df_final.withColumn("ID_MUNICIP", expr("substring(ID_MUNICIP, 1, 6)"))

df_final.coalesce(1).write.option("header", "true").mode("overwrite").csv(
    "file:///home/kuchi/PycharmProjects/TCC/dados/tmp_dicionario"
)