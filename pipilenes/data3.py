import streamlit as st
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

spark = SparkSession.builder.appName("Dashboard_RN").getOrCreate()

# Leitura das tabelas prata
df_fato = spark.read.parquet("file:///home/kuchi/PycharmProjects/TCC/dados/prata/SRAG/fato_notificacao")
df_municipio = spark.read.parquet("file:///home/kuchi/PycharmProjects/TCC/dados/prata/SRAG/dim_municipio")
df_vacina = spark.read.parquet("file:///home/kuchi/PycharmProjects/TCC/dados/prata/SRAG/dim_vacina")

# Adicionar flag tomou_vacina na dimensão vacina
df_vacina = df_vacina.withColumn(
    "tomou_vacina",
    when((col("DOSE_1_COV") == "1") | (col("DOSE_2_COV") == "1") | (col("VACINA") == "1"), "Sim").otherwise("Não")
)

# Join da fato com dimensões municipio e vacina usando as chaves corretas
df = df_fato.alias("f") \
    .join(df_municipio.select("id_municipio", "nome_municipio", "uf").alias("m"), col("f.id_municipio") == col("m.id_municipio"), "left") \
    .join(df_vacina.select("id_vacina", "tomou_vacina").alias("v"), col("f.id_vacina") == col("v.id_vacina"), "left")

# Filtrar apenas UF RN
df = df.filter(col("m.uf") == "RN")

# Selecionar colunas relevantes e chave primária para garantir unicidade
df = df.select(
    col("f.NU_NOTIFIC"),
    col("m.nome_municipio"),
    col("f.idade"),
    col("f.sexo"),
    col("f.evolucao"),
    col("v.tomou_vacina")
).dropDuplicates(["NU_NOTIFIC"])  # evita duplicidade pela chave primária

# Converter para pandas (limite por performance)
pdf = df.limit(2000).toPandas()

# Exibir no Streamlit
st.title("Casos SRAG no Rio Grande do Norte")
st.write(f"Mostrando {len(pdf)} casos")
st.dataframe(pdf)

spark.stop()
