import streamlit as st
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

spark = SparkSession.builder.appName("Dashboard_RN").getOrCreate()

# Leitura das tabelas
df_fato = spark.read.parquet("file:///home/kuchi/PycharmProjects/TCC/dados/prata/SRAG/fato_notificacao")
df_municipio = spark.read.parquet("file:///home/kuchi/PycharmProjects/TCC/dados/prata/SRAG/dim_municipio")
df_vacina = spark.read.parquet("file:///home/kuchi/PycharmProjects/TCC/dados/prata/SRAG/dim_vacina")

# Adiciona flag tomou_vacina
df_vacina = df_vacina.withColumn(
    "tomou_vacina",
    when((col("DOSE_1_COV") == "1") | (col("DOSE_2_COV") == "1") | (col("VACINA") == "1"), "Sim").otherwise("Não")
)

# Join com município e vacina
df = df_fato.join(df_municipio.select("id_municipio", "nome_municipio", "uf"), "id_municipio", "left") \
            .join(df_vacina.select("id_vacina", "tomou_vacina"), "id_vacina", "left") \
            .filter(col("uf") == "RN")

# Seleção de colunas
df = df.select("nome_municipio", "idade", "sexo", "evolucao", "tomou_vacina")

# Converter para pandas (limite por performance)
pdf = df.limit(2000).toPandas()

# Mostrar no Streamlit
st.title("Casos SRAG no Rio Grande do Norte")
st.write(f"Mostrando {len(pdf)} casos")
st.dataframe(pdf)

spark.stop()
