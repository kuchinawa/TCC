import streamlit as st
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
import plotly.express as px

spark = SparkSession.builder.appName("Vacina_Sexo").getOrCreate()

# Leitura dos dados
df_fato = spark.read.parquet("file:///home/kuchi/PycharmProjects/TCC/dados/prata/SRAG/fato_notificacao")
df_vacina = spark.read.parquet("file:///home/kuchi/PycharmProjects/TCC/dados/prata/SRAG/dim_vacina")

# Marca quem tomou vacina (qualquer das 3 doses/vacinas)
df_vacina = df_vacina.withColumn(
    "tomou_vacina",
    when((col("DOSE_1_COV") == "1") | (col("DOSE_2_COV") == "1") | (col("VACINA") == "1"), "Sim").otherwise("Não")
)

# Juntar fato com vacina
df = df_fato.join(df_vacina.select("id_vacina", "tomou_vacina"), "id_vacina", "left")

# Substituir sexo por nome completo
df = df.withColumn(
    "sexo_desc",
    when(col("sexo") == "M", "Masculino")
    .when(col("sexo") == "F", "Feminino")
    .otherwise("Indefinido")
)

# Filtrar só vacinados e sexo definido
df_vacinados = df.filter((col("tomou_vacina") == "Sim") & (col("sexo_desc") != "Indefinido"))

# Agrupar por sexo e contar únicos (usar NU_NOTIFIC pra contar sem duplicados)
df_contagem = df_vacinados.groupBy("sexo_desc").agg({"NU_NOTIFIC": "count"}).withColumnRenamed("count(NU_NOTIFIC)", "total_vacinados")

# Converter para pandas
pdf = df_contagem.toPandas()

fig = px.bar(pdf, x="sexo_desc", y="total_vacinados", color="sexo_desc",
             color_discrete_map={"Masculino": "blue", "Feminino": "pink"},
             labels={"sexo_desc": "Sexo", "total_vacinados": "Total Vacinação"},
             title="Total de Pessoas Vacinadas por Sexo")


st.title("Vacinação por Sexo - SRAG")
st.plotly_chart(fig, use_container_width=True)

spark.stop()
