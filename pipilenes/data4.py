import streamlit as st
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, sum as spark_sum
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

spark = SparkSession.builder.appName("Vacina_Sexo_Nordeste").getOrCreate()

# Leitura dados
df_fato = spark.read.parquet("file:///home/kuchi/PycharmProjects/TCC/dados/prata/SRAG/fato_notificacao")
df_municipio = spark.read.parquet("file:///home/kuchi/PycharmProjects/TCC/dados/prata/SRAG/dim_municipio")
df_vacina = spark.read.parquet("file:///home/kuchi/PycharmProjects/TCC/dados/prata/SRAG/dim_vacina")

# Estados nordeste com nome completo
estados_ne = {
    "MA": "Maranhão",
    "PI": "Piauí",
    "CE": "Ceará",
    "RN": "Rio Grande do Norte",
    "PB": "Paraíba",
    "PE": "Pernambuco",
    "AL": "Alagoas",
    "SE": "Sergipe",
    "BA": "Bahia"
}

# Adicionar flag tomou vacina (qualquer das 3)
df_vacina = df_vacina.withColumn(
    "tomou_vacina",
    when((col("DOSE_1_COV") == "1") | (col("DOSE_2_COV") == "1") | (col("VACINA") == "1"), 1).otherwise(0)
)

# Join
df = df_fato.join(df_municipio.select("id_municipio", "uf"), "id_municipio", "left") \
            .join(df_vacina.select("id_vacina", "tomou_vacina"), "id_vacina", "left")

# Filtrar nordeste
df = df.filter(col("uf").isin(list(estados_ne.keys())))

# Agrupar por estado e sexo, somando vacinados
df_agg = df.groupBy("uf", "sexo").agg(spark_sum("tomou_vacina").alias("total_vacinados"))

# Converter sexo M,F,I para nomes
df_agg = df_agg.withColumn(
    "sexo_desc",
    when(col("sexo") == "M", "Masculino")
    .when(col("sexo") == "F", "Feminino")
    .otherwise("Indefinido")
)

# Converter para pandas
pdf = df_agg.toPandas()

# Mapear siglas dos estados para nome completo
pdf["estado_nome"] = pdf["uf"].map(estados_ne)

# Remover indefinido se quiser
pdf = pdf[pdf["sexo_desc"].isin(["Masculino", "Feminino"])]

# Ordenar estados para visualização
ufs_order = list(estados_ne.keys())

# Preparar subplots: 3 colunas e 3 linhas para 9 estados
fig = make_subplots(rows=3, cols=3,
                    specs=[[{'type':'domain'}]*3]*3,
                    subplot_titles=[estados_ne[uf] for uf in ufs_order])

import plotly.graph_objects as go

for i, uf in enumerate(ufs_order):
    row = i // 3 + 1
    col = i % 3 + 1
    df_estado = pdf[pdf["uf"] == uf]

    # Garantir ordem fixada para cor e label
    df_estado = df_estado.set_index("sexo_desc").reindex(["Masculino", "Feminino"]).reset_index()
    valores = df_estado["total_vacinados"].fillna(0).tolist()
    labels = ["Masculino", "Feminino"]
    cores = ['blue', 'pink']  # Azul para masculino, rosa para feminino

    fig.add_trace(go.Pie(labels=labels, values=valores, marker_colors=cores, showlegend=(i==0)),
                  row=row, col=col)

fig.update_layout(
    title_text="Vacinação por Sexo nos Estados do Nordeste",
    height=500,
    margin=dict(t=50, b=20, l=20, r=20)
)

st.plotly_chart(fig, use_container_width=True)

spark.stop()
