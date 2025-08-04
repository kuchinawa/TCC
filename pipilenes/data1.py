import streamlit as st
import plotly.express as px
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
import pandas as pd

spark = SparkSession.builder.appName("Dashboard SRAG").getOrCreate()

# Leitura dados prata
fato = spark.read.parquet("file:///home/kuchi/PycharmProjects/TCC/dados/prata/SRAG/fato_notificacao")
dim_municipio = spark.read.parquet("file:///home/kuchi/PycharmProjects/TCC/dados/prata/SRAG/dim_municipio")

# Mapeia sexo
fato = fato.withColumn(
    "sexo",
    when(col("sexo") == "M", "Masculino")
    .when(col("sexo") == "F", "Feminino")
    .otherwise("Ignorado")
)
fato = fato.filter(col("sexo").isin("Masculino", "Feminino"))

# Junta e agrega dados por UF e sexo
dados_uf_genero = (
    fato.join(dim_municipio, "id_municipio", "left")
    .groupBy("uf", "sexo")
    .count()
    .filter(col("uf").isNotNull())
)

pdf = dados_uf_genero.toPandas()

st.set_page_config(layout="wide")
st.title("Casos SRAG por UF e Gênero")

genero = st.selectbox("Selecione o gênero:", ["Masculino", "Feminino"])

pdf_filtrado = pdf[pdf["sexo"] == genero]

fig = px.choropleth(
    pdf_filtrado,
    geojson="https://raw.githubusercontent.com/codeforamerica/click_that_hood/master/public/data/brazil-states.geojson",
    locations="uf",
    featureidkey="properties.sigla",
    color="count",
    color_continuous_scale="Viridis",
    scope="south america",
    title=f"Número de Casos por Estado ({genero})"
)

fig.update_geos(fitbounds="locations", visible=False)
fig.update_layout(margin={"r":0,"t":30,"l":0,"b":0})

st.plotly_chart(fig, use_container_width=True)
