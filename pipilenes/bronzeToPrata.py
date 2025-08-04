from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, dayofmonth, weekofyear, quarter, concat_ws, when, expr, broadcast, sha1, row_number
from pyspark.sql.types import IntegerType, DateType
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("ETL_SRAG_Bronze_to_Silver") \
    .master("local[*]") \
    .getOrCreate()

try:
    df_srag_bronze = spark.read.parquet("file:///home/kuchi/PycharmProjects/TCC/dados/bronze/SRAG/")
    print(f"Leitura bronze concluída. Registros: {df_srag_bronze.count()}")
except Exception as e:
    print(f"Erro na leitura: {str(e)}")
    spark.stop()
    exit(1)

sintomas_cols = ["FEBRE", "TOSSE", "GARGANTA", "DISPNEIA", "DESC_RESP", "SATURACAO", "DIARREIA", "VOMITO"]
vacina_cols = ["DOSE_1_COV", "DOSE_2_COV", "VACINA"]

try:
    df_srag_bronze = (df_srag_bronze
                      .withColumn("DT_SIN_PRI", col("DT_SIN_PRI").cast(DateType()))
                      .withColumn("DT_NOTIFIC", col("DT_NOTIFIC").cast(DateType()))
                      .withColumn("DT_NASC", col("DT_NASC").cast(DateType()))
                      .withColumn("NU_IDADE_N", col("NU_IDADE_N").cast(IntegerType()))
                      )

    for c in sintomas_cols:
        df_srag_bronze = df_srag_bronze.withColumn(
            c,
            when(col(c).isin("1", "2"), col(c)).otherwise("9")
        )

    df_srag_bronze = df_srag_bronze.withColumn(
        "CS_GESTANT_DESC",
        when(col("CS_GESTANT") == "1", "1º Trimestre")
        .when(col("CS_GESTANT") == "2", "2º Trimestre")
        .when(col("CS_GESTANT") == "3", "3º Trimestre")
        .when(col("CS_GESTANT") == "4", "Idade Gestacional Ignorada")
        .when(col("CS_GESTANT") == "5", "Não")
        .when(col("CS_GESTANT") == "6", "Não se aplica")
        .otherwise("Ignorado")
    )

    df_srag_bronze = df_srag_bronze.withColumn(
        "EVOLUCAO_DESC",
        when(col("EVOLUCAO") == "1", "Cura")
        .when(col("EVOLUCAO") == "2", "Óbito")
        .when(col("EVOLUCAO") == "3", "Óbito por outras causas")
        .otherwise("Ignorado")
    )

    df_srag_bronze = df_srag_bronze.withColumn(
        "id_sintomas",
        sha1(concat_ws("", *sintomas_cols))
    ).withColumn(
        "id_vacina",
        sha1(concat_ws("", *vacina_cols))
    )

except Exception as e:
    print(f"Erro no tratamento inicial: {str(e)}")
    spark.stop()
    exit(1)

# dim_tempo
try:
    df_dim_tempo = (df_srag_bronze.select("DT_SIN_PRI").distinct()
                    .withColumnRenamed("DT_SIN_PRI", "data")
                    .withColumn("ano", year("data"))
                    .withColumn("mes", month("data"))
                    .withColumn("dia", dayofmonth("data"))
                    .withColumn("semana_epi", weekofyear("data"))
                    .withColumn("trimestre", quarter("data"))
                    )

    window = Window.orderBy("data")
    df_dim_tempo = df_dim_tempo.withColumn("id_tempo", row_number().over(window))
    print(f"Dim Tempo: {df_dim_tempo.count()} registros")
except Exception as e:
    print(f"Erro dim_tempo: {str(e)}")

# dim_municipio
try:
    df_dim_municipio = (df_srag_bronze.select("CO_MUN_RES", "SG_UF", "ID_MN_RESI")
                        .distinct()
                        .withColumnRenamed("CO_MUN_RES", "cod_municipio")
                        .withColumnRenamed("SG_UF", "uf")
                        .withColumnRenamed("ID_MN_RESI", "nome_municipio")
                        .filter(col("cod_municipio").isNotNull())
                        )
    window = Window.orderBy("cod_municipio")
    df_dim_municipio = df_dim_municipio.withColumn("id_municipio", row_number().over(window))
    print(f"Dim Municipio: {df_dim_municipio.count()} registros")
except Exception as e:
    print(f"Erro dim_municipio: {str(e)}")

# dim_faixa_etaria
try:
    faixa_etaria_data = [
        (1, 0, 14, "Criança"),
        (2, 15, 24, "Jovem"),
        (3, 25, 64, "Adulto"),
        (4, 65, 999, "Idoso"),
        (9, -1, -1, "Não Informado")
    ]
    df_dim_faixa_etaria = spark.createDataFrame(
        faixa_etaria_data,
        ["id_faixa_etaria", "idade_min", "idade_max", "faixa"]
    )
    print(f"Dim Faixa Etária: {df_dim_faixa_etaria.count()} registros")
except Exception as e:
    print(f"Erro dim_faixa_etaria: {str(e)}")

# dim_sintomas
try:
    df_dim_sintomas = df_srag_bronze.select("id_sintomas", *sintomas_cols).distinct()

    sintomas_expr = "+".join([f"CAST({c} = '1' AS INT)" for c in sintomas_cols])
    df_dim_sintomas = df_dim_sintomas.withColumn("qtde_sintomas", expr(sintomas_expr))

    for c in sintomas_cols:
        df_dim_sintomas = df_dim_sintomas.withColumnRenamed(c, c.lower())

    print(f"Dim Sintomas: {df_dim_sintomas.count()} registros")
except Exception as e:
    print(f"Erro dim_sintomas: {str(e)}")

# dim_vacina
try:
    df_dim_vacina = df_srag_bronze.select("id_vacina", *vacina_cols).distinct()
    print(f"Dim Vacina: {df_dim_vacina.count()} registros")
except Exception as e:
    print(f"Erro dim_vacina: {str(e)}")


try:
    df_fato = df_srag_bronze.alias("fato")

    df_fato = df_fato.join(
        broadcast(df_dim_faixa_etaria),
        expr("""
            (fato.NU_IDADE_N BETWEEN idade_min AND idade_max) OR
            (fato.NU_IDADE_N IS NULL AND id_faixa_etaria = 9)
        """),
        "left"
    )


    df_fato = df_fato.join(
        df_dim_tempo,
        df_fato.DT_SIN_PRI == df_dim_tempo.data,
        "left"
    ).join(
        df_dim_municipio,
        df_fato.CO_MUN_RES == df_dim_municipio.cod_municipio,
        "left"
    ).join(
        df_dim_sintomas,
        "id_sintomas",
        "left"
    ).join(
        df_dim_vacina,
        "id_vacina",
        "left"
    )

    df_fato = df_fato.select(
        col("NU_NOTIFIC"),
        sha1(concat_ws("_",
                       col("id_tempo").cast("string"),
                       col("id_municipio").cast("string"),
                       col("id_faixa_etaria").cast("string"),
                       col("id_sintomas").cast("string"),
                       col("id_vacina").cast("string"))).alias("id_fato"),
        col("id_tempo"),
        col("id_municipio"),
        col("id_faixa_etaria"),
        col("id_sintomas"),
        col("id_vacina"),
        col("CS_SEXO").alias("sexo"),
        col("NU_IDADE_N").alias("idade"),
        col("CS_GESTANT_DESC").alias("gestante"),
        col("EVOLUCAO_DESC").alias("evolucao")
    )

    df_fato = df_fato.coalesce(10)
    print(f"Fato preparado. Registros: {df_fato.count()}")
except Exception as e:
    print(f"Erro construção fato: {str(e)}")
    import traceback
    traceback.print_exc()
    spark.stop()
    exit(1)


base_path = "file:///home/kuchi/PycharmProjects/TCC/dados/prata/SRAG/"

try:
    df_dim_tempo.coalesce(1).write.mode("overwrite").parquet(base_path + "dim_tempo")
    df_dim_municipio.coalesce(1).write.mode("overwrite").parquet(base_path + "dim_municipio")
    df_dim_faixa_etaria.coalesce(1).write.mode("overwrite").parquet(base_path + "dim_faixa_etaria")
    df_dim_sintomas.coalesce(1).write.mode("overwrite").parquet(base_path + "dim_sintomas")
    df_dim_vacina.coalesce(1).write.mode("overwrite").parquet(base_path + "dim_vacina")

    df_fato.write.option("maxRecordsPerFile", 500000).mode("overwrite").parquet(base_path + "fato_notificacao")

    print("Escrita concluída com sucesso")

except Exception as e:
    print(f"Erro na escrita: {str(e)}")
    import traceback
    traceback.print_exc()
finally:
    spark.stop()

print("ETL Prata concluído com sucesso.")
