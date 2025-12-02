from pyspark.sql.functions import col, when, to_date, year

def limpar(df):

    df = df.withColumn("DT_NOTIFIC_DATE", to_date(col("DT_NOTIFIC")))
    df = df.withColumn("ANO", year(col("DT_NOTIFIC_DATE")))
    df = df.drop("DT_NOTIFIC_DATE")

    df = df.filter((col("NU_IDADE_N") >= 0) & (col("NU_IDADE_N") <= 120))
    df = df.na.drop(subset=["EVOLUCAO", "SG_UF_NOT", "SEM_NOT"])
    df = df.dropDuplicates(["NU_NOTIFIC", "SEM_NOT"])

    df = df.withColumn(
        "FAIXA_ETARIA",
        when((col("NU_IDADE_N") >= 0) & (col("NU_IDADE_N") < 18), "menor")
        .when((col("NU_IDADE_N") >= 18) & (col("NU_IDADE_N") < 60), "adulto")
        .otherwise("idoso")
    )

    df = df.withColumn(
        "QTD_SINTOMAS",
        when(col("FEBRE")      == 1, 1).otherwise(0) +
        when(col("TOSSE")      == 1, 1).otherwise(0) +
        when(col("GARGANTA")   == 1, 1).otherwise(0) +
        when(col("DISPNEIA")   == 1, 1).otherwise(0) +
        when(col("DIARREIA")   == 1, 1).otherwise(0) +
        when(col("VOMITO")     == 1, 1).otherwise(0) +
        when(col("DOR_ABD")    == 1, 1).otherwise(0) +
        when(col("FADIGA")     == 1, 1).otherwise(0) +
        when(col("PERD_OLFT")  == 1, 1).otherwise(0) +
        when(col("PERD_PALA")  == 1, 1).otherwise(0)
    )

    df = df.withColumn(
        "QTD_COMORBIDADES",
        when(col("CARDIOPATI") == 1, 1).otherwise(0) +
        when(col("HEMATOLOGI") == 1, 1).otherwise(0) +
        when(col("SIND_DOWN")  == 1, 1).otherwise(0) +
        when(col("HEPATICA")   == 1, 1).otherwise(0) +
        when(col("ASMA")       == 1, 1).otherwise(0) +
        when(col("DIABETES")   == 1, 1).otherwise(0) +
        when(col("NEUROLOGIC") == 1, 1).otherwise(0) +
        when(col("PNEUMOPATI") == 1, 1).otherwise(0) +
        when(col("IMUNODEPRE") == 1, 1).otherwise(0) +
        when(col("RENAL")      == 1, 1).otherwise(0) +
        when(col("OBESIDADE")  == 1, 1).otherwise(0)
    )

    df = df.withColumn("HOSPITALIZADO_BIN", when(col("HOSPITAL") == 1, 1).otherwise(0))
    df = df.withColumn("UTI_BIN",           when(col("UTI") == 1, 1).otherwise(0))
    df = df.withColumn("SUPORT_VEN_BIN",    when(col("SUPORT_VEN") == 1, 1).otherwise(0))

    df = df.withColumn(
        "VAC_COV_STATUS",
        when(col("VACINA_COV") == 2, "nao_vacinado")
        .when((col("VACINA_COV") == 1) & (col("DOSE_1_COV") == 1) & (col("DOSE_2_COV") != 1), "parcial")
        .when((col("VACINA_COV") == 1) & (col("DOSE_2_COV") == 1) & (col("DOSE_REF") != 1), "completo")
        .when((col("VACINA_COV") == 1) & (col("DOSE_REF") == 1), "reforco")
        .otherwise("ignorado")
    )

    return df
