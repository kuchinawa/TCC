from pyspark.sql.functions import col, when, sum


def ouro_principal(df):
    return df.groupBy("SEM_NOT", "SG_UF_NOT", "FAIXA_ETARIA").agg(
        sum(when(col("EVOLUCAO").isin(1, 9), 1).otherwise(0)).alias("casos"),
        sum(when(col("EVOLUCAO").isin(2, 3), 1).otherwise(0)).alias("obitos")
    ).orderBy("SEM_NOT")


def ouro_sintomas_evolucao(df):
    return df.groupBy("SEM_NOT", "SG_UF_NOT", "FAIXA_ETARIA").agg(
        sum(when(col("EVOLUCAO").isin(1, 9), 1).otherwise(0)).alias("casos"),
        sum(when(col("EVOLUCAO").isin(2, 3), 1).otherwise(0)).alias("obitos"),
        sum(col("QTD_SINTOMAS")).alias("sintomas_totais"),
        sum(when(col("DISPNEIA") == 1, 1).otherwise(0)).alias("casos_dispneia"),
        sum(when(col("SATURACAO") == 1, 1).otherwise(0)).alias("casos_sat_baixa")
    )


def ouro_comorbidades_risco(df):
    return df.groupBy("SEM_NOT", "SG_UF_NOT", "FAIXA_ETARIA").agg(
        sum(when(col("QTD_COMORBIDADES") > 0, 1).otherwise(0)).alias("casos_com_comorb"),
        sum(when(col("QTD_COMORBIDADES") == 0, 1).otherwise(0)).alias("casos_sem_comorb"),
        sum(when((col("QTD_COMORBIDADES") > 0) & col("EVOLUCAO").isin(2, 3), 1).otherwise(0)).alias("obitos_com_comorb"),
        sum(when((col("QTD_COMORBIDADES") == 0) & col("EVOLUCAO").isin(2, 3), 1).otherwise(0)).alias("obitos_sem_comorb")
    )


def ouro_internacao_uti(df):
    return df.groupBy("SEM_NOT", "SG_UF_NOT", "FAIXA_ETARIA").agg(
        sum(col("HOSPITALIZADO_BIN")).alias("internacoes"),
        sum(col("UTI_BIN")).alias("casos_uti"),
        sum(col("SUPORT_VEN_BIN")).alias("casos_suporte_ven"),
        sum(when(col("EVOLUCAO").isin(2, 3) & (col("UTI_BIN") == 1), 1).otherwise(0)).alias("obitos_em_uti")
    )

def ouro_testes_classificacao(df):
    return df.groupBy("SEM_NOT", "SG_UF_NOT").agg(
        sum(when(col("PCR_SARS2") == 1, 1).otherwise(0)).alias("confirmados_pcr"),
        sum(when(col("AN_SARS2") == 1, 1).otherwise(0)).alias("confirmados_antigeno"),
    )

def ouro_vacinacao_gravidade(df):
    return df.groupBy("SEM_NOT", "SG_UF_NOT", "FAIXA_ETARIA", "VAC_COV_STATUS").agg(
        sum(when(col("EVOLUCAO").isin(1, 9), 1).otherwise(0)).alias("casos"),
        sum(when(col("EVOLUCAO").isin(2, 3), 1).otherwise(0)).alias("obitos"),
        sum(col("HOSPITALIZADO_BIN")).alias("internacoes"),
        sum(col("UTI_BIN")).alias("casos_uti")
    )


