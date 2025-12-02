from pyspark.sql import SparkSession
from sparkmeasure import StageMetrics, TaskMetrics

import cru_bronze
import bronze_prata
import prata_ouro
import load
import performance


def salvar_ouro(df_ouro, nome_base):
    caminho_parquet = f"../dados/ouro/{nome_base}.parquet"
    load.save_parquet(df_ouro.coalesce(1), caminho_parquet)


def main():
    spark = SparkSession.builder \
        .appName("ETL_TCC") \
        .master("local[*]") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .getOrCreate()

    stage_metrics = StageMetrics(spark)
    task_metrics = TaskMetrics(spark)
    stage_metrics.begin()
    task_metrics.begin()


    df_bruto = cru_bronze.load_bronze(spark)
    df_silver = bronze_prata.limpar(df_bruto)



    df_gold_principal = prata_ouro.ouro_principal(df_silver)
    salvar_ouro(df_gold_principal, "ouro_principal")


    df_gold_sintomas = prata_ouro.ouro_sintomas_evolucao(df_silver)
    salvar_ouro(df_gold_sintomas, "ouro_sintomas")


    df_gold_comorb = prata_ouro.ouro_comorbidades_risco(df_silver)
    salvar_ouro(df_gold_comorb, "ouro_comorb")


    df_gold_internacao = prata_ouro.ouro_internacao_uti(df_silver)
    salvar_ouro(df_gold_internacao, "ouro_internacao")


    df_gold_testes = prata_ouro.ouro_testes_classificacao(df_silver)
    salvar_ouro(df_gold_testes, "ouro_testes")


    df_gold_vac = prata_ouro.ouro_vacinacao_gravidade(df_silver)
    salvar_ouro(df_gold_vac, "ouro_vacinacao")

    stage_metrics.end()
    task_metrics.end()

    arquivo = "2021-2022-2023-2024-2025-"
    nucleos = "1-"
    driverexecutores = "4"
    exec_num = 5

    performance.save(stage_metrics, "../dados/logs", "stage_metrics" + arquivo + nucleos + driverexecutores, exec_num )
    performance.save(task_metrics, "../dados/logs", "task_metrics" + arquivo + nucleos + driverexecutores, exec_num)


    stage_metrics.print_report()
    task_metrics.print_report()

    spark.stop()

if __name__ == "__main__":
    main()
