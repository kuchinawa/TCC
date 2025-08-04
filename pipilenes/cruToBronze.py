from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, when

spark = SparkSession.builder.appName("ETL-SRAG").master("local[*]").getOrCreate()

# Leitura dos arquivos CSV
df_cru = spark.read.option("header", "true").option("delimiter", ";").csv([
    "file:///home/kuchi/PycharmProjects/TCC/dados/cru/SRAG/INFLUD21.csv",
    "file:///home/kuchi/PycharmProjects/TCC/dados/cru/SRAG/INFLUD22.csv",
    "file:///home/kuchi/PycharmProjects/TCC/dados/cru/SRAG/INFLUD23.csv",
    "file:///home/kuchi/PycharmProjects/TCC/dados/cru/SRAG/INFLUD24.csv",
    "file:///home/kuchi/PycharmProjects/TCC/dados/cru/SRAG/INFLUD25.csv"
])

# Padronização de datas
date_fields = ["DT_NOTIFIC", "DT_SIN_PRI", "DT_NASC", "DT_UT_DOSE"]

for field in date_fields:
    if field in df_cru.columns:
        df_cru = df_cru.withColumn(
            field,
            when(col(field).rlike("^(19|20)\\d{2}-\\d{2}-\\d{2}$"), to_date(col(field), "yyyy-MM-dd"))
            .when(col(field).rlike("^\\d{2}/\\d{2}/\\d{4}$"), to_date(col(field), "dd/MM/yyyy"))
            .otherwise(None)
        )

# Salvar como Parquet
df_cru.write.mode("overwrite").parquet("file:///home/kuchi/PycharmProjects/TCC/dados/bronze/SRAG")

spark.stop()
