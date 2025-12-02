def load_bronze(spark):
    df = spark.read.parquet(
        "../dados/bronze/INFLUD21.parquet",
        "../dados/bronze/INFLUD22.parquet",
        "../dados/bronze/INFLUD23.parquet",
        "../dados/bronze/INFLUD24.parquet",
        "../dados/bronze/INFLUD25.parquet"
    )
    return df
