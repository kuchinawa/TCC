def save_parquet(df, caminho, modo="overwrite"):
    df.write.mode(modo).parquet(caminho)


def save_csv(df, caminho, modo="overwrite", header=True):
    df.write.mode(modo).option("header", str(header).lower()).csv(caminho)
