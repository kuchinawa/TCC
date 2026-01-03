import json
from pathlib import Path
from collections import defaultdict

def media_execucoes(prefixo: str, pasta="dados/logs/json", inicio=1, fim=5):
    pasta_logs = Path(pasta)

    soma = defaultdict(float)
    cont = 0

    for i in range(inicio, fim + 1):
        nome = f"{prefixo}_exec{i:03d}.json"
        path = pasta_logs / nome
        if not path.exists():
            continue

        with path.open("r", encoding="utf-8") as f:
            dados = json.load(f)

        for k, v in dados.items():
            if isinstance(v, (int, float)):
                soma[k] += v
        cont += 1

    if cont == 0:
        return {}

    medias = {k: v / cont for k, v in soma.items()}
    return medias

def main():
    prefixo = "task_metrics2021-2022-2023-2024-2025-16-4"
    pasta = "dados/logs/json"

    medias = media_execucoes(prefixo, pasta=pasta, inicio=1, fim=5)

    if not medias:
        print("Nenhum arquivo encontrado.")
        return

    # nome do arquivo de saída, por exemplo: stage_metrics2021-1-4_media.json
    caminho_saida = Path(pasta) / f"{prefixo}_media.json"

    with caminho_saida.open("w", encoding="utf-8") as f_out:
        json.dump(medias, f_out, ensure_ascii=False, indent=2)

    print(f"Médias salvas em: {caminho_saida}")

if __name__ == "__main__":
    main()
