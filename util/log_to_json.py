import re
import json
import sys
from pathlib import Path

def parse_log(path_txt: Path) -> dict:
    dados = {}
    with path_txt.open("r", encoding="utf-8") as f:
        for linha in f:
            linha = linha.strip()
            if "=>" not in linha:
                continue

            chave, valor_bruto = linha.split("=>", 1)
            chave = chave.strip()
            valor_bruto = valor_bruto.strip()

            # remove parte entre parênteses
            valor_sem_par = re.split(r"\s*\(", valor_bruto, maxsplit=1)[0].strip()

            try:
                valor = int(valor_sem_par)
            except ValueError:
                try:
                    valor = float(valor_sem_par.replace(",", "."))
                except ValueError:
                    valor = valor_bruto

            dados[chave] = valor
    return dados

def main():
    # pasta dos logs; ajuste se o script estiver em outro lugar
    pasta_logs = Path("dados/logs")

    for path_txt in pasta_logs.glob("*.txt"):
        dados = parse_log(path_txt)
        path_json = path_txt.with_suffix(".json")  # mesmo nome, extensão .json
        with path_json.open("w", encoding="utf-8") as f_out:
            json.dump(dados, f_out, ensure_ascii=False, indent=2)
        print(f"Convertido: {path_txt.name} -> {path_json.name}")

if __name__ == "__main__":
    main()
