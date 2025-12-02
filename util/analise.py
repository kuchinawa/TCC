import re
import json
import sys
from pathlib import Path

def parse_log(path_txt: str) -> dict:
    path = Path(path_txt)
    dados = {}

    with path.open("r", encoding="utf-8") as f:
        for linha in f:
            linha = linha.strip()
            # ignora linhas vazias ou sem "=>"
            if "=>" not in linha:
                continue

            chave, valor_bruto = linha.split("=>", 1)
            chave = chave.strip()
            valor_bruto = valor_bruto.strip()

            # tira a parte entre parênteses, se existir
            # ex: "34311 (34 s)" -> "34311"
            valor_sem_par = re.split(r"\s*\(", valor_bruto, maxsplit=1)[0].strip()

            # tenta converter para int, senão mantém como string
            try:
                valor = int(valor_sem_par)
            except ValueError:
                try:
                    valor = float(valor_sem_par.replace(",", "."))
                except ValueError:
                    valor = valor_bruto  # fica texto original

            dados[chave] = valor

    return dados

def main():
    if len(sys.argv) < 3:
        print("Uso: python log_to_json.py entrada.txt saida.json")
        sys.exit(1)

    entrada = sys.argv[1]
    saida = sys.argv[2]

    dados = parse_log(entrada)

    with open(saida, "w", encoding="utf-8") as f_out:
        json.dump(dados, f_out, ensure_ascii=False, indent=2)

    print(f"JSON salvo em: {saida}")

if __name__ == "__main__":
    main()
