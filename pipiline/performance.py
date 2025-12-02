import sys
import os

def save(obj_metricas, diretorio, prefixo, numero_execucao):

    os.makedirs(diretorio, exist_ok=True)
    nome_arquivo = f"{prefixo}_exec{numero_execucao:03d}.txt"
    caminho_arquivo = os.path.join(diretorio, nome_arquivo)

    stdout_original = sys.stdout
    try:
        with open(caminho_arquivo, "w", encoding="utf-8") as f:
            sys.stdout = f
            obj_metricas.print_report()
    finally:
        sys.stdout = stdout_original
