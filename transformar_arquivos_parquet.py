'''
Criado por: Felipe Paskevicius
Data de criação: 12 de dezembro de 2023
'''

import os
from pathlib import Path
import chardet
import pandas as pd
from tkinter import filedialog
from tkinter import Tk

def obter_diretorio():
    '''
    Seleciona um diretório com os arquivos CSV
    '''
    root = Tk()
    root.withdraw()
    diretorio = filedialog.askdirectory(title="Selecione a pasta...")

    if diretorio:
        return Path(diretorio)
    else:
        print("Nenhum diretório selecionado. Encerrando.")
        return None

def detectar_codificacao(arquivo, num_bytes=1024):
    '''
    Detecta a codificação de um arquivo de texto.
    Parâmetros:
    arquivo: str, caminho para o arquivo.
    '''
    with open(arquivo, 'rb') as f:
        result = chardet.detect(f.read(num_bytes))
    return result['encoding']

def agrupar_csv_parquet(diretorio_csv, arquivo_saida_parquet):
    '''
    Agrupa todos os arquivos CSV de um diretório em um único arquivo Parquet.
    Parâmetros:
    diretorio_csv: str, diretório onde os arquivos CSV estão localizados.
    arquivo_saida_parquet: str, caminho para o arquivo Parquet de saída.
    '''
    dfs = [
        pd.read_csv(
            arquivo,
            delimiter=';',
            encoding=detectar_codificacao(arquivo),
            on_bad_lines='skip',
            #header=None,
            low_memory=False
        )
        for arquivo in diretorio_csv.glob("*.csv")
    ]
    
    dataframe_final = pd.concat(dfs, ignore_index=True)
    df_novo = pd.DataFrame(dataframe_final.values[1:], columns=dataframe_final.iloc[0])
    df_novo.to_parquet(arquivo_saida_parquet, index=False)

if __name__ == "__main__":
    diretorio_csv = obter_diretorio()
    arquivo_saida_parquet = diretorio_csv.parent / 'historico_entregas.parquet'
    agrupar_csv_parquet(diretorio_csv, arquivo_saida_parquet)
    
    print('Arquivo parquet gerado com sucesso!')