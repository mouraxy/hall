'''
Criado por: Felipe Paskevicius
Data de criação: 11 de dezembro de 2023
'''

# Importar bibliotecas...
import os
import pandas as pd
from datetime import datetime

DIRETORIO = 'C:/User/'
CHAVE = 'perf'

def obter_metadados(caminho):
    '''
    retorna um dicionário contendo os metadados do arquivo
    Parametros:
    caminho:str, caminho completo do arquivo.
    '''
    data_modificacao = datetime.fromtimestamp(os.path.getmtime(caminho))
    return {
        'arquivo': os.path.basename(caminho), 
        'data_modificacao': data_modificacao,
        'dia_modificacao': data_modificacao.strftime('%A'), 
        'hora_modificacao': data_modificacao.time(),
        'hora_decimal': data_modificacao.hour + data_modificacao.minute / 60
    }

def exibir_arquivos(diretorio, chave):
    '''
    retorna um dataframe contendo os metadados dos arquivos
    Parametros:
    diretorio:str, diretório onde os arquivos serão procurados.
    chave:str, palavra-chave para filtrar os nomes dos arquivos.
    '''
    arquivos = [arquivo for arquivo in os.listdir(diretorio) if chave in arquivo]
    metadados = [obter_metadados(os.path.join(diretorio, arquivo)) for arquivo in arquivos]
    return pd.DataFrame(metadados).sort_values(by='data_modificacao')

df_metadados = exibir_arquivos(DIRETORIO, CHAVE)
df_metadados.to_parquet(f'C:/Users/{os.getenv("USERNAME")}/Downloads/metadados_arquivos.parquet', index=False)
print('Relatório criado com sucesso!')