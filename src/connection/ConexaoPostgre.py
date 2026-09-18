'''Arquivo para chamar a conexao de banco com o postgre'''
import os

import pandas as pd
from dotenv import load_dotenv
import psycopg2
from sqlalchemy import create_engine
from src.configApp import configApp

env_path = configApp.localProjeto
# Carregar variáveis de ambiente do arquivo .env
load_dotenv(f'{env_path}/_ambiente.env')

# ---------------------------------------------------------------------------
# Cache de engines SQLAlchemy.
#
# Antes, cada chamada a conexaoEngine() criava um Engine novo (com seu proprio
# pool de conexoes) e nunca o descartava. Como os models chamam essa funcao a
# cada query, o processo acumulava pools/conexoes abertas e a memoria so
# crescia. Agora existe UM engine por string de conexao, por processo.
#
# O cache e' indexado tambem pelo PID: se o processo for forkado (Gunicorn),
# o filho cria os seus proprios engines em vez de herdar sockets do pai.
# ---------------------------------------------------------------------------
_engines = {}


def _obterEngine(connection_string):
    chave = (os.getpid(), connection_string)
    engine = _engines.get(chave)
    if engine is None:
        engine = create_engine(
            connection_string,
            pool_size=5,          # conexoes mantidas abertas por engine/worker
            max_overflow=5,       # conexoes extras temporarias em pico
            pool_pre_ping=True,   # descarta conexao morta antes de usar
            pool_recycle=1800,    # renova conexoes a cada 30 min
            pool_timeout=60,
        )
        _engines[chave] = engine
    return engine


def _stringConexao(db_name, db_user, db_password, db_host, db_porta):
    if not all([db_name, db_user, db_password, db_host]):
        raise ValueError("One or more environment variables are not set")
    return f"postgresql://{db_user}:{db_password}@{db_host}:{db_porta}/{db_name}"


def conexaoEngine():
    connection_string = _stringConexao(
        os.getenv('POSTGRES_DB'),
        os.getenv('POSTGRES_USER'),
        os.getenv('POSTGRES_PASSWORD_SRV1'),
        os.getenv('POSTGRES_HOST_SRV1'),
        os.getenv('POSTGRES_PORT'),
    )
    return _obterEngine(connection_string)

def conexaoEngineWMSSrv():
    connection_string = _stringConexao(
        os.getenv('POSTGRES_DB2'),
        os.getenv('POSTGRES_USER'),
        os.getenv('POSTGRES_PASSWORD_SRV2'),
        os.getenv('POSTGRES_HOST_SRV2'),
        os.getenv('POSTGRES_PORT'),
    )
    return _obterEngine(connection_string)

def conexaoEngineWms():
    # Mesmo destino de conexaoEngine(); mantido por compatibilidade.
    return conexaoEngine()

def Funcao_InserirOFF (df_tags, tamanho,tabela, metodo):
    # Configurações de conexão ao banco de dados
    db_name = os.getenv('POSTGRES_DB')
    db_user = os.getenv('POSTGRES_USER')
    db_password = os.getenv('POSTGRES_PASSWORD_SRV1')
    db_host = os.getenv('POSTGRES_HOST_SRV1')
    db_porta = os.getenv('POSTGRES_PORT')


# Cria conexão ao banco de dados usando SQLAlchemy
    engine = create_engine(f'postgresql://{db_user}:{db_password}@{db_host}:{db_porta}/{db_name}')

    # Inserir dados em lotes
    chunksize = tamanho
    try:
        for i in range(0, len(df_tags), chunksize):
            df_tags.iloc[i:i + chunksize].to_sql(tabela, engine, if_exists=metodo, index=False , schema='pcp')
    finally:
        engine.dispose()

def Funcao_InserirOFF_srvWMS (df_tags, tamanho,tabela, metodo):
    # Configurações de conexão ao banco de dados
    db_name = os.getenv('POSTGRES_DB')
    db_user = os.getenv('POSTGRES_USER')
    db_password = os.getenv('POSTGRES_PASSWORD_SRV2')
    db_host = os.getenv('POSTGRES_HOST_SRV2')
    db_porta = os.getenv('POSTGRES_PORT')

# Cria conexão ao banco de dados usando SQLAlchemy
    engine = create_engine(f'postgresql://{db_user}:{db_password}@{db_host}:{db_porta}/{db_name}')

    # Inserir dados em lotes
    chunksize = tamanho
    try:
        for i in range(0, len(df_tags), chunksize):
            df_tags.iloc[i:i + chunksize].to_sql(tabela, engine, if_exists=metodo, index=False , schema='pcp')
    finally:
        engine.dispose()

def conexaoInsercao():
    db_name = os.getenv('POSTGRES_DB')
    db_user = os.getenv('POSTGRES_USER')
    db_password = os.getenv('POSTGRES_PASSWORD_SRV1')
    db_host = os.getenv('POSTGRES_HOST_SRV1')
    db_porta = os.getenv('POSTGRES_PORT')

    return psycopg2.connect(dbname=db_name, user=db_user, password=db_password, host=db_host, port=db_porta)




