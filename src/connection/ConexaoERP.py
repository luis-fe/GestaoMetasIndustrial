import jaydebeapi
from contextlib import contextmanager
from dotenv import load_dotenv, dotenv_values
import os


@contextmanager
def ConexaoInternoMPL():
    conn = None
    load_dotenv("C:\Users\luis.fernando\Desktop\PROJETOS MPL\ModuloGestaoMetasIndustrialMPL\ambiente.env")
    user = os.getenv('CSW_USER')
    password = os.getenv('CSW_PASSWORD')
    host = os.getenv('CSW_HOST')

    try:
        conn = jaydebeapi.connect(
            'com.intersys.jdbc.CacheDriver',
            f'jdbc:Cache://{host}:1972/CONSISTEM',
            {'user': f'{user}', 'password': f'{password}'},
            f'{caminhoAbsoluto}/src/connection/CacheDB.jar'
        )
        yield conn
    finally:
        if conn is not None:
            conn.close()


load_dotenv('./var_ambiente.env')
host = os.getenv('CSW_HOST')

print(host)