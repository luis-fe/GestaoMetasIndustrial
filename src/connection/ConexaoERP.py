import jaydebeapi
from contextlib import contextmanager
from dotenv import load_dotenv
import os


@contextmanager
def ConexaoInternoMPL():
    """ Gerencia a conexão com o banco de dados usando JayDeBeApi """

    env_path = r"C:\Users\luis.fernando\Desktop\PROJETOS MPL\ModuloGestaoMetasIndustrialMPL\_ambiente.env"
    # Carregar variáveis de ambiente do arquivo .env
    load_dotenv(env_path)

    # Obter valores das variáveis
    caminho_absoluto = os.getenv('CAMINHO')
    user = os.getenv('CSW_USER')
    password = os.getenv('CSW_PASSWORD')
    host = os.getenv('CSW_HOST')

    # Verificar se variáveis foram carregadas corretamente
    if not all([caminho_absoluto, user, password, host]):
        raise ValueError("Erro: Variáveis de ambiente ausentes ou não carregadas corretamente.")

    conn = None  # Inicializa a variável antes do bloco try

    try:
        conn = jaydebeapi.connect(
            'com.intersys.jdbc.CacheDriver',
            f'jdbc:Cache://{host}:1972/CONSISTEM',
            {'user': user, 'password': password},
            f'{caminho_absoluto}/src/connection/CacheDB.jar'
        )
        yield conn
    finally:
        if conn:  # Verifica se a conexão foi estabelecida antes de tentar fechar
            conn.close()



