"""
Configuracao do Gunicorn para o microservico Gestao de Metas Industriais.

Uso (na raiz do projeto):
    gunicorn -c gunicorn.conf.py app_run:app

Variaveis de ambiente opcionais (podem ir no _ambiente.env):
    PORT / PORTA_APLICACAO   porta HTTP (PORT tem prioridade)
    GUNICORN_WORKERS         qtd de processos worker (padrao 2)
    GUNICORN_TIMEOUT         tempo maximo de uma requisicao em segundos (padrao 900)
    GUNICORN_MAX_REQUESTS    requisicoes atendidas antes do worker ser reciclado (padrao 200)
"""
import os
from dotenv import load_dotenv

# Raiz do projeto = pasta onde este arquivo esta.
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, '_ambiente.env'))

# O driver JDBC e' carregado por caminho relativo (./src/connection/CacheDB.jar)
# e os CSVs congelados ficam em ./dados, portanto o cwd precisa ser a raiz.
chdir = BASE_DIR

bind = f"0.0.0.0:{os.getenv('PORT', os.getenv('PORTA_APLICACAO', '5000'))}"

# Processos worker. Cada worker sobe a sua propria JVM (JPype/JayDeBeApi),
# entao mais workers = mais memoria fixa. 2 e' um bom ponto de partida.
workers = int(os.getenv('GUNICORN_WORKERS', '2'))
worker_class = 'sync'
threads = 1

# As consultas de metas sao demoradas: timeout alto para nao matar o worker
# no meio de uma consulta legitima.
timeout = int(os.getenv('GUNICORN_TIMEOUT', '900'))
graceful_timeout = 60
keepalive = 5

# Reciclagem de workers: devolve ao SO a memoria que o pandas/JVM retem
# entre requisicoes. O jitter evita que todos reiniciem ao mesmo tempo.
max_requests = int(os.getenv('GUNICORN_MAX_REQUESTS', '200'))
max_requests_jitter = 50

# NAO usar preload: a JVM iniciada pelo JPype nao sobrevive ao fork.
# Cada worker importa a aplicacao (e inicia a JVM) por conta propria.
preload_app = False

# Logs para stdout/stderr (capturados pelo systemd / docker).
accesslog = '-'
errorlog = '-'
loglevel = os.getenv('GUNICORN_LOGLEVEL', 'info')
capture_output = True
