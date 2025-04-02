from flask import Flask
import os

from src.configApp import configApp
from src.routes import routes_blueprint  # Certifique-se de que 'routes.py' está na mesma pasta
from dotenv import load_dotenv

app = Flask(__name__)
port = int(os.environ.get('PORT', 5000))

# Registrar o Blueprint corretamente
app.register_blueprint(routes_blueprint)

if __name__ == '__main__':

    env_path = configApp.localProjeto
    # Carregar variáveis de ambiente do arquivo .env
    load_dotenv(f'{env_path}/_ambiente.env')
    db_name = os.getenv('POSTGRES_DB')

    print(db_name)
    app.run(host='0.0.0.0', port=port)
