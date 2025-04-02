from flask import Blueprint
# Crie um Blueprint para as rotas
routes_blueprint = Blueprint('routes', __name__)




# Informando de onde quero importar as rotas
from src.routes.MetaFasesController import MetasFases_routes


# Importacao das rotas para o blueprint:
routes_blueprint.register_blueprint(MetasFases_routes)
