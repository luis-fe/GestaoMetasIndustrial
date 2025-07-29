from flask import Blueprint
# Crie um Blueprint para as rotas

routes_blueprint = Blueprint('routes', __name__)




# Informando de onde quero importar as rotas
from src.routes.MetaFasesController import MetasFases_routes
from src.routes.FaturamentoController import Faturamento_routes
from src.routes.ProducaoFasesController import ProducaoFases_routes
from src.routes.GastosCentroCusto import  GastosCentroCusto_routes
from src.routes.CronogramaFasesController import cronograma_routes

# Importacao das rotas para o blueprint:
routes_blueprint.register_blueprint(MetasFases_routes)
routes_blueprint.register_blueprint(Faturamento_routes)
routes_blueprint.register_blueprint(ProducaoFases_routes)
routes_blueprint.register_blueprint(GastosCentroCusto_routes)
routes_blueprint.register_blueprint(cronograma_routes)
