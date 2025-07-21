import pandas as pd
from flask import Blueprint, jsonify, request
from functools import wraps
from src.models import GastosCentroCusto_CSW
import datetime
import pytz

GastosCentroCusto_routes = Blueprint('GastosCentroCusto_routes', __name__)

def token_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = request.headers.get('Authorization')
        if token == 'a44pcp22':  # Verifica se o token é igual ao token fixo
            return f(*args, **kwargs)
        return jsonify({'message': 'Acesso negado'}), 401

    return decorated_function




@GastosCentroCusto_routes.route('/pcp/api/GastosCentroCusto', methods=['GET'])
@token_required
def get_GastosCentroCusto():

    codEmpresa = request.args.get('codEmpresa', '1')
    dataCompentencia = request.args.get('dataCompentencia', '1')


    dados = GastosCentroCusto_CSW.Gastos_centroCusto_CSW(codEmpresa,dataCompentencia).get_notasEntredas_Csw()


    # Obtém os nomes das colunas
    column_names = dados.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    OP_data = []
    for index, row in dados.iterrows():
        op_dict = {}
        for column_name in column_names:
            op_dict[column_name] = row[column_name]
        OP_data.append(op_dict)
    del dados
    return jsonify(OP_data)