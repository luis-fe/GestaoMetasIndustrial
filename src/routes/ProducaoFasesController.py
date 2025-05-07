import pandas as pd
from flask import Blueprint, jsonify, request
from functools import wraps
from src.models import MetaFases, ProducaoFases
import datetime
import pytz

ProducaoFases_routes = Blueprint('ProducaoFases_routes', __name__)

def token_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = request.headers.get('Authorization')
        if token == 'a44pcp22':  # Verifica se o token é igual ao token fixo
            return f(*args, **kwargs)
        return jsonify({'message': 'Acesso negado'}), 401

    return decorated_function

def dayAtual():
    fuso_horario = pytz.timezone('America/Sao_Paulo')
    agora = datetime.datetime.now(fuso_horario)
    day = agora.strftime('%Y-%m-%d')
    return day


@ProducaoFases_routes.route('/pcp/api/RetornoPorFaseDiaria', methods=['GET'])
@token_required
def get_RetornoPorFaseDiaria():

    nomeFase = request.args.get('nomeFase')
    dataInicio = request.args.get('dataInicio')
    dataFinal = request.args.get('dataFinal')
    codEmpresa = request.args.get('codEmpresa','1')
    print(dataInicio)
    print(dataFinal)

    realizado = ProducaoFases.ProducaoFases(dataInicio, dataFinal, '','',codEmpresa,'','',None,'sim',nomeFase)
    dados = realizado.realizadoFasePeriodoFase()


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


@ProducaoFases_routes.route('/pcp/api/realizadoFasePeriodoFase_detalhaDia', methods=['GET'])
@token_required
def get_realizadoFasePeriodoFase_detalhaDia():

    nomeFase = request.args.get('nomeFase')
    dataInicio = request.args.get('dataInicio')
    codEmpresa = request.args.get('codEmpresa','1')
    print(dataInicio)

    realizado = ProducaoFases.ProducaoFases(dataInicio, dataInicio, '','',codEmpresa,'','',None,'sim',nomeFase)
    dados = realizado.realizadoFasePeriodoFase_detalhaDia()


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