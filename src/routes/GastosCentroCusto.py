import pandas as pd
from flask import Blueprint, jsonify, request
from functools import wraps
from src.models import GastosCentroCusto_CSW, GastosOrçamentoBI
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
    dataCompentenciaInicial = request.args.get('dataCompentenciaInicial', '1')
    dataCompentenciaFinal = request.args.get('dataCompentenciaFinal', '1')
    nomeCentroCusto = request.args.get('nomeCentroCusto', '')
    GRUPO = request.args.get('GRUPO', '')


    dados = GastosCentroCusto_CSW.Gastos_centroCusto_CSW(codEmpresa,dataCompentenciaInicial,dataCompentenciaFinal,'','','','','','','','','','',nomeCentroCusto,'',GRUPO).get_notasEntredas_Csw()


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


@GastosCentroCusto_routes.route('/pcp/api/ResumooGastosCentroCusto', methods=['GET'])
@token_required
def get_ResumooGastosCentroCustoo():

    codEmpresa = request.args.get('codEmpresa', '1')
    dataCompentenciaInicial = request.args.get('dataCompentenciaInicial', '2025-07-01')
    dataCompentenciaFinal = request.args.get('dataCompentenciaFinal', '2025-07-31')
    nomeArea = request.args.get('nomeArea', '')
    GRUPO = request.args.get('GRUPO', '')


    dados = GastosCentroCusto_CSW.Gastos_centroCusto_CSW(codEmpresa,dataCompentenciaInicial,dataCompentenciaFinal,'','','','','','','','','','','',nomeArea, GRUPO).resumo_centroCusto()


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


@GastosCentroCusto_routes.route('/pcp/api/ResumooGastosCentroCustoConta', methods=['GET'])
@token_required
def get_ResumooGastosCentroCustooConta():

    codEmpresa = request.args.get('codEmpresa', '1')
    dataCompentenciaInicial = request.args.get('dataCompentenciaInicial', '2025-07-01')
    dataCompentenciaFinal = request.args.get('dataCompentenciaFinal', '2025-07-31')
    nomeArea = request.args.get('nomeArea', '')
    GRUPO = request.args.get('GRUPO', '')

    nomeCentroCusto = request.args.get('nomeCentroCusto', '')

    dados = GastosCentroCusto_CSW.Gastos_centroCusto_CSW(codEmpresa,dataCompentenciaInicial,dataCompentenciaFinal,'','','','','','','','','','',nomeCentroCusto,nomeArea, GRUPO).resumo_contacontabil()


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





@GastosCentroCusto_routes.route('/pcp/api/CentroCustos', methods=['GET'])
@token_required
def get_CentroCustos():


    dados = GastosCentroCusto_CSW.Gastos_centroCusto_CSW().get_centro_custo()


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


@GastosCentroCusto_routes.route('/pcp/api/EmpresasGrupoMPL', methods=['GET'])
@token_required
def get_EmpresasGrupoMPL():


    dados = GastosCentroCusto_CSW.Gastos_centroCusto_CSW().get_Empresa()


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



@GastosCentroCusto_routes.route('/pcp/api/AreaCusto', methods=['GET'])
@token_required
def get_AreaCusto():


    dados = GastosCentroCusto_CSW.Gastos_centroCusto_CSW().get_area()


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


@GastosCentroCusto_routes.route('/pcp/api/GrupoGastos', methods=['GET'])
@token_required
def get_GrupoGastos():


    dados = GastosCentroCusto_CSW.Gastos_centroCusto_CSW().get_GrupoContas()


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



@GastosCentroCusto_routes.route('/pcp/api/OrcamentoGastosBI', methods=['GET'])
@token_required
def get_OrcamentoGastosBI():


    codEmpresa = request.args.get('codEmpresa', '1')
    dataInicial = request.args.get('dataInicial', '1')
    dataFinal = request.args.get('dataFinal', '1')


    dados = GastosOrçamentoBI.GastosOrcamentoBI(codEmpresa, dataInicial, dataFinal)


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