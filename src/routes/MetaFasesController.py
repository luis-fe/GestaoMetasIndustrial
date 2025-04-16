import pandas as pd
from flask import Blueprint, jsonify, request
from functools import wraps
from src.models import MetaFases
import datetime
import pytz

MetasFases_routes = Blueprint('MetasFases_routes', __name__)

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

@MetasFases_routes.route('/pcp/api/MetasFases', methods=['POST'])
@token_required
def pOST_MetasFases():

    data = request.get_json()
    dia = dayAtual()
    codigoPlano = data.get('codigoPlano')
    arrayCodLoteCsw = data.get('arrayCodLoteCsw', '-')
    dataMovFaseIni = data.get('dataMovFaseIni', dia)
    dataMovFaseFim = data.get('dataMovFaseFim', dia)
    congelado = data.get('congelado', False)
    dataBackupMetas = data.get('dataBackupMetas', '2025-03-26')
    modeloAnalise = data.get('modeloAnalise', 'LoteProducao')

    print(data)
    if congelado =='' or congelado == '-':
        congelado = False
    else:
        congelado = congelado

    meta = MetaFases.MetaFases(codigoPlano, '','',dataMovFaseIni,dataMovFaseFim,congelado,arrayCodLoteCsw, '1',dataBackupMetas,modeloAnalise )
    dados = meta.metasFase()


    column_names = dados.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    OP_data = []
    for index, row in dados.iterrows():
        op_dict = {}
        for column_name in column_names:
            op_dict[column_name] = row[column_name]
        OP_data.append(op_dict)
    return jsonify(OP_data)

@MetasFases_routes.route('/pcp/api/MetasFasesPorVendido', methods=['POST'])
@token_required
def pOST_MetasFasesPorVendido():

    data = request.get_json()
    dia = dayAtual()
    codigoPlano = data.get('codigoPlano')
    arrayCodLoteCsw = data.get('arrayCodLoteCsw', '-')
    dataMovFaseIni = data.get('dataMovFaseIni', dia)
    dataMovFaseFim = data.get('dataMovFaseFim', dia)
    congelado = data.get('congelado', False)
    dataBackupMetas = data.get('dataBackupMetas', '2025-03-26')
    modeloAnalise = data.get('modeloAnalise', 'Vendas')

    print(data)
    if congelado =='' or congelado == '-':
        congelado = False
    else:
        congelado = congelado

    meta = MetaFases.MetaFases(codigoPlano, '','',dataMovFaseIni,dataMovFaseFim,congelado,arrayCodLoteCsw, '1',dataBackupMetas,modeloAnalise )
    dados = meta.metasFase()


    column_names = dados.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    OP_data = []
    for index, row in dados.iterrows():
        op_dict = {}
        for column_name in column_names:
            op_dict[column_name] = row[column_name]
        OP_data.append(op_dict)
    return jsonify(OP_data)


@MetasFases_routes.route('/pcp/api/previsaoCategoriaFase', methods=['POST'])
@token_required
def get_previsaoCategoriaFase():
    data = request.get_json()

    nomeFase = data.get('nomeFase', '-')
    codigoPlano = data.get('codigoPlano')
    arrayCodLoteCsw = data.get('arrayCodLoteCsw', '-')

    meta = MetaFases.MetaFases(codigoPlano,'',nomeFase,'','','',arrayCodLoteCsw)

    dados = meta.previsao_categoria_fase()

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



@MetasFases_routes.route('/pcp/api/previsaoCategoriaFase_peloVendido', methods=['POST'])
@token_required
def get_previsaoCategoriaFase_peloVendido():
    data = request.get_json()

    nomeFase = data.get('nomeFase', '-')
    codigoPlano = data.get('codigoPlano')

    meta = MetaFases.MetaFases(codigoPlano,'',nomeFase)
    dados = meta.previsao_categoria_faseVendido()

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


@MetasFases_routes.route('/pcp/api/faltaProgcategoria_fase', methods=['POST'])
@token_required
def get_faltaProgcategoria_fase():
    data = request.get_json()

    nomeFase = data.get('nomeFase', '-')
    codigoPlano = data.get('codigoPlano')
    arrayCodLoteCsw = data.get('arrayCodLoteCsw', '-')

    meta = MetaFases.MetaFases(codigoPlano,'',nomeFase,'','','',arrayCodLoteCsw)

    dados = meta.faltaProgcategoria_fase()
    #controle.salvarStatus(rotina, ip, datainicio)

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

@MetasFases_routes.route('/pcp/api/faltaProgcategoria_fase_Vendido', methods=['POST'])
@token_required
def get_faltaProgcategoria_fase_vendido():
    data = request.get_json()

    nomeFase = data.get('nomeFase', '-')
    codigoPlano = data.get('codigoPlano')

    meta = MetaFases.MetaFases(codigoPlano,'',nomeFase,'','','')

    dados = meta.faltaProgcategoria_faseVendido()
    #controle.salvarStatus(rotina, ip, datainicio)

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

@MetasFases_routes.route('/pcp/api/FaltaProduzircategoria_fase', methods=['GET'])
@token_required
def get_FaltaProduzircategoria_fase():
    nomeFase = request.args.get('nomeFase', '-')
    codPlano = request.args.get('codPlano', '-')

    meta = MetaFases.MetaFases(codPlano,'',nomeFase)

    dados = meta.faltaProduzirCategoriaFase()
    #controle.salvarStatus(rotina, ip, datainicio)

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