
import pandas as pd
from flask import Blueprint, jsonify, request
from functools import wraps
from src.models import ControlePilotos
import datetime
import pytz

controle_pilotos = Blueprint('controle_pilotos', __name__)

def token_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = request.headers.get('Authorization')
        if token == 'a44pcp22':  # Verifica se o token é igual ao token fixo
            return f(*args, **kwargs)
        return jsonify({'message': 'Acesso negado'}), 401

    return decorated_function



@controle_pilotos.route('/pcp/api/Consula_tags_pilotos', methods=['GET'])
@token_required
def get_Consula_tags_pilotos():
    codEmpresa = request.args.get('codEmpresa','1')

    dados = ControlePilotos.ControlePilotos(codEmpresa).get_tags_piloto()
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


@controle_pilotos.route('/pcp/api/gerarNovoDocumento', methods=['GET'])
@token_required
def gerar_novo_documento():
    """
    Endpoint para buscar as tags de piloto e gerar um novo documento (ou conjunto de dados).
    """
    novo_doc = ControlePilotos.ControlePilotos().gerarCodigoDocumento()

    if novo_doc is None:
        # 204 No Content - Retorna sem corpo
        return '', 204

        # --- CORREÇÃO AQUI: Retorne um dicionário com um campo ---
    # O valor da API será acessado no JS como data.codigo
    resposta_json = {
        "codigo": novo_doc
    }

    # Retorna o JSON com o status 200 OK
    return jsonify(resposta_json), 200

