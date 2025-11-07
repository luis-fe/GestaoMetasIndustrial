
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
    try:
        # Instancia a classe e chama o método de negócio.
        # É mais limpo instanciar a classe APENAS para a chamada do método.
        # Se a classe for um Singleton ou for cara de inicializar, use um método estático ou a instância existente.
        novo_doc = ControlePilotos.ControlePilotos().gerarCodigoDocumento()

        if novo_doc is None:
            # Caso a chamada seja bem-sucedida, mas retorne vazio/nulo (sem dados)
            return jsonify({"message": "Nenhuma tag de piloto encontrada.", "data": []}), 204 # 204 No Content

        # Retorna o JSON com o status 200 OK
        return jsonify(novo_doc), 200

    except Exception as e:
        # Captura qualquer erro inesperado durante o processamento
        print(f"Erro ao gerar novo documento: {e}") # Loga o erro no console/logs
        # Retorna um erro 500 (Internal Server Error)
        return jsonify({
            "error": "Erro interno do servidor.",
            "details": str(e) # Opcional: remover 'details' em produção para segurança
        }), 500