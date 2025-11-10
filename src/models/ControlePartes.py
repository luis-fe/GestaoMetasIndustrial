from src.models import OP_CSW
import pandas as pd
from src.connection import ConexaoPostgre

class ControlePartes():
    '''Classe responsavel pelo controle das Partes'''

    def __init__(self, codEmpresa = 1):

        self.codEmpresa = codEmpresa
        self.op_csw = OP_CSW.OP_CSW(self.codEmpresa)


    def __relacao_ops_interalo_separaco_montagem(self):
        '''metodo privado que obtem as OPs que estao no intervalo entre separacao e montagem '''

        ops = self.op_csw.ordem_prod_situacao_aberta_mov_separacao()
        ops_montagem = self.op_csw.ordem_prod_situacao_aberta_mov_montagem()

        ops = pd.merge(ops, ops_montagem, on= 'numeroOP', how='left')
        ops.fillna('-',inplace=True)

        ops = ops[ops['obs2']=='-'].reset_index()

        return ops

    def ops_demanda_partes(self):


        ops = self.__relacao_ops_interalo_separaco_montagem()
        ops_partes = self.op_csw.relacao_ops_que_consome_partes()

        # 1-  Retirando o numero da op em "ops_partes"
        ops_partes['numeroOP'] = ops_partes.iloc[:, 1].str.split('/').str[1]
        ops = pd.merge(ops, ops_partes, on= 'numeroOP')


        n_pecas = self.__adicionando_numeroPcs()

        ops = pd.merge(ops, n_pecas, on= ['numeroOP','codSortimento','seqTam'], how='left')
        ops.fillna('-',inplace=True)


        return ops


    def __adicionando_numeroPcs(self):

        sql = """
        select
            o.numeroop as "numeroOP",
            o."codSortimento" as "codSortimento",
            o."seqTamanho" as "seqTam",
            o.total_pcs
        from
            "PCP".pcp.ordemprod o
        """

        conn = ConexaoPostgre.conexaoEngine()

        consulta = pd.read_sql(sql,conn)

        return consulta

