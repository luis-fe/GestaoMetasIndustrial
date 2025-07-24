
import pandas as pd
from src.connection import ConexaoPostgre
from datetime import datetime
from dateutil.relativedelta import relativedelta


class GastosOrcamentoBI():
    '''Classe que interage com os dados de orçamento'''


    def __init__(self, codEmpresa = '1', dataInicial = '', dataFinal = ''):

        self.codEmpresa = codEmpresa
        self.dataInicial = dataInicial
        self.dataFinal = dataFinal



    def get_orcamentoGastos(self):
        '''Metodo que consulta o orçamento projetado'''


        sql = """
        select
            centrocusto ,
            "codEmpresa" ,
            "contaContabil" ,
            mes ,
            ano ,
            valor
        from
            "PCP".pcp."orcamentoCentroCusto" occ
        where
            mes in %
            and ano in %
            and "codEmpresa" = %s
            """
        ano_array = self.__obter_anos(self.dataInicial, self.dataFinal)
        nomes_array = self.__obter_nomes_meses(self.dataInicial, self.dataFinal)

        ano_str = "({})".format(", ".join("'{}'".format(ano) for ano in ano_array))
        nome_str = "({})".format(", ".join("'{}'".format(nome) for ano in nomes_array))

        print(ano_str)
        print(nome_str)


        return pd.DataFrame([{'Mensagem':'retornando o teste'}])

    def __obter_nomes_meses(self, data_inicial: str, data_final: str):
        ''' Metodo para obter os meses de acordo com as datas informada
        '''


        meses_pt = {
            1: 'janeiro', 2: 'fevereiro', 3: 'março', 4: 'abril',
            5: 'maio', 6: 'junho', 7: 'julho', 8: 'agosto',
            9: 'setembro', 10: 'outubro', 11: 'novembro', 12: 'dezembro'
        }

        data_ini = datetime.strptime(data_inicial, "%Y-%m-%d")
        data_fim = datetime.strptime(data_final, "%Y-%m-%d")

        nomes_meses = []
        while data_ini <= data_fim:
            nome_mes = meses_pt[data_ini.month]
            if nome_mes not in nomes_meses:
                nomes_meses.append(nome_mes)
            data_ini += relativedelta(months=1)

        return nomes_meses

    def __obter_anos(self, data_inicial: str, data_final: str):
        ano_ini = datetime.strptime(data_inicial, "%Y-%m-%d").year
        ano_fim = datetime.strptime(data_final, "%Y-%m-%d").year

        return [str(ano) for ano in range(ano_ini, ano_fim + 1)]



