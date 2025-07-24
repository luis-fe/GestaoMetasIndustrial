
import pandas as pd
from src.connection import ConexaoPostgre
from datetime import datetime
from dateutil.relativedelta import relativedelta


class GastosOrcamentoBI():
    '''Classe que interage com os dados de orçamento'''


    def __init__(self, codEmpresa = '1', dataInicial = '', dataFinal = ''):

        self.codEmpresa = str(codEmpresa)
        self.dataInicial = dataInicial
        self.dataFinal = dataFinal



    def get_orcamentoGastos(self):
        '''Metodo que consulta o orçamento projetado'''



        ano_array = self.__obter_anos(self.dataInicial, self.dataFinal)
        nomes_array = self.__obter_nomes_meses(self.dataInicial, self.dataFinal)

        ano_str = "({})".format(", ".join("'{}'".format(ano) for ano in ano_array))
        nome_str = "({})".format(", ".join("'{}'".format(nome) for nome in nomes_array))

        conn = ConexaoPostgre.conexaoEngine()

        sql = f"""
        select
            centrocusto as centrocusto,
            "codEmpresa" ,
            "contaContabil" ,
            mes ,
            ano ,
            valor as "valorOrcado"
        from
            "PCP".pcp."orcamentoCentroCusto" occ
        where
            mes in {nome_str}
            and ano in {ano_str}
            and "codEmpresa" = '{self.codEmpresa}'
            """


        consulta = pd.read_sql(sql,conn)


        consulta = consulta.groupby('centrocusto').agg({'valorOrcado':'sum'}).reset_index()



        return consulta

    def __obter_nomes_meses(self, data_inicial: str, data_final: str):
        ''' Metodo para obter os meses de acordo com as datas informada
        '''


        meses_pt = {
            1: 'Janeiro', 2: 'Fevereiro', 3: 'Marco', 4: 'Abril',
            5: 'Maio', 6: 'Junho', 7: 'Julho', 8: 'Agosto',
            9: 'Setembro', 10: 'Outubro', 11: 'Novembro', 12: 'Dezembro'
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



