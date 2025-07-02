import gc
import pandas as pd
import pytz
from src.connection import ConexaoPostgre
from datetime import datetime, timedelta


class Plano():
    '''
    Classe criada para o "Plano" do PCP que é um conjunto de parametrizacoes para se fazer um planejamento.
    '''
    def __init__(self, codPlano= None ,descricaoPlano = None, iniVendas= None, fimVendas= None,
                 iniFat= None, fimFat= None, usuarioGerador= None, codLote = None, codEmpresa = '1'):
        '''
        Definicao do construtor: atributos do plano
        '''
        self.codPlano = codPlano
        self.descricaoPlano = descricaoPlano
        self.iniVendas = iniVendas
        self.fimVendas = fimVendas
        self.codEmpresa = codEmpresa

        self.iniFat = iniFat
        if self.iniFat == None and self.codPlano!= None: # Atributo de Inicio do Faturamento, caso nao seja informado busca via sql na funcao obterDataInicioFatPlano()
            self.iniFat = self.obterDataInicioFatPlano()
        else:
            self.iniFat = iniFat


        self.fimFat = fimFat
        self.usuarioGerador = usuarioGerador
        self.codLote = codLote



    def obterdiaAtual(self):
        '''
        Método para obter a data atual do dia
        :return:
            'data de hoje no formato - %d/%m/%Y'
        '''
        fuso_horario = pytz.timezone('America/Sao_Paulo')  # Define o fuso horário do Brasil
        agora = datetime.now(fuso_horario)
        agora = agora.strftime('%Y-%M-%D')
        return agora



    def vincularLotesAoPlano(self, codPlano):
        '''
        metodo criado para vincular lotes de producao ao Plano
        :return:
        '''

    def obterDataInicioFatPlano(self):
        '''Metodo que obtem a DataInicial de faturamento do plano'''


        sql = """SELECT p."inicoFat"::varchar FROM pcp."Plano" p where codigo = %s and "codEmpresa" = %s """
        conn = ConexaoPostgre.conexaoEngine()
        dataInicial =  pd.read_sql(sql,conn, params=(str(self.codPlano),self.codEmpresa))

        return dataInicial['inicoFat'][0]

    def obterDataInicioVendoPlano(self):
        '''Metodo que obtem a DataInicial de faturamento do plano'''


        sql = """SELECT p."inicioVenda"::varchar FROM pcp."Plano" p where codigo = %s  and "codEmpresa" = %s  """
        conn = ConexaoPostgre.conexaoEngine()
        dataInicial =  pd.read_sql(sql,conn, params=(str(self.codPlano),self.codEmpresa))

        return dataInicial['inicioVenda'][0]

    def obterDataFimVendoPlano(self):
        '''Metodo que obtem a DataInicial de faturamento do plano'''


        sql = """SELECT p."FimVenda"::varchar FROM pcp."Plano" p where codigo = %s  and "codEmpresa" = %s """
        conn = ConexaoPostgre.conexaoEngine()
        dataInicial =  pd.read_sql(sql,conn, params=(str(self.codPlano),self.codEmpresa,))

        return dataInicial['FimVenda'][0]

    def obterDataFinalFatPlano(self):
        sql = """SELECT p."finalFat" FROM pcp."Plano" p where codigo = %s  and "codEmpresa" = %s """
        conn = ConexaoPostgre.conexaoEngine()
        dataInicial = pd.read_sql(sql, conn, params=(str(self.codPlano),self.codEmpresa))

        return dataInicial['finalFat'][0]



    def obterNumeroSemanasVendas(self):
            '''Metodo que obtem o numero de semanas de vendas do Plano
            Calcula o número de semanas entre duas datas, considerando:
            - A semana começa na segunda-feira.
            - Se a data inicial não for uma segunda-feira, considera a primeira semana começando na data inicial.

            Parâmetros:
                ini (str): Data inicial no formato 'YYYY-MM-DD'.
                fim (str): Data final no formato 'YYYY-MM-DD'.

            Retorna:
                int: Número de semanas entre as duas datas.
            '''

            self.iniVendas, self.fimVendas = self.pesquisarInicioFimVendas()

            if self.iniVendas == '-':
                return 0
            else:

                data_ini = datetime.strptime(self.iniVendas, '%Y-%m-%d')
                data_fim = datetime.strptime(self.fimVendas, '%Y-%m-%d')

                if data_ini > data_fim:
                    raise ValueError("A data inicial deve ser anterior ou igual à data final.")

                # Ajustar para a próxima segunda-feira, se a data inicial não for segunda
                if data_ini.weekday() != 0:  # 0 representa segunda-feira
                    proxima_segunda = data_ini + timedelta(days=(7 - data_ini.weekday()))
                else:
                    proxima_segunda = data_ini

                # Calcular o número de semanas completas a partir da próxima segunda-feira
                semanas_completas = (data_fim - proxima_segunda).days // 7

                # Verificar se existe uma semana parcial no final
                dias_restantes = (data_fim - proxima_segunda).days % 7
                semana_inicial_parcial = 1 if data_ini.weekday() != 0 else 0
                semana_final_parcial = 1 if dias_restantes > 0 else 0

                return semanas_completas + semana_inicial_parcial + semana_final_parcial

    def obterNumeroSemanasFaturamento(self):
            '''Metodo que obtem o numero de semanas de faturamento do Plano
            Calcula o número de semanas entre duas datas, considerando:
            - A semana começa na segunda-feira.
            - Se a data inicial não for uma segunda-feira, considera a primeira semana começando na data inicial.

            Parâmetros:
                ini (str): Data inicial no formato 'YYYY-MM-DD'.
                fim (str): Data final no formato 'YYYY-MM-DD'.

            Retorna:
                int: Número de semanas entre as duas datas.
            '''

            self.iniFat, self.fimFat = self.pesquisarInicioFimFat()

            if self.iniFat == '-':
                return 0
            else:

                data_ini = datetime.strptime(self.iniFat, '%Y-%m-%d')
                data_fim = datetime.strptime(self.fimFat, '%Y-%m-%d')

                if data_ini > data_fim:
                    raise ValueError("A data inicial deve ser anterior ou igual à data final.")

                # Ajustar para a próxima segunda-feira, se a data inicial não for segunda
                if data_ini.weekday() != 0:  # 0 representa segunda-feira
                    proxima_segunda = data_ini + timedelta(days=(7 - data_ini.weekday()))
                else:
                    proxima_segunda = data_ini

                # Calcular o número de semanas completas a partir da próxima segunda-feira
                semanas_completas = (data_fim - proxima_segunda).days // 7

                # Verificar se existe uma semana parcial no final
                dias_restantes = (data_fim - proxima_segunda).days % 7
                semana_inicial_parcial = 1 if data_ini.weekday() != 0 else 0
                semana_final_parcial = 1 if dias_restantes > 0 else 0

                return semanas_completas + semana_inicial_parcial + semana_final_parcial

    def obterSemanaAtual(self):
        '''Calcula em qual semana está o dia atual dentro do intervalo de vendas.
        Caso o dia atual esteja fora do intervalo (após a data final), retorna "finalizado".

        Retorna:
            int ou str: Número da semana atual ou "finalizado".
        '''
        self.iniVendas, self.fimVendas = self.pesquisarInicioFimVendas()

        if self.iniVendas == '-':
            return "finalizado"

        data_ini = datetime.strptime(self.iniVendas, '%Y-%m-%d')
        data_fim = datetime.strptime(self.fimVendas, '%Y-%m-%d')
        hoje = datetime.today()

        if data_ini > data_fim:
            raise ValueError("A data inicial deve ser anterior ou igual à data final.")

        if hoje > data_fim:
            return "finalizado"

        # Ajustar para a próxima segunda-feira, se a data inicial não for segunda
        if data_ini.weekday() != 0:  # 0 representa segunda-feira
            proxima_segunda = data_ini + timedelta(days=(7 - data_ini.weekday()))
        else:
            proxima_segunda = data_ini

        # Calcular a diferença de semanas entre a data inicial ajustada e hoje
        semanas_completas = (hoje - proxima_segunda).days // 7

        # Verificar se hoje está na primeira semana parcial
        semana_inicial_parcial = 1 if hoje < proxima_segunda and hoje >= data_ini else 0

        # Retornar o número da semana atual
        return semanas_completas + semana_inicial_parcial + 1

    def obterSemanaAtualFat(self):
        '''Calcula em qual semana está o dia atual dentro do intervalo de vendas.
        Caso o dia atual esteja fora do intervalo (após a data final), retorna "finalizado".

        Retorna:
            int ou str: Número da semana atual ou "finalizado".
        '''
        self.iniFat, self.fimFat = self.pesquisarInicioFimFat()

        if self.iniFat == '-':
            return "finalizado"

        data_ini = datetime.strptime(self.iniFat, '%Y-%m-%d')
        data_fim = datetime.strptime(self.fimFat, '%Y-%m-%d')
        hoje = datetime.today()

        if data_ini > data_fim:
            raise ValueError("A data inicial deve ser anterior ou igual à data final.")

        if hoje > data_fim:
            return "finalizado"

        # Ajustar para a próxima segunda-feira, se a data inicial não for segunda
        if data_ini.weekday() != 0:  # 0 representa segunda-feira
            proxima_segunda = data_ini + timedelta(days=(7 - data_ini.weekday()))
        else:
            proxima_segunda = data_ini

        # Calcular a diferença de semanas entre a data inicial ajustada e hoje
        semanas_completas = (hoje - proxima_segunda).days // 7

        # Verificar se hoje está na primeira semana parcial
        semana_inicial_parcial = 1 if hoje < proxima_segunda and hoje >= data_ini else 0

        # Retornar o número da semana atual
        return semanas_completas + semana_inicial_parcial + 1

    def pesquisarInicioFimVendas(self):
        '''metodo que pesquisa o inicio e o fim das vendas passeado no codPlano'''

        sql = """
        select 
            "inicioVenda","FimVenda"
        from
            "PCP".pcp."Plano"
        where
            "codigo" = %s
        """

        conn = ConexaoPostgre.conexaoEngine()
        consulta = pd.read_sql(sql,conn,params=(self.codPlano,))

        if not consulta.empty:

            inicioVenda = consulta['inicioVenda'][0]
            FimVenda = consulta['FimVenda'][0]

            return inicioVenda, FimVenda

        else:
            return '-', '-'

    def pesquisarInicioFimFat(self):
        '''metodo que pesquisa o inicio e o fim das vendas passeado no codPlano'''

        sql = """
        select 
            "inicoFat","finalFat"
        from
            "PCP".pcp."Plano"
        where
            "codigo" = %s
        """

        conn = ConexaoPostgre.conexaoEngine()
        consulta = pd.read_sql(sql,conn,params=(self.codPlano,))

        if not consulta.empty:

            inicoFat = consulta['inicoFat'][0]
            finalFat = consulta['finalFat'][0]

            return inicoFat, finalFat

        else:
            return '-', '-'



    def pesquisarTipoNotasPlano(self):
        '''Metodo utilizado para obter os tipo de notas de um determinado plano'''

        sql = """
            select
	            "tipo nota" as "codTipoNota"
            from
	            "PCP".pcp."tipoNotaporPlano" tnp
            where
	            plano = %s
        """

        conn = ConexaoPostgre.conexaoEngine()
        consulta = pd.read_sql(sql,conn,params=(self.codPlano,))

        return consulta

    def get_FaltaProg_PCP(self):
        '''Método que consulta se um determinado lote vinculado a um plano esta como True em :  Considera PCP como falta Programar
         (caso que se aplica somente se o momento da venda da colecao estiver encerrado e o PCP ja programaou tudo)'''


        sql = """
        select
            "cons_Pcp_faltaProg"
        from
            "PCP".pcp."LoteporPlano" lp
        where
            plano = %s
            and lote = %s
            and "cons_Pcp_faltaProg" = true
        """

        conn = ConexaoPostgre.conexaoEngine()
        consulta = pd.read_sql(sql, conn, params=(self.codPlano,))







