from datetime import datetime
import numpy as np
import pandas as pd
from src.connection import ConexaoPostgre
import pytz
from src.models import Cronograma, OrdemProd
import calendar
import datetime
import re



class ProducaoFases():
    '''Classe que controla a producao das fases '''

    def __init__(self, periodoInicio = None, periodoFinal = None, codFase = None, dias_buscaCSW = 0, codEmpresa = None, limitPostgres = None, utimosDias = None,
                 arraytipoOPExluir = None, consideraMost ='nao', nomeFase ='', arrayTipoProducao = None):
        '''Contrutor da Classe'''
        self.periodoInicio = periodoInicio
        self.periodoFinal = periodoFinal
        self.codFase = codFase
        self.dias_buscaCSW = dias_buscaCSW
        self.codEmpresa = codEmpresa # codEmpresa - codEmpresa utilizado para consultar os dados
        self.limitPostgres = limitPostgres
        self.utimosDias = utimosDias
        self.arraytipoOPExluir = arraytipoOPExluir # Array com os codigo de tipo de op a serem excluidos da analise
        self.consideraMost = 'nao'
        self.nomeFase = nomeFase
        self.arrayTipoProducao = arrayTipoProducao # Array com os tipo de producao desejado


    def realizadoMediaMovel(self):
        '''Método que retona o realizado por fase de acordo com o periodo informado'''

        realizado = self.__sqlRealizadoPeriodo() # realiza a consulta sql no banco postgre do realizado

        ordemProd = OrdemProd.OrdemProd()

        # 1 - verfica se existe tipo de ops a serem excluidos da analise
        if self.arraytipoOPExluir is not None and isinstance(self.arraytipoOPExluir, list):
            realizado = realizado[~realizado['codtipoop'].isin(self.arraytipoOPExluir)]

        # 2 - verfica o arrayTipoProducao com os tipo de ordens de producao que desejo consultar
        if self.arrayTipoProducao != None:

            agrupamentoOP = ordemProd.agrupado_x_tipoOP()
            dataFrameTipoProducao = pd.DataFrame({'Agrupado': self.arrayTipoProducao})
            dataFrameTipoProducao = pd.merge(agrupamentoOP,dataFrameTipoProducao, on='Agrupado')
            realizado = pd.merge(realizado,dataFrameTipoProducao, on='codtipoop')


        else:
            #self.arrayTipoProducao = ['Producao']
            agrupamentoOP = ordemProd.agrupado_x_tipoOP()
            #dataFrameTipoProducao = pd.DataFrame({'Agrupado': self.arrayTipoProducao})
            #dataFrameTipoProducao = pd.merge(agrupamentoOP,dataFrameTipoProducao, on='Agrupado')
            #realizado['codtipoop'] = realizado['codtipoop'].astype(str)
            #realizado = pd.merge(realizado,dataFrameTipoProducao, on='codtipoop')


        realizado['filtro'] = realizado['codFase'].astype(str) + '|' + realizado['codEngenharia'].str[0]
        realizado = realizado[(realizado['filtro'] != '401|6')]
        realizado = realizado[(realizado['filtro'] != '401|5')]
        realizado = realizado[(realizado['filtro'] != '426|6')]
        realizado = realizado[(realizado['filtro'] != '441|5')]
        realizado = realizado[(realizado['filtro'] != '412|5')]

        realizado['codFase'] = np.where(realizado['codFase'].isin(['431', '455', '459']), '429', realizado['codFase'])
        realizado['Tipo Producao'] = realizado['descricaolote'].apply(self.__tratamentoInformacaoColecao2)
        print(realizado)

        print(f'teste arrayTipoProducaoRealizado{self.arrayTipoProducao}')
        if self.arrayTipoProducao ==[] or self.arrayTipoProducao == None :
            print('nada filtrado')
        else:
            df = pd.DataFrame(self.arrayTipoProducao, columns=['Tipo Producao'])
            realizado = pd.merge(realizado, df, on='Tipo Producao')



        realizado = realizado.groupby(["codFase"]).agg({"Realizado": "sum"}).reset_index()

        cronograma = Cronograma.Cronograma()
        diasUteis = cronograma.calcular_dias_uteis(self.periodoInicio, self.periodoFinal,True, False)

        # Evitar divisão por zero ou infinito
        realizado['Realizado'] = np.where(diasUteis == 0, 0, realizado['Realizado'] / diasUteis)
        #print(f'dias uteis {diasUteis}')
        realizado['diasUteis'] = diasUteis
        return realizado

    def __sqlRealizadoPeriodo(self):
        '''Metodo privado que consulta via sql o realizado no banco de dados Postgre '''
        sql = """
        select 
            rf."codEngenharia",
    	    rf.numeroop ,
    	    rf.codfase:: varchar as "codFase", rf."seqRoteiro" , rf."dataBaixa"::date , rf."nomeFaccionista", rf."codFaccionista" , rf."horaMov"::time,
    	    rf."totPecasOPBaixadas" as "Realizado", rf."descOperMov" as operador, rf.chave ,"codtipoop", rf.descricaolote 
        from
    	    pcp.realizado_fase rf 
        where 
    	    rf."dataBaixa"::date >= %s 
    	    and rf."dataBaixa"::date <= %s ;
        """
        conn = ConexaoPostgre.conexaoEngineWMSSrv()
        realizado = pd.read_sql(sql, conn, params=(self.periodoInicio, self.periodoFinal,))


        return realizado

    def lotesFiltragrem(self):

        sql = """
            SELECT 
                DISTINCT rf.descricaolote AS filtro
            FROM 
                pcp.realizado_fase rf
            WHERE 
                rf."dataBaixa"::DATE BETWEEN %s AND %s
                AND rf.descricaolote NOT LIKE '%%LOTO%%';
        """
        conn = ConexaoPostgre.conexaoEngineWMSSrv()
        realizado = pd.read_sql(sql, conn, params=(self.periodoInicio, self.periodoFinal,))


        realizado['filtro'] = realizado['filtro'].str.replace('LOTE INTERNO ','')
        realizado['filtro'] = realizado['filtro'].str.replace('PRODUÇÃO ','')
        realizado['filtro'].fillna('-',inplace = True)
        realizado.loc[realizado["filtro"].str.contains("ENCOMENDA", na=False), "filtro"] = "ENCOMENDA"
        realizado.loc[realizado["filtro"].str.contains(" TH", na=False), "filtro"] = "ENCOMENDA"
        realizado.loc[realizado["filtro"].str.contains(" QUIOSQUE", na=False), "filtro"] = "ENCOMENDA"

        realizado['filtro'] = realizado['filtro'].str.replace('Á','A')
        realizado['filtro'] = realizado['filtro'].str.replace('ÃO','AO')

        return realizado.drop_duplicates()




    def calcular_dias_sem_domingos(self,dataInicio, dataFim):
        # Obtendo a data atual
        dataHoje = self.obterdiaAtual()
        # Convertendo as datas para o tipo datetime, se necessário
        if not isinstance(dataInicio, pd.Timestamp):
            dataInicio = pd.to_datetime(dataInicio)
        if not isinstance(dataFim, pd.Timestamp):
            dataFim = pd.to_datetime(dataFim)
        if not isinstance(dataHoje, pd.Timestamp):
            dataHoje = pd.to_datetime(dataFim)

        # Inicializando o contador de dias
        dias = 0
        data_atual = dataInicio

        # Iterando através das datas
        while data_atual <= dataFim:
            # Se o dia não for sábado (5) ou domingo (6), incrementa o contador de dias
            if data_atual.weekday() != 5 and data_atual.weekday() != 6:
                dias += 1
            # Incrementa a data atual em um dia
            data_atual += pd.Timedelta(days=1)

        if dias == 0:
            dias = 1

        return dias

    def obterdiaAtual(self):
        fuso_horario = pytz.timezone('America/Sao_Paulo')  # Define o fuso horário do Brasil
        agora = datetime.now(fuso_horario)
        agora = agora.strftime('%Y-%m-%d')
        return pd.to_datetime(agora)


    def __sqlObterFases(self):

        sql = """
        select
	        distinct "codFase"::varchar ,
	        "nomeFase"
        from
	        pcp."Eng_Roteiro" er
        """

        conn = ConexaoPostgre.conexaoEngine()
        realizado = pd.read_sql(sql, conn)
        return realizado



    def realizadoFasePeriodoFase(self):

        realizado = self.__sqlRealizadoPeriodo()


        realizado['filtro'] = realizado['codFase'].astype(str) + '|' + realizado['codEngenharia'].str[0]
        realizado = realizado[(realizado['filtro'] != '401|6')]
        realizado = realizado[(realizado['filtro'] != '401|5')]
        realizado = realizado[(realizado['filtro'] != '426|6')]
        realizado = realizado[(realizado['filtro'] != '441|5')]
        realizado = realizado[(realizado['filtro'] != '412|5')]

        # filtrando o nome da fase
        fases = self.__sqlObterFases()

        realizado = pd.merge(realizado, fases , on ="codFase")

        realizado = realizado[realizado["nomeFase"] == str(self.nomeFase)].reset_index()



        realizado = realizado.groupby(["codFase",'dataBaixa']).agg({"Realizado": "sum"}).reset_index()


        # Convertendo para datetime sem especificar o formato fixo
        realizado["dataBaixa"] = pd.to_datetime(realizado["dataBaixa"], errors="coerce")

        # Criando a coluna formatada no padrão brasileiro
        realizado["dataBaixa"] = realizado["dataBaixa"].dt.strftime("%d/%m/%Y")

        dias_semana = {
            0: "segunda-feira",
            1: "terça-feira",
            2: "quarta-feira",
            3: "quinta-feira",
            4: "sexta-feira",
            5: "sábado",
            6: "domingo"
        }

        realizado["dia"] = pd.to_datetime(realizado["dataBaixa"], format="%d/%m/%Y", errors="coerce").dt.dayofweek.map(
            dias_semana)
        realizado = realizado.astype(str)

        return realizado



    def realizadoFasePeriodoFase_detalhaDia(self):

        realizado = self.__sqlRealizadoPeriodo()


        realizado['filtro'] = realizado['codFase'].astype(str) + '|' + realizado['codEngenharia'].str[0]
        realizado = realizado[(realizado['filtro'] != '401|6')]
        realizado = realizado[(realizado['filtro'] != '401|5')]
        realizado = realizado[(realizado['filtro'] != '426|6')]
        realizado = realizado[(realizado['filtro'] != '441|5')]
        realizado = realizado[(realizado['filtro'] != '412|5')]

        # filtrando o nome da fase
        fases = self.__sqlObterFases()

        realizado = pd.merge(realizado, fases , on ="codFase")

        realizado = realizado[realizado["nomeFase"] == str(self.nomeFase)].reset_index()

        realizado = realizado.groupby(["codEngenharia","numeroop",'dataBaixa']).agg({"Realizado": "sum","horaMov":"first","descricaolote":"first"}).reset_index()
        # Conversão de datas
        realizado["dataBaixa"] = pd.to_datetime(realizado["dataBaixa"], errors="coerce")
        realizado["dataBaixa"] = realizado["dataBaixa"].dt.strftime("%d/%m/%Y")

        # Função para formatar horaMov
        def formatar_hora(hora):
            try:
                if pd.isnull(hora):
                    return "-"
                if isinstance(hora, str):
                    # tenta parsear uma string tipo '13:45:00'
                    dt = pd.to_datetime(hora, format="%H:%M:%S", errors="coerce")
                    return dt.strftime("%H:%M:%S") if not pd.isnull(dt) else "-"
                if isinstance(hora, datetime.time):
                    return hora.strftime("%H:%M:%S")
                if isinstance(hora, (datetime.datetime, pd.Timestamp)):
                    return hora.time().strftime("%H:%M:%S")
                return "-"
            except Exception:
                return "-"

        # Aplicar formatação segura
        realizado["horaMov"] = realizado["horaMov"].apply(formatar_hora)
        realizado['COLECAO'] = realizado['descricaolote'].apply(self.__tratamentoInformacaoColecao)
        realizado.drop(['descricaolote'], axis=1, inplace=True)
        realizado = realizado.sort_values(by=['horaMov'], ascending=True)

        return realizado

    def __tratamentoInformacaoColecao(self, descricaoLote):
        if 'INVERNO' in descricaoLote:
            return 'INVERNO'
        elif 'PRI' in descricaoLote:
            return 'VERAO'
        elif 'ALT' in descricaoLote:
            return 'ALTO VERAO'
        elif 'VER' in descricaoLote:
            return 'VERAO'
        else:
            return 'ENCOMENDAS'

    def __tratamentoInformacaoColecao2(self, descricaoLote):
        '''Método privado que trata a informação do nome da coleção'''

        descricaoLote = descricaoLote.upper()  # padroniza para evitar erro com letras minúsculas
        ano_match = re.search(r'\d{4}', descricaoLote)

        if ano_match:
            ano = ano_match.group()
        else:
            return 'ENCOMENDAS/OUTRAS'

        if 'INVERNO' in descricaoLote:
            nome = f'INVERNO {ano}'
        elif 'PRI' in descricaoLote:
            nome = f'VERAO {ano}'
        elif 'ALT' in descricaoLote:
            nome = f'ALTO VERAO {ano}'
        elif 'VER' in descricaoLote:
            nome = f'VERAO {ano}'
        else:
            nome = 'ENCOMENDAS/OUTRAS'

        return nome





