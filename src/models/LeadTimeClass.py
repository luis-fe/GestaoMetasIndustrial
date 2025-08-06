import gc
import numpy as np
import pandas as pd
from src.connection import ConexaoPostgre
import pytz
from datetime import datetime
from src.models import OP_CSW

class LeadTimeCalculator:
    """
    Classe para calcular o Lead Time das fases de produção.

    Atributos:
        data_inicio (str): Data de início do intervalo para análise.
        data_final (str): Data final do intervalo para análise.
    """

    def __init__(self, data_inicio, data_final,tipoOPs = None, categorias = None, congelado=False):
        """
        Inicializa a classe com o intervalo de datas para análise.

        Args:
            data_inicio (str): Data de início do intervalo.
            data_final (str): Data final do intervalo.
            tipoOPs ([str]): tipos de Ops a serem filtradas
            categorias ([str]): tipos de categorias a serem filtradas
        """
        self.data_inicio = data_inicio
        self.data_final = data_final
        self.tipoOps = tipoOPs
        self.categorias = categorias
        self.congelado = congelado
    def getLeadTimeFases(self):
        if self.congelado ==True:

            TotaltipoOp = [int(item.split('-')[0]) for item in self.tipoOps]
            id = self.data_inicio + '||' + self.data_final + '||' + str(TotaltipoOp)

            # Usando a string formatada na consulta SQL
            sql = f"""
                select
                    *
                from
                    backup."leadTimeFases" l
                where
                    l.id = %s
            """

            conn = ConexaoPostgre.conexaoEngine()
            saida = pd.read_sql(sql, conn, params=(id,))

            if self.categorias != []:
                categorias = pd.DataFrame(self.categorias, columns=["categoria"])
                saida = pd.merge(saida, categorias, on=['categoria'])

            if self.tipoOps != []:
                result = [int(item.split('-')[0]) for item in self.tipoOps]
                codtipoops = pd.DataFrame(result, columns=["codtipoop"])
                saida = pd.merge(saida, codtipoops, on=['codtipoop'])


        else:
            saida = self.obter_lead_time_fases()

            if self.categorias != []:
                categorias = pd.DataFrame(self.categorias, columns=["categoria"])
                saida = pd.merge(saida, categorias, on=['categoria'])

            if self.tipoOps != []:
                result = [int(item.split('-')[0]) for item in self.tipoOps]
                codtipoops = pd.DataFrame(result, columns=["codtipoop"])
                saida = pd.merge(saida, codtipoops, on=['codtipoop'])

        saida = saida.groupby(["codfase"]).agg({"LeadTime(diasCorridos)": "mean", "Realizado": "sum",
                                                    "LeadTime(PonderadoPorQtd)": 'sum', 'nomeFase': 'first','metaLeadTime':'first'}).reset_index()

        saida['LeadTime(PonderadoPorQtd)'] = saida['LeadTime(PonderadoPorQtd)'] / 100
        saida['LeadTime(diasCorridos)'] = saida['LeadTime(diasCorridos)'].round()
        saida.fillna('-',inplace=True)
        return saida
    def obter_lead_time_fases(self):
        """
        Calcula o Lead Time para as fases de produção no intervalo especificado.

        Returns:
            pd.DataFrame: DataFrame contendo as informações de Lead Time por fase.
        """
        # Consulta SQL para obter os dados de saída
        if self.tipoOps != [] :
            result = [int(item.split('-')[0]) for item in self.tipoOps]
            result = f"({', '.join(str(x) for x in result)})"
            sql = """
            SELECT
                rf.numeroop,
                rf.codfase,
                rf2."metaLeadTime"::varchar,
                rf."seqRoteiro",
                rf."dataBaixa"||' '||rf."horaMov" as "dataBaixa",
                rf."totPecasOPBaixadas" as "Realizado"
            FROM
                pcp.realizado_fase rf 
            join 
                pcp."responsabilidadeFase" rf2 on rf2."codFase" = rf.codfase::varchar
            WHERE
                rf."dataBaixa"::date >= %s AND rf."dataBaixa"::date <= %s and codtipoop in """+result

        else:
            sql = """
            SELECT
                rf.numeroop,
                rf.codfase,
                rf2."metaLeadTime"::varchar,
                rf."seqRoteiro",
                rf."dataBaixa"||' '||rf."horaMov" as "dataBaixa",
                rf."totPecasOPBaixadas" as "Realizado"
            FROM
                pcp.realizado_fase rf 
            join 
                pcp."responsabilidadeFase" rf2 on rf2."codFase" = rf.codfase::varchar
            WHERE
                rf."dataBaixa"::date >= %s AND rf."dataBaixa"::date <= %s ;
            """


        # Consulta SQL para obter os dados de entrada NO CSW (maior velocidade de processamento))
        # Conectar ao banco de dados
        conn = ConexaoPostgre.conexaoEngineWMSSrv()

        # Executar as consultas
        saida = pd.read_sql(sql, conn, params=(self.data_inicio, self.data_final))


        op_csw = OP_CSW.OP_CSW()
        entrada, sqlFasesCsw = op_csw.get_leadTimeCsW()

        # Processar os dados
        entrada['seqRoteiro'] = entrada['seqRoteiro'] + 1
        entrada.rename(columns={'dataBaixa': 'dataEntrada'}, inplace=True)
        print('saida')
        print(saida)
        saida = pd.merge(saida, entrada, on=['numeroop', 'seqRoteiro'])
        saida = saida.drop_duplicates()

        saida = pd.merge(saida,sqlFasesCsw,on='codfase')

        # Verifica e converte para datetime se necessário



        saida['dataEntrada'] = pd.to_datetime((saida['dataEntrada'] + ' ' + saida['horaMovEntrada']),errors='coerce')
        saida['dataBaixa'] = pd.to_datetime(saida['dataBaixa'] ,errors='coerce')



        saida['LeadTime(diasCorridos)'] = (saida['dataBaixa'] - saida['dataEntrada']).dt.total_seconds() / 3600
        print(saida['LeadTime(diasCorridos)'])
        saida['LeadTime(diasCorridos)'] =  saida['LeadTime(diasCorridos)'] / 24

        saida['RealizadoFase'] = saida.groupby('codfase')['Realizado'].transform('sum')
        saida['LeadTime(PonderadoPorQtd)'] = (saida['Realizado'] / saida['RealizadoFase']) * 100

        saida['LeadTime(PonderadoPorQtd)'] = saida['LeadTime(diasCorridos)']*saida['LeadTime(PonderadoPorQtd)']
        saida['LeadTime(PonderadoPorQtd)'] = saida['LeadTime(PonderadoPorQtd)'].round()

        saida['categoria'] = saida['nome'].astype(str).apply(self.mapear_categoria)
        saida['categoria'] = '-'
        '''Inserindo as informacoes no banco para acesso temporario'''

        TotaltipoOp = [int(item.split('-')[0]) for item in self.tipoOps]
        id = self.data_inicio+'||'+self.data_final+'||'+str(TotaltipoOp)
        saida['id'] = id
        saida['diaAtual'] = self.obterdiaAtual()
        self.deletar_backup(id,"leadTimeFases")

        try:
            ConexaoPostgre.Funcao_InserirOFF_srvWMS(saida,saida['codfase'].size,'leadTimeFases','append')
        except:
            print('erro')

        return saida

    def deletar_backup(self, id, tabela_temporaria):
        tabela_temporaria = '"'+tabela_temporaria+'"'
        delete = """
        DELETE FROM backup.%s
        WHERE id = %s
        """ % (tabela_temporaria, '%s')  # Substituindo tabela_temporaria corretamente

        with ConexaoPostgre.conexaoInsercao() as conn:
            with conn.cursor() as curr:
                curr.execute(delete, (id,))
                conn.commit()


    def mapear_categoria(self,nome):
        categorias_map = {
            'CAMISA': 'CAMISA',
            'POLO': 'POLO',
            'BATA': 'CAMISA',
            'TRICOT': 'TRICOT',
            'BONE': 'BONE',
            'CARTEIRA': 'CARTEIRA',
            'TSHIRT': 'CAMISETA',
            'REGATA': 'CAMISETA',
            'BLUSAO': 'AGASALHOS',
            'BABY': 'CAMISETA',
            'JAQUETA': 'JAQUETA',
            'CINTO': 'CINTO',
            'PORTA CAR': 'CARTEIRA',
            'CUECA': 'CUECA',
            'MEIA': 'MEIA',
            'SUNGA': 'SUNGA',
            'SHORT': 'SHORT',
            'BERMUDA': 'BERMUDA'
        }
        for chave, valor in categorias_map.items():
            if chave in nome.upper():
                return valor
        return '-'
    def obterdiaAtual(self):
        fuso_horario = pytz.timezone('America/Sao_Paulo')  # Define o fuso horário do Brasil
        agora = datetime.now(fuso_horario)
        agora = agora.strftime('%Y-%m-%d')
        return pd.to_datetime(agora)

    def LimpezaBackpCongelamento(self,QuantidadeDiasEmBackup):
        QuantidadeDiasEmBackup = "'"+str(QuantidadeDiasEmBackup)+" days'"
        delete = """
        		delete 
                    from
                        backup."leadTimeFases" l
                    where 
                	    l."diaAtual"::Date  < CURRENT_DATE - INTERVAL """+QuantidadeDiasEmBackup

        with ConexaoPostgre.conexaoInsercao() as conn:
            with conn.cursor() as curr:
                curr.execute(delete,)
                conn.commit()

    def ObterCategorias(self):
        sql = """Select "nomecategoria" as categoria, "leadTime" as meta from pcp.categoria """
        conn = ConexaoPostgre.conexaoEngine()
        consulta = pd.read_sql(sql, conn)

        return consulta

    def getLeadTimeFaccionistas(self, faccionistas):

        oP_CSW = OP_CSW.OP_CSW()

        realizado, sqlRetornoFaccionista =  oP_CSW.leadtimeFaccionistaCsw(self.data_inicio, self.data_final)

        realizado['categoria'] = '-'
        realizado['nome'] = realizado['nome'].astype(str)
        faccionistas['codfaccionista'] = faccionistas['codfaccionista'].astype(str)
        realizado['codfaccionista'] = realizado['codfaccionista'].astype(str)
        sqlRetornoFaccionista['codfaccionista'] = sqlRetornoFaccionista['codfaccionista'].astype(str)

        realizado['categoria'] = realizado['nome'].apply(self.mapear_categoria)
        faccionistas = faccionistas.drop(columns='categoria')

        realizado = pd.merge(realizado, faccionistas, on='codfaccionista', how='left')
        realizado = pd.merge(realizado, sqlRetornoFaccionista, on=['codfaccionista', 'codFase', 'codOP'])

        realizado.fillna('-', inplace=True)
        # Verifica e converte para datetime se necessário
        realizado['dataEntrada'] = pd.to_datetime(realizado['dataEntrada'], errors='coerce')
        realizado['dataBaixa'] = pd.to_datetime(realizado['dataBaixa'], errors='coerce')
        realizado['LeadTime(diasCorridos)'] = (realizado['dataBaixa'] - realizado['dataEntrada']).dt.days

        # Convertendo a lista em um DataFrame

        if self.tipoOps != []:
            result = [int(item.split('-')[0]) for item in self.tipoOps]
            codtipoops = pd.DataFrame(result, columns=["codtipoop"])

            realizado = pd.merge(realizado, codtipoops, on=['codtipoop'])

        if self.categorias != []:
            categoriasData = pd.DataFrame(self.categorias, columns=["categoria"])
            realizado = pd.merge(realizado, categoriasData, on='categoria')

        realizado['Realizadofac'] = realizado.groupby('codfaccionista')['Realizado'].transform('sum')
        realizado['LeadTime(PonderadoPorQtd)'] = (realizado['Realizado'] / realizado['Realizadofac']) * 100

        realizado['LeadTime(PonderadoPorQtd)'] = realizado['LeadTime(diasCorridos)'] * realizado[
            'LeadTime(PonderadoPorQtd)']
        realizado['LeadTime(PonderadoPorQtd)'] = realizado['LeadTime(PonderadoPorQtd)'].round()
        realizado = realizado.groupby(["codfaccionista"]).agg({"LeadTime(diasCorridos)": "mean", "Realizado": "sum",
                                                               "LeadTime(PonderadoPorQtd)": 'sum',
                                                               'apelidofaccionista': 'first'}).reset_index()
        realizado['LeadTime(PonderadoPorQtd)'] = realizado['LeadTime(PonderadoPorQtd)'] / 100
        realizado['LeadTime(diasCorridos)'] = realizado['LeadTime(diasCorridos)'].round()
        print('realizado faccionistas:')
        print(realizado)

        return realizado


