import numpy as np
import pandas as pd

from src.connection import ConexaoERP
from src.models import GastosOrçamentoBI


class Gastos_centroCusto_CSW():
    '''Classe que captura as informacoes de gastos e centro de custo '''

    def __init__(self, codEmpresa = '1' , dataCompentenciaInicial= '',dataCompentenciaFinal= '',
                 codFornecedor = '', nomeFornecedor= '', dataLcto ='', codDocumento = '',
                 seqItemDocumento = '', descricaoItem = '', centroCustovalor ='', codContaContabil = '', nomeItem ='',
                 codCentroCusto = '', nomeCentroCusto = '',  nomeArea =''
                 ):

        self.codEmpresa = str(codEmpresa)
        self.dataCompentenciaInicial = str(dataCompentenciaInicial)
        self.dataCompentenciaFinal = str(dataCompentenciaFinal)
        self.codFornecdor = codFornecedor
        self.nomeFornecedor = nomeFornecedor
        self.dataLcto = dataLcto
        self.codDocumento = codDocumento
        self.seqItemDocumento = seqItemDocumento
        self.descricaoItem = descricaoItem
        self.centroCustovalor = centroCustovalor
        self.codContaContabil = codContaContabil
        self.nomeItem = nomeItem
        self.codCentroCusto = codCentroCusto
        self.nomeCentroCusto = nomeCentroCusto
        self.nomeArea = nomeArea

        self.gastosOrcamentoBI = GastosOrçamentoBI.GastosOrcamentoBI(self.codEmpresa, self.dataCompentenciaInicial, self.dataCompentenciaFinal)




    def get_notasEntredas_Csw(self):
        '''Metodo que captura as notas de entrda do CSW'''


        sql = f"""
            SELECT
                e.fornecedor as codFornecedor,
                f.nome as nomeFornecedor,
                e.dataEntrada as dataLcto,
                e.numDocumento as codDocumento,
                ei.item as seqItemDocumento,
                ei.descricaoItem as descricaoItem,
                ei.centroCustoValor as centroCustovalor,
                ei.contaContabil as codContaContabil,
                cb.nome as nomeItem
            FROM
                est.NotaFiscalEntrada e
            INNER JOIN
                    est.NotaFiscalEntradaItens ei   
                on 
                ei.codEmpresa = e.codEmpresa 
                and ei.codFornecedor = e.fornecedor 
                and ei.numDocumento = e.numDocumento 
            inner JOIN 
                CPG.Fornecedor F 
                ON F.codEmpresa = e.codEmpresa 
                and f.codigo = e.fornecedor 
            inner JOIN 
                ctb.ContaContabil cb 
                on cb.codigo = ei.contaContabil
            WHERE
                e.codEmpresa = {self.codEmpresa}
                and e.dataEntrada  >= '{self.dataCompentenciaInicial}'
                and e.dataEntrada  >= '{self.dataCompentenciaFinal}'
                and ei.centroCustoValor > 0
        """



        with ConexaoERP.ConexaoInternoMPL() as conn:
            with conn.cursor() as cursor_csw:
                # Executa a primeira consulta e armazena os resultados
                cursor_csw.execute(sql)
                colunas = [desc[0] for desc in cursor_csw.description]
                rows = cursor_csw.fetchall()
                consulta = pd.DataFrame(rows, columns=colunas)
                del rows
        # Função para extrair pares e manter outras colunas
        def extrair_pares(row):
            valores = row['centroCustovalor'].split(';')
            pares = []
            for i in range(0, len(valores), 2):
                if i + 1 < len(valores):
                    nova_linha = row.to_dict()
                    nova_linha['centrocusto'] = valores[i]
                    nova_linha['valor'] = valores[i + 1]
                    pares.append(nova_linha)
            return pares

        # Aplica a função
        linhas_expandida = sum(consulta.apply(extrair_pares, axis=1), [])
        consulta = pd.DataFrame(linhas_expandida)
        centroCusto = self.__get_centroCusto()
        contacontb = self.__getContaContabil()

        consulta['qtd'] = ''
        consulta['vlrUnitario'] = ''
        consulta['codItem'] = ''
        consulta['valor'] =consulta['valor'].astype(float)
        consulta['valor'] =consulta['valor']/100000

        consulta2 = self.__get_intensReqIndependente()
        consulta = pd.concat([consulta, consulta2])

        consulta['centrocusto'] =consulta['centrocusto'].astype(str)
        centroCusto['centrocusto'] =centroCusto['centrocusto'].astype(str)

        consulta = pd.merge(consulta, centroCusto , on ='centrocusto', how='left')
        consulta['codContaContabil'] =consulta['codContaContabil'].astype(str)


        consulta = pd.merge(consulta, contacontb , on ='codContaContabil',how='left')

        consulta.fillna('-', inplace=True)


        return consulta



    def __get_centroCusto(self):
        '''Metodo que obtem os centro de custos cadastrados no erp cesw'''


        sql = """
        select
            c.mascaraRdz as centrocusto,
            c.nome as nomeCentroCusto, 
            c.codarea as codArea, 
            a.nome as nomeArea
        FROM
            Cad.CCusto c
        inner join
            cad.CCustoArea a on a.codigo = c.codArea 
        WHERE
            c.codEmpresa = 1 and c.situacao = 1
        """
        with ConexaoERP.ConexaoInternoMPL() as conn:
            with conn.cursor() as cursor_csw:
                # Executa a primeira consulta e armazena os resultados
                cursor_csw.execute(sql)
                colunas = [desc[0] for desc in cursor_csw.description]
                rows = cursor_csw.fetchall()
                consulta = pd.DataFrame(rows, columns=colunas)
                del rows

        consulta['nomeArea'] = np.where(consulta['codArea'] == '4', 'PRODUCAO', consulta['nomeArea'])

        return consulta



    def get_centro_custo(self):
        '''Metodo publico para obter os centro de custos cadastrddos no ERP CSW'''


        consulta = self.__get_centroCusto()



        return consulta


    def get_Empresa(self):


        empresa = pd.DataFrame({'codEmpresa': ['1','4'], 'nomeEmpresa': ['Matriz','Filial-Cianorte']})


        return empresa


    def get_area(self):

        area = self.get_centro_custo()

        area = area.groupby('nomeArea').agg({'codArea': 'first'}).reset_index()

        return area


    def __getContaContabil(self):
        '''Metodo privado que busca no ERP do CSW o plano de contas contabil'''

        sql = """
        SELECT
            c.codigo as codContaContabil,
            c.nome as nomeContaContabil,
            pl.mascaraEdt ,
            case 
                when substring(pl.mascaraEdt,0,9) = '3.2.1.05' then 'MAO OBRA PRODUCAO' 
                when substring(pl.mascaraEdt,0,9) = '3.2.1.15' then 'GASTOS GERAIS FABRICACAO' 
                when substring(pl.mascaraEdt,0,9) = '3.3.3.10' then 'DESPESAS ADM' 
                when substring(pl.mascaraEdt,0,9) = '3.3.3.05' then 'DESPESAS ADM PESSOAL' 
                ELSE '-' END GRUPO 
        FROM
            ctb.ContaContabil c
        inner join 
            ctb.PlanoContasPadrao pl on
            c.codigo = pl.codContaContabil 
        WHERE 
            SUBSTRING(pl.mascaraEdt, 0, 9) = '3.2.1.05'
            OR SUBSTRING(pl.mascaraEdt, 0, 9) = '3.2.1.15'
            OR SUBSTRING(pl.mascaraEdt, 0, 9) = '3.3.3.10'
            OR SUBSTRING(pl.mascaraEdt, 0, 9) = '3.3.3.05'
        """


        with ConexaoERP.ConexaoInternoMPL() as conn:
            with conn.cursor() as cursor_csw:
                # Executa a primeira consulta e armazena os resultados
                cursor_csw.execute(sql)
                colunas = [desc[0] for desc in cursor_csw.description]
                rows = cursor_csw.fetchall()
                consulta = pd.DataFrame(rows, columns=colunas)
                del rows
        consulta['codContaContabil'] =consulta['codContaContabil'].astype(str)


        return consulta



    def get_GrupoContas(self):

        grupo = self.__getContaContabil()

        grupos_unicos = grupo[['GRUPO']].drop_duplicates().reset_index(drop=True)

        return grupos_unicos


    def __get_intensReqIndependente(self):
        '''Metodo privado que busca no CSW as requisicoes indepedentes'''


        sql = f"""
        SELECT
            CentroCusto as centroCustovalor,
            codCCusto as centrocusto,
            m.codTransacao,
            numDocto as codDocumento,
            dataLcto,
            nomeItem as descricaoItem,
            codItem,
            vlrUnitario ,
            qtdMovto as qtd ,
            vlrTotal as valor
        FROM
            est.Movimento m
        WHERE
            m.codEmpresa = {self.codEmpresa}
            and m.dataLcto >= '{self.dataCompentenciaInicial}'
            and m.dataLcto <= '{self.dataCompentenciaFinal}'
            and numDocto like '%RQI%'
            and codCCusto > 0
        """




        with ConexaoERP.ConexaoInternoMPL() as conn:
            with conn.cursor() as cursor_csw:
                # Executa a primeira consulta e armazena os resultados
                cursor_csw.execute(sql)
                colunas = [desc[0] for desc in cursor_csw.description]
                rows = cursor_csw.fetchall()
                consulta = pd.DataFrame(rows, columns=colunas)
                del rows



        sql2 = """
        SELECT 
		    DISTINCT 
			        contadebito as contaContabil, codTransacao  
			    FROM 
			        Est.CtbIntLctCont e
                WHERE e.codempresa = 1 and e.anoMes like '202%' and codtransacao > 0
        """

        with ConexaoERP.ConexaoInternoMPL() as conn:
            with conn.cursor() as cursor_csw:
                # Executa a primeira consulta e armazena os resultados
                cursor_csw.execute(sql2)
                colunas = [desc[0] for desc in cursor_csw.description]
                rows = cursor_csw.fetchall()
                sql2 = pd.DataFrame(rows, columns=colunas)
                del rows
        sql2['codTransacao'] = sql2['codTransacao'].astype(str)
        sql2['ocorrencia_acumulada'] = sql2.groupby('codTransacao').cumcount() + 1

        sql2 = sql2[sql2['ocorrencia_acumulada']>1].reset_index()

        consulta['codTransacao'] = consulta['codTransacao'].astype(str)

        consulta = pd.merge(consulta, sql2, on='codTransacao', how='left')
        consulta['codFornecedor'] = '-'
        consulta['nomeFornecedor'] = '-'
        consulta['seqItemDocumento'] = '-'


        return consulta


    def resumo_centroCusto(self):
        '''Metodo para resumir os gasto por centro de custo no periodo'''

        resumo = self.get_notasEntredas_Csw()
        orcamento = self.gastosOrcamentoBI.get_orcamentoGastos()
        orcamento['centrocusto'] =  orcamento['centrocusto'].astype(str)
        orcamento['valorOrcado'] =  orcamento['valorOrcado'].astype(float)

        centroCusto = self.__get_centroCusto()
        centroCusto['centrocusto'] =centroCusto['centrocusto'].astype(str)
        orcamento = pd.merge(orcamento, centroCusto , on ='centrocusto', how='left')
        print(orcamento)

        resumo['centrocusto'] =  resumo['centrocusto'].astype(str)


        if self.nomeArea != '':

            resumo = resumo[resumo['nomeArea']==self.nomeArea].reset_index()

            orcamento = orcamento[orcamento['nomeArea']==self.nomeArea].reset_index()


        resumo = resumo.groupby('centrocusto').agg({
            'nomeCentroCusto':'first',
            'valor':'sum'
        }).reset_index()



        orcamento = orcamento.groupby('centrocusto').agg({'valorOrcado':'sum'}).reset_index()

        resumo = pd.merge(resumo, orcamento, on='centrocusto', how='outer')


        resumo['valor'] = resumo['valor'].round(2)

        return resumo











