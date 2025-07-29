import numpy as np
import pandas as pd

from src.connection import ConexaoERP
from src.models import GastosOrçamentoBI


class Gastos_centroCusto_CSW():
    '''Classe que captura as informacoes de gastos e centro de custo '''

    def __init__(self, codEmpresa = '1' , dataCompentenciaInicial= '',dataCompentenciaFinal= '',
                 codFornecedor = '', nomeFornecedor= '', dataLcto ='', codDocumento = '',
                 seqItemDocumento = '', descricaoItem = '', centroCustovalor ='', codContaContabil = '', nomeItem ='',
                 codCentroCusto = '', nomeCentroCusto = '',  nomeArea ='', grupo = ''
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
        if self.nomeCentroCusto != '':
            self.__pesquisarCC_peloNome()

        self.nomeArea = nomeArea
        self.grupo = grupo

        self.gastosOrcamentoBI = GastosOrçamentoBI.GastosOrcamentoBI(self.codEmpresa, self.dataCompentenciaInicial, self.dataCompentenciaFinal)


    def __pesquisarCC_peloNome(self):
        '''Metodo privado que pesquisa o codigo do centro de custo pelo nomeCentroCusto'''

        consulta = self.get_centro_custo()

        consulta = consulta[consulta['nomeCentroCusto']==self.nomeCentroCusto].reset_index()

        self.codCentroCusto = consulta['centrocusto'][0]





    def get_notasEntredas_Csw(self):
        '''Metodo que captura as notas de entrda do CSW'''




        if self.nomeCentroCusto == '':
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
                    and e.dataEntrada  <= '{self.dataCompentenciaFinal}'
                    and ei.centroCustoValor > 0
            """

        else:

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
                                and e.dataEntrada  <= '{self.dataCompentenciaFinal}'
                                and ei.centroCustoValor like '%%{str(self.codCentroCusto)}%%'
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
            pares = []
            try:
                valores = row['centroCustovalor'].split(';')
                for i in range(0, len(valores), 2):
                    if i + 1 < len(valores):
                        nova_linha = row.to_dict()
                        nova_linha['centrocusto'] = valores[i]
                        nova_linha['valor'] = valores[i + 1]
                        pares.append(nova_linha)
            except Exception as e:
                print(f"Erro ao processar linha: {row} - Erro: {e}")
            return pares

        # Aplica a função
        linhas_expandida = sum(consulta.apply(extrair_pares, axis=1), [])
        consulta = pd.DataFrame(linhas_expandida)
        centroCusto = self.__get_centroCusto()
        contacontb = self.__getContaContabil()

        consulta['qtd'] = ''
        consulta['vlrUnitario'] = ''
        consulta['codItem'] = ''
        if 'valor' in consulta.columns:
            consulta['valor'] = consulta['valor'].astype(float)
        else:
            consulta['valor'] = 0
        consulta['valor'] =consulta['valor']/100000

        consulta2 = self.__get_intensReqIndependente()
        consulta3 = self.__getSalarios()

        consulta = pd.concat([consulta, consulta2,consulta3])

        consulta['centrocusto'] =consulta['centrocusto'].astype(str)
        centroCusto['centrocusto'] =centroCusto['centrocusto'].astype(str)

        consulta = pd.merge(consulta, centroCusto , on ='centrocusto', how='left')
        consulta['codContaContabil'] =consulta['codContaContabil'].astype(str)


        consulta = pd.merge(consulta, contacontb , on ='codContaContabil',how='left')

        consulta.fillna('-', inplace=True)




        if self.nomeCentroCusto != '':
            print(self.nomeCentroCusto)
            print(self.codCentroCusto)
            consulta = consulta[consulta['centrocusto']==str(self.codCentroCusto)].reset_index()


        if self.grupo != '':
            consulta = consulta[consulta['GRUPO']==str(self.grupo)].reset_index(drop=True)



        consulta = consulta[consulta['GRUPO']!='SERVIÇO INDUSTRIALIZACAO'].reset_index(drop=True)
        consulta = consulta[consulta['GRUPO']!='MATÉRIA PRIMA'].reset_index(drop=True)
        consulta.drop(['index', 'mascaraEdt','codTransacao','centroCustovalor','codArea','ocorrencia_acumulada','seqItemDocumento'], axis=1, inplace=True)

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
            cad.CCustoArea a on a.codigo = c.codArea and a.codempresa = c.codempresa
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


        data2 = {'codContaContabil':['3151','3201','3323','3330','3411','3466'],
                 "nomeContaContabil":["Serviços de Industrialização",'MATÉRIA PRIMA','Salarios e Ordenados','Horas extras','Manutencao de Veiculos','Serviço Técnico Profissionais'],
                 "mascaraEdt":['-','-','-','-','-','-'],
                 "GRUPO":['SERVIÇO INDUSTRIALIZACAO','MATÉRIA PRIMA','DESPESAS ADM PESSOAL','DESPESAS ADM PESSOAL','GASTOS GERAIS FABRICACAO','GASTOS GERAIS FABRICACAO']}
        consulta2 = pd.DataFrame(data2)

        consulta = pd.concat([consulta,consulta2])


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
			        contadebito as codContaContabil, codTransacao  
			    FROM 
			        Est.CtbIntLctCont e
                WHERE e.codempresa = 1 and e.anoMes like '202%' and codtransacao > 0
                ORDER BY E.valor DESC
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

        sql2 = sql2[sql2['ocorrencia_acumulada']==1].reset_index()

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
        contacontb = self.__getContaContabil()

        centroCusto['centrocusto'] =centroCusto['centrocusto'].astype(str)
        orcamento = pd.merge(orcamento, centroCusto , on ='centrocusto', how='left')
        orcamento = pd.merge(orcamento, contacontb , on ='codContaContabil', how='left')


        resumo['centrocusto'] =  resumo['centrocusto'].astype(str)


        if self.nomeArea != '':

            resumo = resumo[resumo['nomeArea']==self.nomeArea].reset_index()

            orcamento = orcamento[orcamento['nomeArea']==self.nomeArea].reset_index()


        if self.grupo != '':

            resumo = resumo[resumo['GRUPO']==self.grupo].reset_index(drop=True)

            orcamento = orcamento[orcamento['GRUPO']==self.grupo].reset_index(drop=True)


        resumo = resumo.groupby('centrocusto').agg({
            'valor':'sum'
        }).reset_index()




        orcamento = orcamento.groupby('centrocusto').agg({'valorOrcado':'sum'}).reset_index()
        print(orcamento)

        resumo = pd.merge(resumo, orcamento, on='centrocusto', how='right')
        print(resumo)

        print(centroCusto[centroCusto['centrocusto']=='21110210'])

        resumo = pd.merge(resumo, centroCusto, on='centrocusto', how='left')

        resumo['valor'] = resumo['valor'].round(2)
        resumo['valorOrcado'] = resumo['valorOrcado'].round(2)
        resumo['valorOrcado'].fillna(0, inplace=True)
        resumo['valor'].fillna(0, inplace=True)


        return resumo


    def resumo_contacontabil(self):
        '''Metodo para resumir os gasto por centro de custo no periodo'''

        resumo = self.get_notasEntredas_Csw()
        orcamento = self.gastosOrcamentoBI.get_orcamentoGastos()
        orcamento['centrocusto'] =  orcamento['centrocusto'].astype(str)
        orcamento['valorOrcado'] =  orcamento['valorOrcado'].astype(float)

        centroCusto = self.__get_centroCusto()
        contacontb = self.__getContaContabil()

        centroCusto['centrocusto'] =centroCusto['centrocusto'].astype(str)
        orcamento = pd.merge(orcamento, centroCusto , on ='centrocusto', how='left')
        orcamento = pd.merge(orcamento, contacontb , on ='codContaContabil', how='left')


        resumo['centrocusto'] =  resumo['centrocusto'].astype(str)


        if self.nomeArea != '':

            resumo = resumo[resumo['nomeArea']==self.nomeArea].reset_index()

            orcamento = orcamento[orcamento['nomeArea']==self.nomeArea].reset_index()


        if self.grupo != '':

            resumo = resumo[resumo['GRUPO']==self.grupo].reset_index(drop=True)

            orcamento = orcamento[orcamento['GRUPO']==self.grupo].reset_index(drop=True)


        resumo = resumo.groupby(['centrocusto','codContaContabil']).agg({
            'valor':'sum',

        }).reset_index()




        orcamento = orcamento.groupby(['centrocusto','codContaContabil']).agg({'valorOrcado':'sum'}).reset_index()
        print(orcamento)

        resumo = pd.merge(resumo, orcamento, on=['centrocusto','codContaContabil'], how='right')
        print(resumo)

        print(centroCusto[centroCusto['centrocusto']=='21110210'])

        resumo = pd.merge(resumo, centroCusto, on='centrocusto', how='left')
        resumo = pd.merge(resumo, contacontb, on='codContaContabil', how='left')

        resumo['valor'] = resumo['valor'].round(2)
        resumo['valorOrcado'] = resumo['valorOrcado'].round(2)
        resumo['valorOrcado'].fillna(0, inplace=True)
        resumo['valor'].fillna(0, inplace=True)
        resumo.fillna('-',inplace=True)
        resumo = resumo[resumo['GRUPO']!='-']


        if self.nomeCentroCusto != '':
            resumo = resumo[resumo['nomeCentroCusto']==self.nomeCentroCusto].reset_index(drop=True)




        return resumo




    def __getSalarios(self):


        sql = f"""
        SELECT 
                CONVERT(varchar(10), codcentrocusto) as centrocusto,
                CONVERT(varchar(10), codcentrocusto) as centroCustovalor,
                codcontacontabil as codContaContabil,
                m.data as dataLcto,
                (totalDebito - totalCredito) as valor
        FROM
            CTB.MovContaCentroCusto m
        WHERE
            m.codEmpresa = {self.codEmpresa}
            and m.codContaContabil in (3063, 3323, 3174, 3323, 3081, 3084 ,3083, 3069, 3080, 3085 , 3071, 3188)
            and m.data >= '{self.dataCompentenciaInicial}'
            and m.data <= '{self.dataCompentenciaFinal}'
        UNION 
                SELECT 
                CONVERT(varchar(10), codcentrocusto) as centrocusto,
                CONVERT(varchar(10), codcentrocusto) as centroCustovalor,
                codcontacontabil as codContaContabil,
                m.data as dataLcto,
                ( - totalCredito) as valor
        FROM
            CTB.MovContaCentroCusto m
        WHERE
            m.codEmpresa = {self.codEmpresa}
            and m.codContaContabil not in (3063, 3323, 3174, 3323, 3081, 3084 ,3083, 3069, 3080, 3085, 3071, 3188 )
            and m.data >= '{self.dataCompentenciaInicial}'
            and m.data <= '{self.dataCompentenciaFinal}'
            and totalCredito > 0
        """


        with ConexaoERP.ConexaoInternoMPL() as conn:
            with conn.cursor() as cursor_csw:
                # Executa a primeira consulta e armazena os resultados
                cursor_csw.execute(sql)
                colunas = [desc[0] for desc in cursor_csw.description]
                rows = cursor_csw.fetchall()
                consulta = pd.DataFrame(rows, columns=colunas)
                del rows

        consulta['descricaoItem'] = 'Pagamento de Salario'

        consulta['descricaoItem'] = np.where(consulta['valor'] < 0, 'credito na conta', consulta['descricaoItem'])
        consulta['descricaoItem'] = np.where(consulta['codContaContabil'] == '3179', 'credito Manutencao de Softwares', consulta['descricaoItem'])
        consulta['descricaoItem'] = np.where(consulta['codContaContabil'] == '3189', 'credito Manuutencao de veiculos', consulta['descricaoItem'])
        consulta['descricaoItem'] = np.where(consulta['codContaContabil'] == '3174', 'DEPRECIACAO', consulta['descricaoItem'])

        consulta['codTransacao'] = '-'
        consulta['codDocumento'] = '-'
        consulta['codItem'] = '-'
        consulta['vlrUnitario'] = consulta['valor']
        consulta['qtd'] = 1


        return consulta












