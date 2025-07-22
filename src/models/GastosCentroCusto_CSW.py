import pandas as pd

from src.connection import ConexaoERP


class Gastos_centroCusto_CSW():
    '''Classe que captura as informacoes de gastos e centro de custo '''

    def __init__(self, codEmpresa = '1' , dataCompentenciaInicial= '',dataCompentenciaFinal= '',
                 codFornecedor = '', nomeFornecedor= '', dataEntradaNF ='', codDocumento = '',
                 seqItemDocumento = '', descricaoItem = '', centroCustovalor ='', codContaContabil = '', nomeItem ='',
                 codCentroCusto = '', nomeCentroCusto = ''
                 ):

        self.codEmpresa = str(codEmpresa)
        self.dataCompentenciaInicial = str(dataCompentenciaInicial)
        self.dataCompentenciaFinal = str(dataCompentenciaFinal)
        self.codFornecdor = codFornecedor
        self.nomeFornecedor = nomeFornecedor
        self.dataEntradaNF = dataEntradaNF
        self.codDocumento = codDocumento
        self.seqItemDocumento = seqItemDocumento
        self.descricaoItem = descricaoItem
        self.centroCustovalor = centroCustovalor
        self.codContaContabil = codContaContabil
        self.nomeItem = nomeItem
        self.codCentroCusto = codCentroCusto
        self.nomeCentroCusto = nomeCentroCusto


    def get_notasEntredas_Csw(self):
        '''Metodo que captura as notas de entrda do CSW'''


        sql = f"""
            SELECT
                e.fornecedor as codFornecedor,
                f.nome as nomeFornecedor,
                e.dataEntrada as dataEntradaNF,
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
        consulta = pd.merge(consulta, centroCusto , on ='centrocusto')

        consulta.fillna('-', inplace=True)

        return consulta



    def __get_centroCusto(self):
        '''Metodo que obtem os centro de custos cadastrados no erp cesw'''


        sql = """
        select
            c.mascaraRdz as centrocusto,
            c.nome as nomeCentroCusto, 
            c.codarea, 
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

        return consulta



    def get_centro_custo(self):
        '''Metodo publico para obter os centro de custos cadastrddos no ERP CSW'''


        consulta = self.__get_centroCusto()

        consulta['nomeArea'] = consulta.apply(lambda r: 'PRODUCAO' if r['nomeArea'] == 'CONFECÇÃO' else r['nomeArea'],
                                              axis=1)


        return consulta


    def get_Empresa(self):


        empresa = pd.DataFrame([{'codEmpresa': ['1','4'], 'nomeEmpresa': ['Matriz','Filial-Cianorte']}])


        return empresa










