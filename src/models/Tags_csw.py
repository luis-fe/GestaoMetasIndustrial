import gc
import pandas as pd
from src.connection import ConexaoERP


class Tag_Csw():
    '''Classe para buscar as tags do Csw '''


    def __init__(self, codEmpresa = '1'):

        self.codEmpresa = codEmpresa

    def buscar_tags_csw_estoque_pilotos(self):


        consulta = f"""
        SELECT
            t.codBarrasTag,
            t.codEngenharia,
            (select s.corbase from tcp.SortimentosProduto s where s.codempresa = 1 
            and s.codproduto = t.codEngenharia and t.codSortimento = s.codsortimento)
            as cor,
            (select s.descricao from tcp.Tamanhos s where s.codempresa = 1 
            and s.sequencia = t.seqTamanho)
            as tamanho,
            (select s.descricao from tcp.Engenharia s where s.codempresa = 1 
            and s.codengenharia = t.codEngenharia)
            as descricao
        FROM
            tcr.TagBarrasProduto t
        WHERE
            t.codEmpresa = {self.codEmpresa}
            and t.situacao = 3
            and t.codNaturezaAtual = 24
        """


        with ConexaoERP.ConexaoInternoMPL() as conn:
            with conn.cursor() as cursor:
                cursor.execute(consulta)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                consulta = pd.DataFrame(rows, columns=colunas)

        # Libera memória manualmente
        del rows
        gc.collect()


        inventario = self.__ultimo_inventario_tag()

        consulta = pd.merge(consulta, inventario, on='codBarrasTag', how='left')
        consulta.fillna('-',inplace=True)

        return consulta



    def __ultimo_inventario_tag(self):


        sql = """
        SELECT 
            convert(varchar(40),t.codBarrasTag) as codBarrasTag, 
            ip.dataEncContagem as ultimoInv
        FROM tci.InventarioProdutosTagLidas t
        INNER JOIN tci.InventarioProdutos ip 
            ON ip.Empresa = 1 
            AND t.inventario = ip.inventario 
        WHERE t.Empresa = 1 
          AND ip.codnatureza = 24 
          AND ip.situacao = 4
          AND ip.dataEncContagem = (
                SELECT MAX(ip2.dataEncContagem)
                FROM tci.InventarioProdutos ip2
                WHERE ip2.Empresa = ip.Empresa
                  AND ip2.codnatureza = ip.codnatureza
            );
        """

        with ConexaoERP.ConexaoInternoMPL() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                consulta = pd.DataFrame(rows, columns=colunas)

        # Libera memória manualmente
        del rows
        gc.collect()

        return consulta