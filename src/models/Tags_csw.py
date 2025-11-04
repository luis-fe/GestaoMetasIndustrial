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
        ultimamov = self._ultima_saida_tercerizado()

        consulta = pd.merge(consulta, inventario, on='codBarrasTag', how='left')
        consulta = pd.merge(consulta, ultimamov, on='codBarrasTag', how='left')


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
            )
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



    def _ultima_saida_tercerizado(self):

        sql = """
        	SELECT
                observacao1 as codbarrastag,
                numeroOP,
                observacao10,
                nomeFase 
            FROM
                tco.RoteiroOP m
            WHERE
                m.codEmpresa = 1
                and m.observacao1 like '0%'
                and m.codFase in (406, 429, 425 )
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

        # --- Extrai data/hora, separa data e hora ---
        consulta['dataHoraFase'] = consulta['observacao10'].str.extract(r'(\d{2}/\d{2}/\d{4}\s\d{2}:\d{2})')
        consulta['dataHoraFase'] = pd.to_datetime(consulta['dataHoraFase'], format='%d/%m/%Y %H:%M')
        consulta['dataFase'] = consulta['dataHoraFase'].dt.date
        consulta['horaFase'] = consulta['dataHoraFase'].dt.time
        consulta['observacao10'] = consulta['observacao10'].str.replace(r'\d{2}/\d{2}/\d{4}\s\d{2}:\d{2}', '',
                                                            regex=True).str.strip()

        # --- Remove duplicadas, mantendo a última movimentação ---
        consulta = consulta.sort_values('dataHoraFase').drop_duplicates(subset='codBarrasTag', keep='last')

        return consulta