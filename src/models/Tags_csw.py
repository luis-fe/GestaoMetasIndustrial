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

        return consulta