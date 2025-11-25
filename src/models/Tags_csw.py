import gc

import numpy as np
import pandas as pd
from src.connection import ConexaoERP, ConexaoPostgre


class Tag_Csw():
    '''Classe para buscar as tags do Csw '''


    def __init__(self, codEmpresa = '1', codbarrastag =''):

        self.codEmpresa = codEmpresa

        self.codbarrastag = codbarrastag
    def buscar_tags_csw_estoque_pilotos(self):


        consulta = f"""
                select * from 
                "PCP".pcp."tags_piloto_csw" 
        """


        conn = ConexaoPostgre.conexaoEngine()
        consulta = pd.read_sql(consulta, conn)





        consulta['numeroOP'] = np.where(
            # Condição: dataBaixa > ultimoInventario E ultimoInventario NÃO é nulo (caso do '-')
            (consulta['dataBaixa'] > consulta['ultimoInv']) & (consulta['ultimoInv'].notna()),

            # Se V: Mantém o valor original de 'numeroOP'
            consulta['numeroOP'],

            # Se F: Substitui por '-' (engloba as outras duas condições: menor ou igual E ultimoInventario é '-')
            '-'
        )


        retornoPilotos = self.__ultimo_retorno_tercerizado()
        pilotoNRetornada = self.piloto_nao_retornada()

        consulta = pd.merge(consulta, retornoPilotos, on='numeroOP', how='left')
        consulta = pd.merge(consulta, pilotoNRetornada, on='numeroOP', how='left')

        consulta.fillna('-',inplace=True)
        consulta['status'] ='-'

        consulta['status'] = np.where(
            consulta['dataEntrega'] != '-' ,
            'Piloto na Unid. 2',
            consulta['status']
        )

        consulta['status'] = np.where(
            consulta['codBarrasTag_nao_retorno'] == '01000000000-Piloto nao retornada' ,
            'Piloto nao retornada !',
            consulta['status']
        )


        consulta['numeroOP'] = np.where(
            consulta['status'] == 'Piloto na Unid. 2' ,
            '-',
            consulta['numeroOP']
        )



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






    def __ultimo_retorno_tercerizado(self):

        sql = """
        SELECT
            observacao1 as codBarrasTag_retorno,
            m.numeroOP,
            observacao10 as ob10 ,
            nomeFase, m2.dataBaixa as dataEntrega
        FROM
            tco.RoteiroOP m
        left join tco.MovimentacaoOPFase m2 on m2.codEmpresa = 1 
            and m2.numeroOP = m.numeroOP  
            and m2.codFase = m.codFase 
        WHERE
        	m.observacao1 like '0%'
        	and m.observacao1 not like '%Piloto na%'
        	and m.numeroOP like '%-001'
            and m.codEmpresa = 1
            and m.codFase in (429, 432, 441 )
            and m2.codFase in (429, 432, 441 )
            AND m2.dataBaixa > DATEADD(day, -500, CURRENT_DATE)
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

    def piloto_nao_retornada(self):


        sql = """
        SELECT
            observacao1 as codBarrasTag_nao_retorno,
            m.numeroOP
        FROM
            tco.RoteiroOP m
        left join tco.MovimentacaoOPFase m2 on m2.codEmpresa = 1 
            and m2.numeroOP = m.numeroOP  
            and m2.codFase = m.codFase 
        WHERE
        	 m.observacao1  like '%Piloto na%'
        	and m.numeroOP like '%-001'
            and m.codEmpresa = 1
            AND m2.dataBaixa > DATEADD(day, -500, CURRENT_DATE)
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



    def validar_tag_estoque_piloto(self):
        '''Metodo que valida se a tag existe no estoque de piloto '''


        sql = f"""
        select 
            codbarrastag 
        from 
            tcr.TagBarrasProduto t
        where 
            codempresa = 1 and t.codbarrastag = '{self.codbarrastag}'
            and codnaturezaatual = 24 and situacao in (3)
        """

        print(sql)

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




