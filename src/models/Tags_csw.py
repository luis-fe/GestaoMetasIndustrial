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
                "PCP".pcp."tags_piloto_csw_2" 
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

        # 1. Defina a condição de forma clara
        condicao_em_transito = (
            # A 'dataBaixa' deve ser ANTERIOR à 'dataTransferencia'
                (consulta['dataBaixa'] < consulta['dataTransferencia']) &
                # E a 'dataTransferencia' deve ser válida (não nula/NaN)
                (consulta['dataTransferencia'].notna())
        )
        consulta.loc[condicao_em_transito, 'status'] = 'em transito'


        condicao_em_montagem = (
            # A 'dataBaixa' deve ser ANTERIOR à 'dataTransferencia'
                (consulta['dataBaixa'] < consulta['dataRecebimento']) &
                # E a 'dataTransferencia' deve ser válida (não nula/NaN)
                (consulta['dataRecebimento'].notna())
        )
        consulta.loc[condicao_em_montagem, 'status'] = 'na Montagem'



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




