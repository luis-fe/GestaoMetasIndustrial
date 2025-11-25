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




        ultimamov = self.__ultima_saida_tercerizado()

        consulta = pd.merge(consulta, ultimamov, on='codBarrasTag', how='left')




        # Converter novamente para string formatada
      #  consulta['dataHoraFase'] = consulta['dataHoraFase'].dt.strftime('%Y-%m-%d %H:%M')



        consulta.fillna('-',inplace=True)

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



    def __ultima_saida_tercerizado(self):

        sql = """
        SELECT
            observacao1 as codBarrasTag,
            m.numeroOP,
            observacao10,
            nomeFase, m2.dataBaixa 
        FROM
            tco.RoteiroOP m
        left join tco.MovimentacaoOPFase m2 on m2.codEmpresa = 1 
            and m2.numeroOP = m.numeroOP  
            and m2.codFase = m.codFase 
        WHERE
            m.codEmpresa = 1
            and m.observacao1 like '0%'
            and m.codFase in (406, 428, 425 )
            and m2.codFase in (406, 428, 425 )
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

        # --- Extrai data/hora, separa data e hora ---
        consulta['dataHoraFase'] = consulta['observacao10'].str.extract(r'(\d{2}/\d{2}/\d{4}\s\d{2}:\d{2})')
        consulta['dataHoraFase'] = pd.to_datetime(consulta['dataHoraFase'], format='%d/%m/%Y %H:%M')
        consulta['dataFase'] = consulta['dataHoraFase'].dt.date
        consulta['dataBaixa'] = pd.to_datetime(consulta['dataBaixa'], format='%Y-%m-%d ')

        consulta['horaFase'] = consulta['dataHoraFase'].dt.time
        consulta['observacao10'] = consulta['observacao10'].str.replace(r'\d{2}/\d{2}/\d{4}\s\d{2}:\d{2}', '',
                                                            regex=True).str.strip()

        # --- Remove duplicadas, mantendo a última movimentação ---
        consulta = (
            consulta
            .sort_values('dataBaixa')
            .drop_duplicates(subset='codBarrasTag', keep='last')
            .reset_index(drop=True)
        )


        consulta['dataFase'] = consulta['dataHoraFase'].astype(str).str.split(' ').str[0]
        consulta['horaFase'] = consulta['dataHoraFase'].astype(str).str.split(' ').str[1]
        consulta['dataBaixa'] = consulta['dataBaixa'].dt.strftime('%Y-%m-%d')

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




