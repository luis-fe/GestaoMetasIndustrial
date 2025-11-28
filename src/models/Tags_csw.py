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
                select 
                    * 
                from 
                    "PCP".pcp."tags_pilotos" 
        """


        conn = ConexaoPostgre.conexaoEngine()
        consulta = pd.read_sql(consulta, conn)

        consulta = consulta.replace('-', np.nan)
        colunas_datas = ['dataBaixa', 'dataRecebimento', 'dataTransferencia', 'DataHoraInvLocal']

        for col in colunas_datas:
            consulta[col] = pd.to_datetime(consulta[col], errors='coerce')

        consulta['tipo considerar'] = consulta[colunas_datas].idxmax(axis=1)
        consulta.loc[consulta[colunas_datas].isna().sum(axis=1) == len(colunas_datas), 'tipo considerar'] = np.nan


        # Regra para obter a OP atrelada na Tag
        # 1. Defina a condição de forma clara
        condicao_avaliativa = (
            # A 'dataBaixa' deve ser ANTERIOR à 'dataTransferencia'
                (consulta['dataBaixa'] < consulta['ultimoInv']) &
                # E a 'dataTransferencia' deve ser válida (não nula/NaN)
                (consulta['ultimoInv'].notna())
        )


        consulta.loc[condicao_avaliativa, 'numeroOP'] = '-'



        consulta.fillna('-',inplace=True)


        # Atribuindo a coluna Status
        consulta['status'] ='-'



    # Verificando se o Status está na Unidade 2
        consulta['status'] = np.where(
            (consulta['dataEntrega'] != '-') & (consulta['dataBaixa'] < consulta['dataEntrega']) ,
            'Piloto na Unid. 2',
            consulta['status']
        )

        consulta['status'] = np.where(
            (consulta['codBarrasTag_nao_retorno'] == '01000000000-Piloto nao retornada')&(consulta['dataBaixa'] < consulta['dataEntrega']) ,
            'Piloto nao retornada !',
            consulta['status']
        )


        consulta['numeroOP'] = np.where(
            consulta['status'] == 'Piloto na Unid. 2' ,
            '-',
            consulta['numeroOP']
        )



        # --------- CONDICAO PARA ACERTAR O QUE FOI TRANSFERIDO APOS A DATA DE SAIDA PARA A FACCAO


        # 1. Defina a condição de forma clara
        condicao_em_transito = (
            # A 'dataBaixa' deve ser ANTERIOR à 'dataTransferencia'
                (consulta['dataBaixa'] < consulta['dataTransferencia']) &
                # E a 'dataTransferencia' deve ser válida (não nula/NaN)
                (consulta['dataTransferencia'].notna())
        )


        condicao_nova = (
            # dataBaixa é igual a '-'
                (consulta['dataBaixa'] == '-') &
                # DataHoraInvLocal é diferente de '-'
                (consulta['DataHoraInvLocal'] != '-')
        )

        condicao_em_transito = condicao_em_transito | condicao_nova


        consulta.loc[condicao_em_transito, 'status'] = 'em transito'


        condicao_em_montagem = (
            # A 'dataBaixa' deve ser ANTERIOR à 'dataTransferencia'
                (consulta['dataBaixa'] < consulta['dataRecebimento']) &
                # E a 'dataTransferencia' deve ser válida (não nula/NaN)
                (consulta['dataRecebimento'].notna())

        )



        # --------- CONDICAO PARA ACERTAR O QUE FOI INVENTARIADO APOS A DATA DE SAIDA PARA A FACCAO
        consulta.loc[condicao_em_montagem, 'status'] = 'na Montagem'

        # 1. Condição Anterior (dataBaixa ANTERIOR à DataHoraInvLocal, e DataHoraInvLocal válida)
        condicao_anterior = (
                (consulta['dataBaixa'] < consulta['DataHoraInvLocal']) &
                (consulta['DataHoraInvLocal'].notna())
        )

        # 2. Nova Condição
        condicao_nova = (
            # dataBaixa é igual a '-'
                (consulta['dataBaixa'] == '-') &
                # DataHoraInvLocal é diferente de '-'
                (consulta['DataHoraInvLocal'] != '-')
        )

        # 3. Combinação das Condições: (Condição Anterior) OU (Nova Condição)
        condicao_em_montagem_total = condicao_anterior | condicao_nova

        # 4. Atribuição do novo status para as linhas que satisfazem a condição total
        consulta.loc[condicao_em_montagem_total, 'status'] = consulta['localInv']

        # 5. Substituição final (mantida do código original)
        consulta['status'] = consulta['status'].replace('Montagem', 'na Montagem')

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




