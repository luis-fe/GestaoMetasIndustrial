import numpy as np
import pandas as pd
import pytz
import datetime
from src.models import Tags_csw, Colaboradores_TI_MPL
from src.connection import ConexaoPostgre


class ControlePilotos():

    def __init__(self, codEmpresa = '1',codbarrastag = '', matricula = '', documento = '', ):

        self.codEmpresa = codEmpresa

        self.codbarrastag = codbarrastag
        self.tags_csw = Tags_csw.Tag_Csw(self.codEmpresa, self.codbarrastag)

        self.matricula = matricula
        self.dataHora, self.dataAtual = self.__obterHoraAtual()
        self.documento = documento

    def get_tags_piloto(self):
        '''Metodo para levantar as tags das pilotos'''

        consulta = self.tags_csw.buscar_tags_csw_estoque_pilotos()
        consulta['numeroOP'].fillna('-', inplace=True)
        consulta['EstoquePiloto'] = consulta['codBarrasTag'].count()
        consulta['PilotoUnd2'] = (consulta['status'] == 'Piloto na Unid. 2').sum()
        consulta.fillna('-',inplace=True)
        return consulta

    def transferir_pilotos(self):
        '''Metodo para transferir piloto'''

        validacao = self.verificar_tag_estoque()

        if not validacao.empty:

            veriicaTag_no_Doc = self.verificar_se_Tag_esta_no_doc()

            if veriicaTag_no_Doc.empty:
                self.verificar_se_Tag_esta_EMOUTRO_doc()

                sql = '''
                insert into pcp."transacaoPilotos" (codbarrastag, "tipoTransacao", matricula, "dataTransferencia", documento )
                values ( %s, 'Transferencia', %s, %s, %s ) 
                '''

                with ConexaoPostgre.conexaoInsercao() as conn:
                    with conn.cursor() as curr:

                        curr.execute(sql,(self.codbarrastag, self.matricula, self.dataHora, self.documento))
                        conn.commit()

                return pd.DataFrame([{'Status':True , 'Mensagem': 'tag transferida'}])

            else:
                return pd.DataFrame(
                    [{'Status': False, 'Mensagem': f'Tag{self.codbarrastag} tag ja bipada nesse documento PILOTOS'}])


        else:
            return pd.DataFrame([{'Status':False , 'Mensagem': f'Tag{self.codbarrastag} nao esta no estoque de PILOTOS'}])

    def receber_pilotos(self):
        '''Metodo para transferir piloto'''

        sql = '''
            update pcp."transacaoPilotos"  
            set "tipoTransacao" = 'Recebida' , "dataRecebimento" = %s, matricula_receb = %s
            where 
            tipoTransacao = 'Transferencia'
            and codbarrastag = %s
        '''

        with ConexaoPostgre.conexaoInsercao() as conn:
            with conn.cursor() as curr:
                curr.execute(sql, (self.dataHora, self.matricula, self.dataHora))
                conn.commit()

        return pd.DataFrame([{'Status': True, 'Mensagem': 'tag transferida'}])


    def get_pilotos_em_transito(self):
        '''Metodo que obtem as pilotos que estao em transito'''

        consulta = '''
        select 
            codbarrastag, "dataTransferencia", "matricula"
        from
            pcp."transacaoPilotos" 
        where 
            "tipoTransacao" = 'Transferencia'
        '''

        conn = ConexaoPostgre.conexaoEngine()
        consulta = pd.read_sql(consulta, conn, params=(self.documento,))

        colab = Colaboradores_TI_MPL.Colaboradores().get_colaborador()
        colab['matricula'] =  colab['id']

        consulta = pd.merge(consulta, colab , on = 'matricula' , how = 'left')
        consulta.fillna('-', inplace=True)

        # 1. Divide a string em uma lista de palavras, limitando a 2 divisões (max=2)
        #    Isso resulta em uma lista de 3 elementos: [primeira palavra, segunda palavra, resto da string]
        consulta['nome'] = consulta['nome'].str.split(' ', n=2)

        # 2. Seleciona apenas os dois primeiros elementos da lista (índices 0 e 1)
        #    E os junta novamente com um espaço.
        consulta['nome'] = consulta['nome'].str[:2].str.join(' ')


        return consulta


    def gerarCodigoDocumento(self):
        '''metodo que gera o codigo de documento '''

        select = f'''
        select 
            max(SPLIT_PART(documento, '/', 1)::int) AS codigo
        from 
            pcp."transacaoPilotos" 
        where 
            "dataTransferencia"::date = '{self.dataAtual}'
        '''
        print(select)
        conn = ConexaoPostgre.conexaoEngine()
        consulta = pd.read_sql(select, conn)
        print(consulta['codigo'][0])

        if consulta['codigo'][0] == None:
            novoDoc = '1'

        else:
            novoDoc = str(int(consulta['codigo'][0]) + 1)

        novoDoc = novoDoc +'/'+ self.dataAtual

        return novoDoc

    def __obterHoraAtual(self):
        fuso_horario = pytz.timezone('America/Sao_Paulo')  # Define o fuso horário do Brasil
        agora = datetime.datetime.now(fuso_horario)
        hora_str = agora.strftime('%Y-%m-%d %H:%M:%S')
        dia = agora.strftime('%Y-%m-%d')
        return hora_str, dia


    def get_tags_transferidas_documento_atual(self):
        '''Metodo que obtem as tags transferiadas no documento atual '''


        consulta = '''
        select 
            codbarrastag, "dataTransferencia"
        from
            pcp."transacaoPilotos" 
        where 
            documento = %s
        '''

        conn = ConexaoPostgre.conexaoEngine()
        consulta = pd.read_sql(consulta, conn, params=(self.documento,))

        return consulta


    def obter_documentos_transferencia_emaberto(self):
        '''Metodo que obtem os documento '''

        consulta = '''
        select 
            distinct documento
        from
            pcp."transacaoPilotos" 
        where 
            "tipoTransacao" = 'Transferencia'
        '''

        conn = ConexaoPostgre.conexaoEngine()
        consulta = pd.read_sql(consulta, conn)

        return consulta



    def verificar_tag_estoque(self):

        validar = self.tags_csw.validar_tag_estoque_piloto()


        return  validar


    def verificar_se_Tag_esta_no_doc(self):

        sql = """
            select 
                codbarrastag 
            from 
                "PCP".pcp."transacaoPilotos" tp
            where tp.codbarrastag = %s and tp.documento = %s        
        """

        conn = ConexaoPostgre.conexaoEngine()

        consulta = pd.read_sql(sql,conn,params=(self.codbarrastag, self.documento,))

        return consulta


    def verificar_se_Tag_esta_EMOUTRO_doc(self):

        sql = """
            select 
                codbarrastag 
            from 
                "PCP".pcp."transacaoPilotos" tp
            where tp.codbarrastag = %s        
        """

        conn = ConexaoPostgre.conexaoEngine()

        consulta = pd.read_sql(sql,conn,params=(self.codbarrastag,))


        if not consulta.empty:

            with ConexaoPostgre.conexaoInsercao() as conn:
                with conn.cursor() as curr:

                    delete = """
                    delete from "PCP".pcp."transacaoPilotos" t
                    where t.codbarrastag = %s
                    """

                    curr.execute(delete,(self.codbarrastag))
                    conn.commit()

















