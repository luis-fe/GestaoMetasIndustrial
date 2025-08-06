import gc
import pandas as pd
from src.connection import ConexaoERP

class Faccionista_CSW():
    '''Classe Faccionista: definida para instanciar o objeto faccionista ou faccionista(s)'''
    def __init__(self,codfaccionista = None, apelidofaccionista= None, nomecategoria = None, Capacidade_dia = None ):
        '''Construtor da classe, quando oculto os atributos subentende que trata-se de faccionita(s)'''
        self.codfaccionista = codfaccionista
        self.nomefaccionista = None
        self.apelidofaccionista = apelidofaccionista
        self.nomecategoria = nomecategoria
        self.Capacidade_dia = Capacidade_dia
    def obterNomeCSW(self):
        '''Metodo  para obter nome dos faccionistas no csw
        return:
        string: self.nomeFaccionista  - identifica qual o nome o faccionista no csw de acordo com o sef.codFaccionista
        '''

        # 1 - SQL
        sql = """SELECT
        	f.codFaccionista ,
        	f.nome as nomeFaccionista
        FROM
        	tcg.Faccionista f
        WHERE
        	f.Empresa = 1 order by nome """
        with ConexaoERP.ConexaoInternoMPL() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(sql)
                    colunas = [desc[0] for desc in cursor.description]
                    rows = cursor.fetchall()
                    consulta = pd.DataFrame(rows, columns=colunas)

            # Libera memória manualmente
        del rows
        gc.collect()

        consulta = consulta[consulta['codFaccionista']==int(self.codfaccionista)].reset_index()
        self.nomeFaccionista = consulta['nomeFaccionista'][0]

        return self.nomeFaccionista