import pandas as pd
from src.connection import ConexaoPostgre

class Faccionista():

    def __init__(self, codEmpresa= '1'):

        self.codEmpresa = codEmpresa

    def consultarCategoriaMetaFaccionista_S(self):

        conn = ConexaoPostgre.conexaoEngine()


        '''Metodo para consultar a Capacidade a nivel de categoria doS faccionista(s)
         return:
         DataFrame : [{codfaccionista, categoria, "Capacidade/dia", "nomefaccionistaCsw"}] **N Linha(s)
         '''

        select = """
        select 
            nomecategoria as categoria, 
            fc.codfaccionista, 
            "Capacidade/dia"::int, 
            nomefaccionista as "nomefaccionistaCsw", 
            apelidofaccionista
        from 
            pcp."faccaoCategoria" fc
        inner join 
            pcp."faccionista" f on f.codfaccionista = fc.codfaccionista
        """
        consulta = pd.read_sql(select, conn)

        return consulta