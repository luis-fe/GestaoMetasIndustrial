import numpy as np

from src.models import Tags_csw



class ControlePilotos():

    def __init__(self, codEmpresa = '1'):

        self.codEmpresa = codEmpresa
        self.tags_csw = Tags_csw.Tag_Csw(self.codEmpresa)

    def get_tags_piloto(self):
        '''Metodo para levantar as tags das pilotos'''

        consulta = self.tags_csw.buscar_tags_csw_estoque_pilotos()
        consulta['numeroOP'].fillna('-', inplace=True)
        consulta['EstoquePiloto'] = consulta['codBarrasTag'].count()

        return consulta



