import gc

import pandas as pd
from src.connection import ConexaoERP


class OP_CSW():
    '''Classe utilizada para interagir com informacoes de OP do Csw'''

    def __init__(self, codEmpresa = '1', codLote = ''):
        '''Construtor'''

        self.codEmpresa = str(codEmpresa) # atributo de codEmpresa
        self.codLote = codLote # atributo com o codLote

    def ordemProd_csw_aberto(self):
        ''' metodo utilizado para obter no csw as ops em aberto'''

        # 1: Carregar o SQL das OPS em aberto do CSW
        sqlOrdemAbertoCsw = """
            SELECT 
                op.codLote , 
                codTipoOP , 
                numeroOP, 
                codSeqRoteiroAtual, 
                lot.descricao as desLote, 
                codfaseatual 
            from 
                tco.OrdemProd op 
            inner join 
                tcl.Lote lot 
                on lot.codLote = op.codLote  
                and lot.codEmpresa  = """+self.codEmpresa+""" 
            WHERE
                op.codempresa = """+self.codEmpresa+""" 
                and op.situacao = 3 """


        with ConexaoERP.ConexaoInternoMPL() as conn:
            with conn.cursor() as cursor_csw:
                # Executa a primeira consulta e armazena os resultados
                cursor_csw.execute(sqlOrdemAbertoCsw)
                colunas = [desc[0] for desc in cursor_csw.description]
                rows = cursor_csw.fetchall()
                consulta = pd.DataFrame(rows, columns=colunas)
                del rows

        return consulta

    def dataEntradaFases_emAberto_Csw(self):

        sql = f"""
            SELECT 
                movto.numeroOP as numeroOP, 
                movto.dataBaixa AS EntFase,
                ABS(DATEDIFF(DAY, CAST(GETDATE() AS DATE), CAST(movto.dataBaixa AS DATE))) DiasFase
            FROM 
                tco.MovimentacaoOPFase movto
            INNER JOIN 
                tco.OrdemProd op 
                ON op.codEmpresa = movto.codEmpresa 
                AND op.numeroOP = movto.numeroOP 
                AND op.codSeqRoteiroAtual - 1 = movto.seqRoteiro 
            WHERE 
                movto.codEmpresa = {self.codEmpresa}
                AND op.codEmpresa = {self.codEmpresa}
                AND op.situacao = 3
            """

        with ConexaoERP.ConexaoInternoMPL() as conn:
            with conn.cursor() as cursor_csw:
                # Executa a primeira consulta e armazena os resultados
                cursor_csw.execute(sql)
                colunas = [desc[0] for desc in cursor_csw.description]
                rows = cursor_csw.fetchall()
                consulta = pd.DataFrame(rows, columns=colunas)
                del rows

        return consulta


    def roteiro_ordemProd_csw_aberto(self):
        ''' metodo utilizado para obter no csw o roteiro das ops em aberto'''

        sqlCsw = f"""
            SELECT
            	pri.descricao as prioridade,
            	tcp.descricao,
                r.numeroOP ,
                codSeqRoteiro,
                codFase,
                (
	                SELECT
	                    codtipoop
	                from
	                    tco.OrdemProd o
	                WHERE
	                    o.codempresa = {self.codEmpresa} 
	                    and o.numeroop = r.numeroOP
	                ) as tipoOP
            FROM
                tco.RoteiroOP r
            left join 
            	tco.OrdemProd tco on tco.numeroOP = r.numeroOP 
            	and tco.codEmpresa = {self.codEmpresa}  
            left join 
            	tcp.Engenharia tcp on tcp.codEngenharia = tco.codProduto 
            	and tcp.codEmpresa = {self.codEmpresa}  
            left join tcp.PrioridadeOP pri on pri.Empresa = {self.codEmpresa}  and pri.codPrioridadeOP = tco.codPrioridadeOP 
            WHERE
                r.codEmpresa = {self.codEmpresa}  
                and 
            r.numeroOP in (
			                SELECT
			                    op.numeroOP
			                from
			                    tco.OrdemProd op
			                WHERE
			                    op.codempresa = {self.codEmpresa} 
			                    and op.situacao = 3
			                    and op.codFaseAtual not in (1, 401)
			               )
        """

        with ConexaoERP.ConexaoInternoMPL() as conn:
            with conn.cursor() as cursor_csw:
                # Executa a primeira consulta e armazena os resultados
                cursor_csw.execute(sqlCsw)
                colunas = [desc[0] for desc in cursor_csw.description]
                rows = cursor_csw.fetchall()
                consulta = pd.DataFrame(rows, columns=colunas)
                del rows

        consulta['prioridade'].fillna('NORMAL',inplace=True)

        return consulta

    def consultarLoteEspecificoCsw(self):
        '''Método que consulta o codigo do lote no CSW e retorna o seu nome no CSW'''


        sql = """Select codLote, descricao as nomeLote from tcl.lote where codEmpresa= """ + str(
            self.codEmpresa) + """ and codLote =""" + "'" + str(self.codLote) + "'"

        with ConexaoERP.ConexaoInternoMPL() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                lotes = pd.DataFrame(rows, columns=colunas)

        # Libera memória manualmente
        del rows
        gc.collect()

        nomeLote = lotes['nomeLote'][0]
        nomeLote = nomeLote[:2] + '-' + nomeLote

        return nomeLote


    def informacoesFasesCsw(self):
        '''Método que consulta no csw as informacoes das fases cadastradas no CSW'''


        sql_nomeFases = """
        SELECT 
            f.codFase , 
            f.nome as fase 
        FROM 
            tcp.FasesProducao f
        WHERE 
            f.codEmpresa = 1 
        """

        with ConexaoERP.ConexaoInternoMPL() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql_nomeFases)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                sql_nomeFases = pd.DataFrame(rows, columns=colunas)

        # Libera memória manualmente
        del rows
        gc.collect()


        return sql_nomeFases




    def obterTiposOPCSW(self):
        '''Metodo qe busca no ERP CSW todos os tipo de OPS'''

        sql = """
            SELECT
        	t.codTipo || '-' || t.nome as tipoOP
        FROM
        	tcp.TipoOP t
        WHERE
        	t.Empresa = 1
        order by
        	codTipo asc
            """

        with ConexaoERP.ConexaoInternoMPL() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                tipoOP = pd.DataFrame(rows, columns=colunas)

        # Libera memória manualmente
        del rows
        gc.collect()

        return tipoOP

    def obterDataMvtoPCP(self):
        '''Metodo que obtem do ERP CSW a data de movto da fase PCP'''

        sql = f"""
            SELECT 
                SUBSTRING(f.numeroOP,1,6) as OPSemTraco, 
                datamov as dataStart
            FROM 
                tco.MovimentacaoOPFase f
            WHERE 
                f.codEmpresa = {self.codEmpresa} and SUBSTRING(f.numeroOP,1,6)
                in (
                    SELECT
                        SUBSTRING (o.numeroOP,1,6)
                    FROM
                        tco.OrdemProd o
                    where 
                        o.codEmpresa = {self.codEmpresa}
                        and situacao = 3
                ) 
                and f.seqroteiro = 1 
                and f.numeroop like '%-001'
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






