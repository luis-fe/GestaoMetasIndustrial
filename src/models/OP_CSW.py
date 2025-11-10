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
                and op.situacao = 3 
                """


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

    def obterTodosTipos(self):
        '''Metodo qe busca no ERP CSW todos os tipo de OPS'''

        sql = """
            SELECT
        	t.codTipo || '-' || t.nome as tipoOP
        FROM
        	tcp.TipoOP t
        WHERE
        	t.Empresa = 1 and t.codTipo not in (7, 13, 14, 15, 19, 21, 23, 61,24,25,26, 11, 20, 28)
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
                datamov as dataStartOP
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

    def Fases(self):
        sql = """ SELECT f.codFase as codFase , f.nome as nomeFase  FROM tcp.FasesProducao f
                WHERE f.codEmpresa = 1 and f.codFase > 400 and f.codFase < 500 """

        with ConexaoERP.ConexaoInternoMPL() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                fases = pd.DataFrame(rows, columns=colunas)

        # Libera memória manualmente
        del rows
        gc.collect()
        fases['codFase'] = fases['codFase'].astype(str)
        return fases


    def get_leadTimeCsW(self):
        sql_entrada = """
                        SELECT
                            o.numeroop as numeroop,
                            (
                            select
                                e.descricao
                            from
                                tco.OrdemProd op
                            join tcp.Engenharia e on
                                e.codengenharia = op.codproduto
                                and e.codempresa = 1
                            WHERE
                                op.codempresa = 1
                                and op.numeroop = o.numeroOP) as nome,
                            o.dataBaixa,
                            o.seqRoteiro,
                            o.horaMov as horaMovEntrada, (select
                                op.codTipoOP 
                            from
                                tco.OrdemProd op
                            WHERE
                                op.codempresa = 1
                                and op.numeroop = o.numeroOP) as codtipoop
                        FROM
                            tco.MovimentacaoOPFase o
                        WHERE
                            o.codEmpresa = 1
                            AND O.databaixa >= DATEADD(DAY,
                            -30,
                            GETDATE())
                                """

        sqlFasesCsw = """
                select
            f.nome as nomeFase,
            f.codFase as codfase
        FROM
            tcp.FasesProducao f
        WHERE
            f.codempresa = 1
            and f.codFase >400
            and f.codFase <500
        """



        with ConexaoERP.ConexaoInternoMPL() as connCSW:
                    with connCSW.cursor() as cursor:
                        cursor.execute(sql_entrada)
                        colunas = [desc[0] for desc in cursor.description]
                        rows = cursor.fetchall()
                        entrada = pd.DataFrame(rows, columns=colunas)

                        cursor.execute(sqlFasesCsw)
                        colunas = [desc[0] for desc in cursor.description]
                        rows = cursor.fetchall()
                        sqlFasesCsw = pd.DataFrame(rows, columns=colunas)

        # Libera memória manualmente
        del rows


        return entrada,sqlFasesCsw
    def leadtimeFaccionistaCsw(self, data_inicio, data_final):
        sql = """
                SELECT
                    r.codFase ,
                    r.codFaccio as codfaccionista ,
                    r.codOP ,
                    r.dataEmissao as dataEntrada, op.codProduto , e.descricao as nome
                FROM
                    tct.RetSimbolicoNF r
                inner join 
                    tco.OrdemProd op on op.codEmpresa = 1 and op.numeroOP = r.codOP 
                inner JOIN 
                    tcp.Engenharia e on e.codEmpresa = 1 and e.codEngenharia = op.codProduto 
                WHERE
                    r.Empresa = 1 and r.codFase in (429, 431, 455, 459) and r.dataEmissao >= DATEADD(DAY,
                                -80,
                                GETDATE()) and r.dataEmissao <=  '""" + data_final + """'"""

        sqlRetornoFaccionista = """
                SELECT
                    r.codFase  ,
                    r.codFaccio as codfaccionista,
                    r.codOP ,
                    r.quantidade as Realizado ,
                    r.dataEntrada as dataBaixa,
                    op.codtipoop as codtipoop
                FROM
                    tct.RetSimbolicoNFERetorno r
                inner join 
                    tco.OrdemProd op on op.codEmpresa = 1 and op.numeroOP = r.codOP 
                inner JOIN 
                    tcp.Engenharia e on e.codEmpresa = 1 and e.codEngenharia = op.codProduto 
                WHERE
                    r.Empresa = 1 and r.codFase in (429, 431, 455, 459) and r.dataEntrada >= '""" + data_inicio + """'and r.dataEntrada <=  '""" + data_final + """'"""

        with ConexaoERP.ConexaoInternoMPL() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                realizado = pd.DataFrame(rows, columns=colunas)

                cursor.execute(sqlRetornoFaccionista)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                sqlRetornoFaccionista = pd.DataFrame(rows, columns=colunas)

        # Libera memória manualmente
        del rows
        gc.collect()

        return realizado, sqlRetornoFaccionista


    def ordem_prod_situacao_aberta_mov_separacao(self):
        '''Metodo que busca as ops em aberto que ja passaram da fase separacao '''

        sql = """
        SELECT
            op.numeroOP, 'passou pela separacao' as obs1
        FROM
            tco.MovimentacaoOPFase mov
        inner join tco.OrdemProd op on
            op.codEmpresa = mov.codEmpresa
            and op.numeroOP = mov.numeroOP
        WHERE
            mov.codEmpresa = 1
            and op.situacao = 3
            and mov.codFase = 409
        """

        with ConexaoERP.ConexaoInternoMPL() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                consulta = pd.DataFrame(rows, columns=colunas)

        return consulta


    def ordem_prod_situacao_aberta_mov_montagem(self):
        '''Metodo que busca as ops em aberto que ja passaram da fase separacao '''

        sql = """
        SELECT
            op.numeroOP, 'passou pela montagem' as obs2
        FROM
            tco.MovimentacaoOPFase mov
        inner join tco.OrdemProd op on
            op.codEmpresa = mov.codEmpresa
            and op.numeroOP = mov.numeroOP
        WHERE
            mov.codEmpresa = 1
            and op.situacao = 3
            and mov.codFase = 425
        """

        with ConexaoERP.ConexaoInternoMPL() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                consulta = pd.DataFrame(rows, columns=colunas)

        return consulta


    def relacao_ops_que_consome_partes(self):
        '''metodo publico que busca no ERP as Ops, 'em aberto', que possuem partes na sua estrutura '''

        sql = """
        SELECT
            c.CodComponente, c.loteOP, c.codSortimento,
            (select i2.codSeqTamanho  
            from cgi.Item2 i2 WHERE i2.empresa = 1 
                and i2.coditem = c.CodComponente 
            ) as seqTam
        FROM
            tcop.ComponentesVariaveis c
        WHERE
            c.codEmpresa = 1
            and c.codfase = 426
            and codNaturezaOrigem = 20
            and loteOP in (							select op.codlote||'/'||op.numeroop 
                            from tco.OrdemProd op 
                            WHERE op.codempresa = 1
                                    and op.situacao =3
                                    and op.numeroop like '%-001'
                                    and op.codfaseatual 
                                    not in (401, 429, 408, 406)	
                        )
        """


        with ConexaoERP.ConexaoInternoMPL() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                consulta = pd.DataFrame(rows, columns=colunas)

        return consulta



    def sql_estoque_partes(self):

        sql = """
        select 
            coditem as CodComponente,
            estoqueAtual
        from 
            est.DadosEstoque d
        where 
            d.codempresa = 1 
            and d.codnatureza = 20
            and d.estoqueAtual > 0
        """


        with ConexaoERP.ConexaoInternoMPL() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                consulta = pd.DataFrame(rows, columns=colunas)

        return consulta





