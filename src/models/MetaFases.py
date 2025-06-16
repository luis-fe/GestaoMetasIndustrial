import numpy as np
import pytz
from src.configApp import configApp
from src.connection import ConexaoPostgre
import pandas as pd
from datetime import datetime
from src.models import FaturamentoClass, ProducaoFases, Produtos, PlanoClass, Cronograma, OrdemProd
import re


class MetaFases():

    '''Classe utilizada para construcao das metas por fase a nivel departamental '''

    def __init__(self, codPlano = None, codLote = None, nomeFase =None, dt_inicioRealizado = None, dt_fimRealizado = None, analiseCongelada = False, arrayCodLoteCsw = None,
                 codEmpresa = '1', dataBackupMetas = None, modeloAnalise = 'LoteProducao',categoria = None, arrayTipoProducao = '', consideraFaltaProgr = True):
        '''Construtor da classe'''

        self.codPlano = codPlano # codigo do Plano criado
        self.codLote = codLote   # codigo do Lote criado no PCP
        self.nomeFase = nomeFase # nome da fase
        self.dt_inicioRealizado = dt_inicioRealizado # filtro do perido de Inicio do realizado
        self.dt_fimRealizado = dt_fimRealizado # filtro do perido final do realizado
        self.analiseCongelada = analiseCongelada # atributo que informa se a analse vai usar recursos pré salvos em csv
        self.arrayCodLoteCsw = arrayCodLoteCsw # Array com o codigo do lote
        self.codEmpresa = codEmpresa
        self.dataBackupMetas = dataBackupMetas
        self.modeloAnalise = modeloAnalise # Modelo da Analise: Vendas x LoteProducao
        self.categoria = categoria
        self.arrayTipoProducao = arrayTipoProducao
        self.consideraFaltaProgr = consideraFaltaProgr
        if self.arrayTipoProducao == '':
            self.arrayTipoProducao = ['']
        if self.arrayCodLoteCsw == ['25A04B']:
            self.consideraFaltaProgr = False
            self.arrayTipoProducao = ['INVERNO 2025']


        if arrayCodLoteCsw != None or arrayCodLoteCsw != '':
            self.loteIN = self.transformaando_codLote_clausulaIN() # funcao inicial que defini o loteIN

    def transformaando_codLote_clausulaIN(self):
        '''Metodo que transforma o arrayCodLote em cláusula IN'''

        if self.arrayCodLoteCsw == None:

            return ''
        else:
            nomes_com_aspas = [f"'{nome}'" for nome in self.arrayCodLoteCsw]
            novo = ", ".join(nomes_com_aspas)

            return novo

    def metas_Lote(self):
        '''Metodo que consulta as metas de um lote'''

        # 1.1 - Abrindo a conexao com o Banco
        conn = ConexaoPostgre.conexaoEngine()

        # 2.1 sql para pesquisar a previsao a nivel de cor e tam, de acordo com o lote escolhido:

        sqlMetas = """
            SELECT 
                "codLote", 
                "Empresa", 
                "codEngenharia", 
                "codSeqTamanho", 
                "codSortimento", 
                previsao
            FROM 
                "PCP".pcp.lote_itens li
            WHERE 
                "codLote" IN ("""+ self.loteIN +""") 
                and "Empresa" = %s
        """

        sqlMetas = pd.read_sql(sqlMetas, conn, params=(self.codEmpresa,))

        return sqlMetas



    def metasFase(self):
        '''Metodo que consulta as meta por fase'''

        ordemProd = OrdemProd.OrdemProd(self.codEmpresa, self.codLote, self.arrayTipoProducao)


        # 1.0 - Verificando se o usuario está analisando em congelamento , CASE NAO:
        if self.analiseCongelada == False:

            produto = Produtos.Produtos()
            produto.relacao_Partes_Pai()
            consultaPartes = produto.relacaoPartes
            faturado = FaturamentoClass.Faturamento(None,None,None,self.codPlano, consultaPartes)





            # 2.1 pesquisar a previsao a nivel de cor e tam, de acordo com o lote escolhido:

                #2.2 if para verificar se o usuario quer a previsao baseado em lote ou baseado em vendas

            if self.modeloAnalise == 'LoteProducao':
                sqlMetas = self.metas_Lote()
            else:
                sqlMetas = faturado.vendasPeriodo_Plano()
                vendasPeriodoPartes = faturado.vendasPeriodo_Plano_PartesPeca()
                sqlMetas = pd.concat([sqlMetas, vendasPeriodoPartes], ignore_index=True)



            # 2.2 - obtem os roteiros das engenharias

            sqlRoteiro = produto.roteiro_Engenharias()

            # 2.3 obtem a ordem de apresentacao de cada fase
            sqlApresentacao = ordemProd.apresentacao_Fases()

            # 2.4 utilizado como apoio para obter a meta a nivel de cor , tamanho, categoria
            consulta = produto.itens_tam_cor()

            # 2.5 Verificar quais codItemPai começam com '1' ou '2'
            mask = consulta['codItemPai'].str.startswith(('1', '2'))
            # 2.5.1 Aplicar as transformações usando a máscara
            consulta['codEngenharia'] = np.where(mask, '0' + consulta['codItemPai'] + '-0', consulta['codItemPai'] + '-0')

            # 2.6 Merge entre os produtos tam e cor e as metas , para descobrir o codigo reduzido (codItem)  dos produtos projetados
            if self.modeloAnalise == 'LoteProducao':
                sqlMetas = pd.merge(sqlMetas, consulta, on=["codEngenharia", "codSeqTamanho", "codSortimento"], how='left')
                sqlMetas['codItem'].fillna('-', inplace=True)
            else:
                sqlMetas = pd.merge(sqlMetas, consulta, on=["codItem"], how='left')




            # 3 - Obter o faturamento de um determinado plano e aplicar ao calculo
            faturadoPeriodo = faturado.faturamentoPeriodo_Plano()


            # 3.1 - Incluindo o faturamento das partes e concatenando com o faturamento dos itens PAI
            faturadoPeriodoPartes = faturado.faturamentoPeriodo_Plano_PartesPeca()
            faturadoPeriodo = pd.concat([faturadoPeriodo, faturadoPeriodoPartes], ignore_index=True)

            # 3.2 - concatenando com o DataFrame das metas o faturmento:
            sqlMetas = pd.merge(sqlMetas,faturadoPeriodo,on='codItem',how='left')
            sqlMetas.fillna(0, inplace=True)

            # 4 - Aplicando os estoques ao calculo
            #----------------------------------------------------------------------------------------------------------
            estoque = produto.estoqueProdutosPA_addPartes()
            sqlMetas = pd.merge(sqlMetas, estoque, on='codItem', how='left')
            sqlMetas.fillna(0, inplace=True)

            # 5- Aplicando a carga em producao
            #-------------------------------------------------------------------------------------------------------
            cargas = ordemProd.carga_porReduzido_addEquivParte(consultaPartes)

            sqlMetas = pd.merge(sqlMetas, cargas, on='codItem', how='left')
            sqlMetas.fillna(0, inplace=True)

            sqlMetas.fillna({
                'saldo': 0,
                'qtdeFaturada': 0,
                'estoqueAtual': 0,
                'carga': 0
            }, inplace=True)
            #--------------------------------------------------------------------------------------------------------

            # 6 Analisando se esta no periodo de faturamento
            diaAtual = datetime.strptime(self.obterDiaAtual(), '%Y-%m-%d')
            plano = PlanoClass.Plano(self.codPlano)
            IniFat = plano.iniFat
            IniFat = datetime.strptime(IniFat, '%Y-%m-%d')

            # 6.1 Caso o periodo de faturamento da colecao tenha começado
            if diaAtual >= IniFat:
                sqlMetas['FaltaProgramar1'] = (sqlMetas['previsao']-sqlMetas['qtdeFaturada']) - (
                            sqlMetas['estoqueAtual'] + sqlMetas['carga'] +sqlMetas['qtdeCancelada'])
                sqlMetas['saldoPedidoAnt'] = 0

            # 6.2 caso o faturamento da colecao atual nao tenha iniciado
            else:
                faturadoAnterior = faturado.consultaArquivoFastVendasAnteriores()
                sqlMetas = pd.merge(sqlMetas, faturadoAnterior, on ="codItem", how='left')
                sqlMetas.fillna(0,inplace=True)
                sqlMetas['estoque-saldoAnt'] = sqlMetas['estoqueAtual'] - (sqlMetas['saldoPedidoAnt'])
                sqlMetas['FaltaProgramar1'] = (sqlMetas['previsao']-sqlMetas['qtdeFaturada']) - (sqlMetas['estoque-saldoAnt'] + sqlMetas['carga'])

            # ----------------------------------------------------------------------------------------------------------------

            # 7 - criando a coluna do faltaProgramar , retirando os produtos que tem falta programar negativo
            sqlMetas['FaltaProgramar'] = np.where(sqlMetas['FaltaProgramar1'] > 0, sqlMetas['FaltaProgramar1'], 0)


            if self.consideraFaltaProgr == False:
                # 1 Consulta sql para obter as OPs em aberto no sistema do ´PCP
                sqlCarga2 = """
                                      select 
                                          codreduzido as "codItem", 
                                          sum(total_pcs) as carga2  
                                      from 
                                          pcp.ordemprod o 
                                      where 
                                          codreduzido is not null
                                          and 
                                          "codFaseAtual" = '401' 
                                          and "numeroop" in ('152072-001',
                                          '152073-001',
                                          '152074-001',
                                          '152078-001',
                                          '152079-001',
                                          '152080-001',
                                          '152084-001',
                                          '152085-001',
                                          '152086-001',
                                          '152087-001',
                                          '152088-001',
                                          '152089-001',
                                          '152090-001',
                                          '152091-001',
                                          '152092-001',
                                          '152093-001',
                                          '152094-001',
                                          '152095-001',
                                          '152096-001',
                                          '152097-001',
                                          '152098-001',
                                          '152099-001',
                                          '152100-001',
                                          '152101-001',
                                          '152102-001',
                                          '152103-001',
                                          '152104-001',
                                          '152105-001',
                                          '152106-001',
                                          '152107-001',
                                          '152108-001',
                                          '152109-001',
                                          '152110-001',
                                          '152111-001',
                                          '152112-001',
                                          '152113-001',
                                          '152114-001',
                                          '152115-001',
                                          '152116-001',
                                          '152117-001',
                                          '152118-001',
                                          '152119-001',
                                          '152120-001',
                                          '152121-001',
                                          '152122-001',
                                          '152123-001',
                                          '152124-001',
                                          '152125-001',
                                          '152126-001',
                                          '152127-001',
                                          '152128-001',
                                          '152129-001',
                                          '152130-001',
                                          '152131-001',
                                          '152132-001',
                                          '152133-001',
                                          '152134-001',
                                          '152135-001'
                                          )
                                      group by 
                                          codreduzido
                                  """

                conn = ConexaoPostgre.conexaoEngine()
                sqlCarga2 = pd.read_sql(sqlCarga2, conn)

                sqlMetas = pd.merge(sqlMetas, sqlCarga2, on='codItem', how='left')

                sqlMetas['carga2'].fillna(0, inplace=True)
                sqlMetas['FaltaProgramar'] = sqlMetas['carga2']


            # 8 - Salvando os dados para csv que é o retrado da previsao x falta programar a nivel sku
            data = self.__obterdiaAtual()

            if self.modeloAnalise == 'LoteProducao':


                self.backupsCsv(sqlMetas, f'analise_{self.codPlano}_{self.loteIN}_{data}')
                self.backupsCsv(sqlMetas, f'analise_{self.codPlano}_{self.loteIN}')
            else:
                self.backupsCsv(sqlMetas, f'analise_{self.codPlano}_{"Vendido"}_{data}')
                self.backupsCsv(sqlMetas, f'analise_{self.codPlano}_{"Vendido"}')


            print('excutando a etata 8:Salvando os dados para csv que é o retrado da previsao x falta programar a nivel sku')
            # __________________________________________________________________________________________________________________


            # 9 - Obtendo o falta Programar a por sku, considerando so os PRODUTOS PAI
            Meta = sqlMetas.groupby(["codEngenharia", "codSeqTamanho", "codSortimento", "categoria"]).agg(
                {"previsao": "sum", "FaltaProgramar": "sum","codItem":'first',"saldoPedidoAnt":'sum'}).reset_index()
            filtro = Meta[Meta['codEngenharia'].str.startswith('0')]
            totalPc = filtro['previsao'].sum()
            totalFaltaProgramar = filtro['FaltaProgramar'].sum()

            #10 Levantando o total da previsao e do que falta programar
            novo2 = self.loteIN.replace('"', "-")
            Totais = pd.DataFrame(
                [{'0-Previcao Pçs': f'{totalPc} pcs', '01-Falta Programar': f'{totalFaltaProgramar} pçs'}])

            #11 Salvando os dados da previsao e do falta programar
            self.backupsCsv(sqlMetas, f'Totais{novo2}')

            # 12 Merge das metas com  o roteiro
            Meta = pd.merge(Meta, sqlRoteiro, on='codEngenharia', how='left')

            # 13 transformamando o codFase e codEngenharia em array para salvar a analise de falta Programar por fase
            codFase_array = Meta['codFase'].values
            codEngenharia_array = Meta['codEngenharia'].values
            # Filtrar as linhas onde 'codFase' é 401
            fase_401 = codFase_array == 401
            fase_426 = codFase_array == 426
            fase_412 = codFase_array == 412
            fase_441 = codFase_array == 441
            # Filtrar as linhas onde 'codEngenharia' não começa com '0'
            nao_comeca_com_0 = np.vectorize(lambda x: not x.startswith('0'))(codEngenharia_array)
            # Combinar as duas condições para filtrar as linhas
            filtro_comb = fase_401 & nao_comeca_com_0
            filtro_comb2 = fase_426 & nao_comeca_com_0
            filtro_comb3 = fase_412 & nao_comeca_com_0
            filtro_comb4 = fase_441 & nao_comeca_com_0
            # Aplicar o filtro invertido
            Meta = Meta[~(filtro_comb | filtro_comb2 | filtro_comb3 | filtro_comb4)]

            if self.modeloAnalise == 'LoteProducao':

                self.backupsCsv(Meta, f'analiseFaltaProgrFases_{self.codPlano}_{self.loteIN}')
            else:
                self.backupsCsv(Meta, f'analiseFaltaProgrFases_{self.codPlano}_{"Vendido"}')

            # 14 criando o dataFrame das Metas a nivel de fase PREVISAO + FALTAPROGRAMAR
            Meta = Meta.groupby(["codFase", "nomeFase"]).agg({"previsao": "sum", "FaltaProgramar": "sum"}).reset_index()
            Meta = pd.merge(Meta, sqlApresentacao, on='nomeFase', how='left')
            Meta['apresentacao'] = Meta.apply(lambda x: 0 if x['codFase'] == 401 else x['apresentacao'], axis=1)
            Meta = Meta.sort_values(by=['apresentacao'], ascending=True)  # escolher como deseja classificar


            # 15 - Importando o cronograma das fases
            cronograma = Cronograma.Cronograma(self.codPlano)
            cronogramaS = cronograma.get_cronogramaFases()

            Meta = pd.merge(Meta, cronogramaS, on='codFase', how='left')


            #16 - Obtendo a Colecao do Lote
            if self.modeloAnalise == 'LoteProducao':
                colecoes = self.__tratamentoInformacaoColecao()

            # 17 - Consultando o Fila das fases
            #Ponto de Congelamento do lote:
            self.backupsCsv(Meta, f'analise_Plano_{self.codPlano}Lote{novo2}')

            Meta = self.recalculoMetas(Meta, ordemProd)

            # 19 Ponto de Congelamento do lote:
            self.backupsCsv(Meta, f'analiseLote{novo2}')

            # 20 - Buscando o realizado da Producao das fases
            producaofases = ProducaoFases.ProducaoFases(self.dt_inicioRealizado, self.dt_fimRealizado, '', 0, self.codEmpresa, 100, 100,None )
            realizado = producaofases.realizadoMediaMovel()
            realizado['codFase'] = realizado['codFase'].astype(int)
            Meta = pd.merge(Meta, realizado, on='codFase', how='left')
            Meta['Realizado'].fillna(0, inplace=True)
            Meta.fillna('-', inplace=True)
            Meta = Meta[Meta['apresentacao'] != '-']

            # 21 - backup das metas levantadas
            self.backupsCsv(Meta, f'meta_{str(self.codPlano)}_{str(self.loteIN )}_{str(data)}',True)


            dataFrame2 = self.backupMetasAnteriores()
            Meta = pd.merge(Meta, dataFrame2, on='nomeFase', how='left')
            Meta['Meta Anterior'].fillna('-',inplace=True)
            Meta['Realizado'].fillna(0, inplace=True)
            Meta.fillna('-',inplace=True)
            dados = {
                '0-Previcao Pçs': f'{totalPc} pcs',
                '01-Falta Programar': f'{totalFaltaProgramar} pçs',
                '1-Detalhamento': Meta.to_dict(orient='records')}

            return pd.DataFrame([dados])

        else:
            caminhoAbsoluto = configApp.localProjeto
            novo2 = self.loteIN.replace('"', "-")

            Meta = pd.read_csv(f'{caminhoAbsoluto}/dados/analise_Plano_{self.codPlano}Lote{novo2}.csv')
            Meta = self.recalculoMetas(Meta, ordemProd)

            Totais = pd.read_csv(f'{caminhoAbsoluto}/dados/Totais{novo2}.csv')
            try:
                totalPc = Totais['0-Previcao Pçs'][0]
            except:
                totalPc = 0
            try:
                totalFaltaProgramar = Totais['01-Falta Programar'][0]
            except:
                totalFaltaProgramar = 0

            realizadoPeriodo = ProducaoFases.ProducaoFases(self.dt_inicioRealizado, self.dt_fimRealizado, '', 0, self.codEmpresa, 100, 100, None)
            realizado = realizadoPeriodo.realizadoMediaMovel()
            realizado['codFase'] = realizado['codFase'].astype(int)
            Meta = pd.merge(Meta, realizado, on='codFase', how='left')

            Meta['Realizado'].fillna(0, inplace=True)
            Meta.fillna('-', inplace=True)
            Meta = Meta[Meta['apresentacao'] != '-']
            dataFrame2 = self.backupMetasAnteriores()

            Meta = pd.merge(Meta, dataFrame2, on='nomeFase', how='left')
            Meta['Meta Anterior'].fillna('-',inplace=True)


            dados = {
                '0-Previcao Pçs': f'{totalPc} pcs',
                '01-Falta Programar': f'{totalFaltaProgramar} pçs',
                '1-Detalhamento': Meta.to_dict(orient='records')}

            return pd.DataFrame([dados])

    def recalculoMetas(self, Meta, ordemProd):

        if self.analiseCongelada == False:

            filaFase = ordemProd.filaFases()
        else:
            caminhoAbsoluto = configApp.localProjeto
            fila = pd.read_csv(f'{caminhoAbsoluto}/dados/filaroteiroOP.csv')
            print('utilizando a fila congelada ...')
            filaFase = ordemProd.tratandoInformFILA(fila)
            print('termiando o tratamento  para fila congelada ...')




        filaFase = filaFase.loc[:,
                   ['codFase', 'Carga Atual', 'Fila']]
        Meta = pd.merge(Meta, filaFase, on='codFase', how='left')
        # 17- formatando erros de validacao nos valores dos atributos
        Meta['Carga Atual'].fillna(0, inplace=True)
        Meta['Fila'].fillna(0, inplace=True)
        Meta['Falta Produzir'] = Meta['Carga Atual'] + Meta['Fila'] + Meta['FaltaProgramar']
        # 18 - obtendo a Meta diaria das fases:
        Meta['dias'].fillna(1, inplace=True)
        Meta['Meta Dia'] = np.where(Meta['dias'] == 0, Meta['Falta Produzir'],
                                    Meta['Falta Produzir'] / Meta['dias'])
        Meta['Meta Dia'] = Meta['Meta Dia'].round(0)
        return Meta

    def backupsCsv(self, dataFrame, nome, backupMetas = False ):
        '''Metodo que faz o backup em csv da analise do falta a programar'''

        caminhoAbsoluto = configApp.localProjeto

        if backupMetas == False:

            if self.modeloAnalise == 'LoteProducao'  :
                dataFrame.to_csv(f'{caminhoAbsoluto}/dados/{nome}.csv')
            else:
                dataFrame.to_csv(f'{caminhoAbsoluto}/dados/{nome}.csv')

        else:
            if self.modeloAnalise == 'LoteProducao'  :

                dataFrame.to_csv(f'{caminhoAbsoluto}/dados/backup/{nome}.csv')
            else:
                dataFrame.to_csv(f'{caminhoAbsoluto}/dados/Vendido{nome}.csv')






    def obterDiaAtual(self):

        fuso_horario = pytz.timezone('America/Sao_Paulo')  # Define o fuso horário do Brasil
        agora = datetime.now(fuso_horario)
        agora = agora.strftime('%Y-%m-%d')
        return agora


    def previsao_categoria_fase(self):
        '''Metodo que obtem o previsto em cada fase por categoria '''
        caminhoAbsoluto = configApp.localProjeto

        previsao = pd.read_csv(f'{caminhoAbsoluto}/dados/analiseFaltaProgrFases_{self.codPlano}_{self.loteIN}.csv')

        previsao = previsao[previsao['nomeFase'] == self.nomeFase].reset_index()
        previsao = previsao.groupby(["categoria"]).agg({"previsao":"sum"}).reset_index()

        previsao = previsao.sort_values(by=['previsao'], ascending=False)  # escolher como deseja classificar

        return previsao

    def previsao_categoria_faseVendido(self):
        '''Metodo que obtem o previsto em cada fase por categoria ( apenas Vendido) '''
        caminhoAbsoluto = configApp.localProjeto

        previsao = pd.read_csv(f'{caminhoAbsoluto}/dados/analiseFaltaProgrFases_{self.codPlano}_{"Vendido"}.csv')

        previsao = previsao[previsao['nomeFase'] == self.nomeFase].reset_index()
        previsao = previsao.groupby(["categoria"]).agg({"previsao":"sum"}).reset_index()

        previsao = previsao.sort_values(by=['previsao'], ascending=False)  # escolher como deseja classificar

        return previsao


    def faltaProgcategoria_fase(self):
        '''Metodo que obtem o previsto em cada fase por categoria '''
        caminhoAbsoluto = configApp.localProjeto

        previsao = pd.read_csv(f'{caminhoAbsoluto}/dados/analiseFaltaProgrFases_{self.codPlano}_{self.loteIN}.csv')

        previsao = previsao[previsao['nomeFase'] == self.nomeFase].reset_index()
        previsao = previsao.groupby(["categoria"]).agg({"FaltaProgramar":"sum"}).reset_index()

        previsao = previsao.sort_values(by=['FaltaProgramar'], ascending=False)  # escolher como deseja classificar

        return previsao

    def faltaProgEngenharias_categoria_fase_(self):
        '''Metodo que obtem o previsto em cada fase por categoria '''
        caminhoAbsoluto = configApp.localProjeto

        previsao = pd.read_csv(f'{caminhoAbsoluto}/dados/analiseFaltaProgrFases_{self.codPlano}_{self.loteIN}.csv')

        previsao = previsao[previsao['nomeFase'] == self.nomeFase].reset_index()
        previsao = previsao[previsao['categoria'] == self.categoria].reset_index()

        previsao = previsao.groupby(["codEngenharia"]).agg({"FaltaProgramar":"sum"}).reset_index()

        previsao = previsao.sort_values(by=['FaltaProgramar'], ascending=False)  # escolher como deseja classificar

        return previsao




    def faltaProgcategoria_faseVendido(self):
        '''Metodo que obtem o previsto em cada fase por categoria '''

        caminhoAbsoluto = configApp.localProjeto
        previsao = pd.read_csv(f'{caminhoAbsoluto}/dados/analiseFaltaProgrFases_{self.codPlano}_{"Vendido"}.csv')


        previsao = previsao[previsao['nomeFase'] == self.nomeFase].reset_index()
        previsao = previsao.groupby(["categoria"]).agg({"FaltaProgramar":"sum"}).reset_index()

        previsao = previsao.sort_values(by=['FaltaProgramar'], ascending=False)  # escolher como deseja classificar

        return previsao


    def cargaProgcategoria_Geral(self):
        '''Metodo que obtem a carga em cada fase por categoria '''


        cargaAtual = """
        select
            o."codFaseAtual",
            o."codreduzido",
            o.total_pcs,
            "codTipoOP", ic.categoria, o."seqAtual" 
        from
            "PCP".pcp.ordemprod o 
        inner join 
            "PCP".pcp.itens_csw ic on ic.codigo = o.codreduzido
        WHERE 
                "codFaseAtual" <> '401'
        """


        conn = ConexaoPostgre.conexaoEngine()
        cargaAtual = pd.read_sql(cargaAtual, conn)

        return cargaAtual


    def cargaProgcategoria_fase(self):
        '''Metodo que obtem a carga em cada fase por categoria '''
        caminhoAbsoluto = configApp.localProjeto

        cargaAtual = pd.read_csv(f'{caminhoAbsoluto}/dados/filaroteiroOP.csv')
        print(f'{self.nomeFase} nome da fase')
        cargaAtual = cargaAtual[cargaAtual['fase']==self.nomeFase].reset_index()


        cargaAtual = cargaAtual[cargaAtual['Situacao']=='em processo'].reset_index()
        cargaAtual = cargaAtual.groupby(["categoria"]).agg({"pcs": "sum"}).reset_index()
        cargaAtual.rename(columns={'pcs': 'Carga'}, inplace=True)


        return cargaAtual

    def cargaOP_fase(self):
        '''Metodo que obtem a carga em cada fase por categoria '''
        caminhoAbsoluto = configApp.localProjeto

        cargaAtual = pd.read_csv(f'{caminhoAbsoluto}/dados/filaroteiroOP.csv')
        print(f'{self.nomeFase} nome da fase')
        cargaAtual = cargaAtual[cargaAtual['fase']==self.nomeFase].reset_index()


        cargaAtual = cargaAtual[cargaAtual['Situacao']=='em processo'].reset_index()
        cargaAtual = cargaAtual.groupby(["numeroOP"]).agg({"pcs": "sum","categoria":"first",
                                                           "COLECAO":"first", "descricao":"first",
                                                           "codProduto":"first","prioridade":"first","EntFase":'first',
                                                           "DiasFase":"first",
                                                           "Tipo Producao":"first",
                                                           "dataStartOP":"first"
                                                           }).reset_index()
        cargaAtual.rename(columns={'pcs': 'Carga'}, inplace=True)
        cargaAtual = cargaAtual.sort_values(by=['Carga'], ascending=False)  # escolher como deseja classificar

        # 17.2 Transformando o array em dataFrame

        df = pd.DataFrame(self.arrayTipoProducao, columns=['Tipo Producao'])
        cargaAtual = pd.merge(cargaAtual, df, on='Tipo Producao')

        # 1. Converter para datetime
        cargaAtual['dataStartOP'] = pd.to_datetime(cargaAtual['dataStartOP'], errors='coerce')

        # 2. Calcular dias passados
        hoje = pd.Timestamp(datetime.today().date())
        cargaAtual['Lead Time Geral'] = (hoje - cargaAtual['dataStartOP']).dt.days

        # 3. Converter o resultado para string
        cargaAtual['Lead Time Geral'] = cargaAtual['Lead Time Geral'].astype(str)

        cargaAtual['dataStartOP'] = cargaAtual['dataStartOP'].dt.strftime('%Y-%m-%d')
        cargaAtual.drop('Tipo Producao', axis=1, inplace=True)

        return cargaAtual


    def cargaOP_faseCategoria(self):
        '''Metodo que obtem a carga em cada fase por categoria '''
        caminhoAbsoluto = configApp.localProjeto

        cargaAtual = pd.read_csv(f'{caminhoAbsoluto}/dados/filaroteiroOP.csv')
        print(f'{self.nomeFase} nome da fase')
        cargaAtual = cargaAtual[cargaAtual['fase']==self.nomeFase].reset_index()
        cargaAtual = cargaAtual[cargaAtual['categoria']==self.categoria].reset_index()


        cargaAtual = cargaAtual[cargaAtual['Situacao']=='em processo'].reset_index()
        cargaAtual = cargaAtual.groupby(["numeroOP"]).agg({"pcs": "sum","categoria":"first",
                                                           "COLECAO":"first", "descricao":"first",
                                                           "codProduto":"first","prioridade":"first","EntFase":'first',
                                                           "DiasFase":"first",
                                                           "Tipo Producao":"first",
                                                           "dataStartOP":"first"
                                                           }).reset_index()
        cargaAtual.rename(columns={'pcs': 'Carga'}, inplace=True)
        cargaAtual = cargaAtual.sort_values(by=['Carga'], ascending=False)  # escolher como deseja classificar

        # 17.2 Transformando o array em dataFrame

        df = pd.DataFrame(self.arrayTipoProducao, columns=['Tipo Producao'])
        cargaAtual = pd.merge(cargaAtual, df, on='Tipo Producao')

        # 1. Converter para datetime
        cargaAtual['dataStartOP'] = pd.to_datetime(cargaAtual['dataStartOP'], errors='coerce')

        # 2. Calcular dias passados
        hoje = pd.Timestamp(datetime.today().date())
        cargaAtual['Lead Time Geral'] = (hoje - cargaAtual['dataStartOP']).dt.days

        # 3. Converter o resultado para string
        cargaAtual['Lead Time Geral'] = cargaAtual['Lead Time Geral'].astype(str)

        cargaAtual['dataStartOP'] = cargaAtual['dataStartOP'].dt.strftime('%Y-%m-%d')
        cargaAtual.drop('Tipo Producao', axis=1, inplace=True)

        return cargaAtual

    def __sqlObterFases(self):

        sql = """
        select
	        distinct "codFase"::varchar as "codFaseAtual" ,
	        "nomeFase"
        from
	        "PCP".pcp."Eng_Roteiro" er
        """

        conn = ConexaoPostgre.conexaoEngine()
        realizado = pd.read_sql(sql, conn)
        return realizado

    def obterRoteirosFila(self):

        caminhoAbsoluto = configApp.localProjeto

        roteiro = pd.read_csv(f'{caminhoAbsoluto}/dados/filaroteiroOP.csv')
        roteiro = roteiro[roteiro['fase']==self.nomeFase].reset_index()
        roteiro = roteiro[roteiro['Situacao']=='a produzir'].reset_index()
        roteiro = roteiro.groupby(["categoria"]).agg({"pcs": "sum"}).reset_index()
        roteiro.rename(columns={'pcs': 'Fila'}, inplace=True)

        return roteiro

    def __obterCodFase(self):

        fases = self.__sqlObterFases()
        fases = fases[fases['nomeFase']==self.nomeFase].reset_index()
        retorno = fases['codFaseAtual'][0]
        return retorno


    def faltaProduzirCategoriaFase(self):
        '''Metodo que consulta o falta produzir a nivel de categoria'''

        # 1 - Levantando o falta programar
        faltaProgramar = self.faltaProgcategoria_fase()

        # 2 - Carga
        carga = self.cargaProgcategoria_fase()

        faltaProduzir = pd.merge(faltaProgramar,carga, on ='categoria',how='outer')
        print(faltaProduzir)

        # 3 - Fila
        fila = self.obterRoteirosFila()
        faltaProduzir = pd.merge(faltaProduzir,fila, on ='categoria',how='outer')

        faltaProduzir.fillna(0, inplace = True)
        faltaProduzir['faltaProduzir'] = faltaProduzir['FaltaProgramar'] + faltaProduzir['Carga']+ faltaProduzir['Fila']


        cronogramaS =Cronograma.Cronograma(self.codPlano)
        cronogramaS = cronogramaS.get_cronogramaFases()
        codFase = self.__obterCodFase()

        cronogramaS = cronogramaS[cronogramaS['codFase'] == int(codFase)].reset_index()
        print(cronogramaS)

        if not cronogramaS.empty:
            dia_util = cronogramaS['dias'][0]
        else:
            dia_util = 1

        faltaProduzir['dias'] = dia_util

        if dia_util == 0:
            dia_util =1
        faltaProduzir['metaDiaria'] = faltaProduzir['faltaProduzir'] / dia_util
        faltaProduzir['metaDiaria'] = faltaProduzir['metaDiaria'].astype(int).round()
        return faltaProduzir


    def faltaProduzirCategoriaFaseVendido(self):
        '''Metodo que consulta o falta produzir a nivel de categoria'''

        # 1 - Levantando o falta programar
        faltaProgramar = self.faltaProgcategoria_faseVendido()

        # 2 - Carga
        carga = self.cargaProgcategoria_fase()

        faltaProduzir = pd.merge(faltaProgramar,carga, on ='categoria',how='outer')

        # 3 - Fila
        fila = self.obterRoteirosFila()
        faltaProduzir = pd.merge(faltaProduzir,fila, on ='categoria',how='outer')

        faltaProduzir.fillna(0, inplace = True)
        faltaProduzir['faltaProduzir'] = faltaProduzir['FaltaProgramar'] + faltaProduzir['Carga']+ faltaProduzir['Fila']


        cronogramaS =Cronograma.Cronograma(self.codPlano)
        cronogramaS = cronogramaS.get_cronogramaFases()
        codFase = self.__obterCodFase()

        cronogramaS = cronogramaS[cronogramaS['codFase'] == int(codFase)].reset_index()
        print(cronogramaS)

        if not cronogramaS.empty:
            dia_util = cronogramaS['dias'][0]
        else:
            dia_util = 1

        faltaProduzir['dias'] = dia_util

        if dia_util == 0:
            dia_util =1
        faltaProduzir['metaDiaria'] = faltaProduzir['faltaProduzir'] / dia_util
        faltaProduzir['metaDiaria'] = faltaProduzir['metaDiaria'].astype(int).round()
        return faltaProduzir





    def __tratamentoInformacaoColecao(self):
        '''Método privado que trata a informacao do nome da colecao'''

        colecoes = []


        for codLote in self.arrayCodLoteCsw:
            lote = OrdemProd.OrdemProd(self.codEmpresa, codLote)

            descricaoLote = lote.consultaNomeLote()

            if 'INVERNO' in descricaoLote:
                nome = 'INVERNO' + ' ' + self._extrair_ano(descricaoLote)
                colecoes.append(nome)
            elif 'PRI' in descricaoLote:
                nome = 'VERAO' + ' ' + self._extrair_ano(descricaoLote)
                colecoes.append(nome)
            elif 'ALT' in descricaoLote:
                nome = 'ALTO VERAO' + ' ' + self._extrair_ano(descricaoLote)
                colecoes.append(nome)

            elif 'VER' in descricaoLote:
                nome = 'VERAO' + ' ' + self._extrair_ano(descricaoLote)
                colecoes.append(nome)
            else:
                nome = 'ENCOMENDAS' + ' ' + self._extrair_ano(descricaoLote)
                colecoes.append(nome)


        return colecoes

    def _extrair_ano(self, descricaoLote):
        '''Metodo privado que extrai da descricao o ano do lote'''

        match = re.search(r'\b2\d{3}\b', descricaoLote)
        if match:
            return match.group(0)
        else:
            return None


    def backupMetasAnteriores(self):
        '''Metodo que busca as metas anteriores '''

        data = str(self.dataBackupMetas).replace('-', '_')
        plano = self.codPlano
        lote = self.transformaando_codLote_clausulaIN()
        lote = """'25M24A'"""

        nome = f'meta_{plano}_{lote}_{data}.csv'
        caminhoAbsoluto = configApp.localProjeto

        try:
            dataFrame = pd.read_csv(f'{caminhoAbsoluto}/dados/backup/{nome}')
            dataFrame = dataFrame.loc[:, ['Meta Dia', 'nomeFase']].reset_index()
            dataFrame.rename(
                columns={'Meta Dia':'Meta Anterior'},
                inplace=True)
        except:
            dataFrame = pd.DataFrame([{'Meta Anterior':0,'nomeFase':''}])


        return dataFrame

    def __obterdiaAtual(self):
        fuso_horario = pytz.timezone('America/Sao_Paulo')  # Define o fuso horário do Brasil
        agora = datetime.now(fuso_horario)
        agora = agora.strftime('%Y_%m_%d')

        return agora

    def faltaProgramarFaseCategoria(self):
        '''Metodo que busca o que falta programar por fase e categoria , retornando uma lista de referencias'''

    def resumoFilaPorFase(self):
        '''Metodo que resume por fase a fila de peças vinda de outras fases '''
        caminhoAbsoluto = configApp.localProjeto

        cargaAtual = pd.read_csv(f'{caminhoAbsoluto}/dados/filaroteiroOP.csv')
        cargaAtual = cargaAtual[cargaAtual['fase']==self.nomeFase].reset_index()


        cargaAtual = cargaAtual[cargaAtual['Situacao']=='a produzir'].reset_index()

        df = pd.DataFrame(self.arrayTipoProducao, columns=['Tipo Producao'])
        cargaAtual = pd.merge(cargaAtual, df, on='Tipo Producao')



        cargaAtual = cargaAtual.groupby(["faseAtual"]).agg({"pcs": "sum"
                                                           }).reset_index()
        cargaAtual.rename(columns={'pcs': 'Fila'}, inplace=True)
        cargaAtual = cargaAtual.sort_values(by=['Fila'], ascending=False)  # escolher como deseja classificar


        return cargaAtual

    def resumoFilaPorCategoria(self):
        '''Metodo que resume por fase a fila de peças vinda de outras fases '''
        caminhoAbsoluto = configApp.localProjeto

        cargaAtual = pd.read_csv(f'{caminhoAbsoluto}/dados/filaroteiroOP.csv')
        print(f'{self.nomeFase} nome da fase')
        cargaAtual = cargaAtual[cargaAtual['fase']==self.nomeFase].reset_index()


        cargaAtual = cargaAtual[cargaAtual['Situacao']=='a produzir'].reset_index()

        df = pd.DataFrame(self.arrayTipoProducao, columns=['Tipo Producao'])
        cargaAtual = pd.merge(cargaAtual, df, on='Tipo Producao')



        cargaAtual = cargaAtual.groupby(["categoria"]).agg({"pcs": "sum"
                                                           }).reset_index()
        cargaAtual.rename(columns={'pcs': 'Fila'}, inplace=True)
        cargaAtual = cargaAtual.sort_values(by=['Fila'], ascending=False)  # escolher como deseja classificar


        return cargaAtual



















