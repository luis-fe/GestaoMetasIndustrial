import pandas as pd
from src.models import PlanoClass, Pedidos_CSW
import fastparquet as fp
from dotenv import load_dotenv
from src.configApp import configApp
import os

class Faturamento():
    '''Classe que interagem com o faturamento'''
    def __init__(self, dataInicial = None, dataFinal = None, tipoNotas = None, codigoPlano = None, relacaoPartes = None, codsku = None):
        '''Construtor da classe'''

        self.dataInicial = dataInicial # dataInicial de faturamento
        self.dataFinal = dataFinal # dataFinal de faturamento
        self.tipoNotas = tipoNotas
        self.codigoPlano = codigoPlano
        self.relacaoPartes = relacaoPartes

        self.pedidoCsw = Pedidos_CSW.Pedidos_CSW()

        self.pedidosBloqueados()
        self.codsku = codsku

    def pedidosBloqueados(self):
        '''Metodo que busca os pedidos bloqueados e retorna em um DataFrame '''

        self._pedidosBloqueados = self.pedidoCsw.pedidosBloqueados()

    def faturamentoPeriodo_Plano(self):
        '''Metodo para obter o faturamento de um determinado plano
        return:
        Dataframe [{'codPedido', 'codProduto', 'qtdePedida', 'qtdeFaturada', 'qtdeCancelada', 'qtdeSugerida',
                       'PrecoLiquido', 'codTipoNota'}]
        '''


        if self.codigoPlano == None:
            return pd.Dataframe([{'status':False, 'Mensagem':'Plano nao encontrado' }])
        else:
            plano = PlanoClass.Plano(self.codigoPlano)

            #Obtendo a dataInicial e dataFinal do Plano
            self.dataInicial = plano.obterDataInicioFatPlano()
            self.dataFinal = plano.obterDataFinalFatPlano()




            pedidos = self.consultaArquivoFastVendas()

            pedidos['status'] = True
            # 3 - Filtrando os pedidos aprovados
            pedidos = pd.merge(pedidos, self._pedidosBloqueados, on='codPedido', how='left')
            pedidos['situacaobloq'].fillna('Liberado', inplace=True)
            pedidos = pedidos[pedidos['situacaobloq'] == 'Liberado']

            # 4 Filtrando somente os tipo de notas desejados


            tipoNotas = plano.pesquisarTipoNotasPlano()

            pedidos = pd.merge(pedidos, tipoNotas, on='codTipoNota')
            pedidos = pedidos.groupby("codItem").agg({"qtdeFaturada": "sum",'qtdeCancelada':'sum'}).reset_index()
            pedidos = pedidos.sort_values(by=['qtdeFaturada'], ascending=False)
            #pedidos = pedidos[pedidos['qtdeFaturada'] > 0].reset_index()

            return pedidos

    def vendasPeriodo_Plano(self):
        '''Metodo para obter as Vebdas de um determinado plano de acordo com o intervalo das datas de Prev de faturamento
        return:
        Dataframe [{'codPedido', 'codProduto', 'qtdePedida', 'qtdeFaturada', 'qtdeCancelada', 'qtdeSugerida',
                       'PrecoLiquido', 'codTipoNota'}]
        '''

        if self.codigoPlano == None:
            return pd.Dataframe([{'status': False, 'Mensagem': 'Plano nao encontrado'}])
        else:
            plano = PlanoClass.Plano(self.codigoPlano)

            # Obtendo a dataInicial e dataFinal do Plano
            self.dataInicial = plano.obterDataInicioFatPlano()
            self.dataFinal = plano.obterDataFinalFatPlano()

            pedidos = self.consultaArquivoFastVendas()

            pedidos['status'] = True
            # 3 - Filtrando os pedidos aprovados
            pedidos = pd.merge(pedidos, self._pedidosBloqueados, on='codPedido', how='left')
            pedidos['situacaobloq'].fillna('Liberado', inplace=True)
            pedidos = pedidos[pedidos['situacaobloq'] == 'Liberado']

            # 4 Filtrando somente os tipo de notas desejados

            tipoNotas = plano.pesquisarTipoNotasPlano()

            pedidos = pd.merge(pedidos, tipoNotas, on='codTipoNota')
            pedidos['qtdePedida'] = pedidos['qtdePedida']
            pedidos = pedidos.groupby("codItem").agg({"qtdePedida": "sum"}).reset_index()
            pedidos = pedidos.sort_values(by=['qtdePedida'], ascending=False)
            pedidos.rename(columns={'qtdePedida': 'previsao'}, inplace=True)

            return pedidos

    # Colunas realmente usadas pelos calculos de faturamento/vendas. Ler so elas
    # (em vez do arquivo inteiro) e' o que evita o MemoryError no servidor.
    COLUNAS_PEDIDOS = ['codPedido', 'codProduto', 'qtdePedida', 'qtdeFaturada', 'qtdeCancelada',
                       'qtdeSugerida', 'PrecoLiquido', 'codTipoNota', 'dataPrevFat']

    def _caminhoParquetPedidos(self):
        """Caminho absoluto do arquivo pedidos.parquet (CAMINHO_PARQUET_FAT no _ambiente.env)"""
        load_dotenv(f'{configApp.localProjeto}/_ambiente.env')
        caminho_absoluto = os.getenv('CAMINHO_PARQUET_FAT')
        return f'{caminho_absoluto}/pedidos.parquet'

    def _lerPedidosPorPeriodo(self, dataIni, dataFim, colunas=None, codProduto=None):
        """Le o pedidos.parquet um row group por vez, mantendo apenas as colunas pedidas e as
        linhas cuja dataPrevFat esta' entre dataIni e dataFim (e, se informado, o codProduto).

        Antes o arquivo inteiro era carregado em memoria (todas as colunas, todas as linhas) a
        cada requisicao, o que derrubava o processo por falta de memoria. Aqui o pico de memoria
        fica limitado a um row group com as colunas selecionadas.

        return: DataFrame filtrado, com 'dataPrevFat' ja convertida para datetime.
        """
        parquet_file = fp.ParquetFile(self._caminhoParquetPedidos())

        if colunas is not None:
            colunas = list(colunas)
            if 'dataPrevFat' not in colunas:
                colunas.append('dataPrevFat')
            if codProduto is not None and 'codProduto' not in colunas:
                colunas.append('codProduto')

        partes = []
        for bloco in parquet_file.iter_row_groups(columns=colunas):
            if codProduto is not None:
                bloco = bloco[bloco['codProduto'] == str(codProduto)]
                if bloco.empty:
                    continue

            datas = pd.to_datetime(bloco['dataPrevFat'], errors='coerce')
            mascara = (datas >= dataIni) & (datas <= dataFim)
            if not mascara.any():
                continue

            bloco = bloco.loc[mascara].copy()
            bloco['dataPrevFat'] = datas[mascara]
            partes.append(bloco)

        if partes:
            return pd.concat(partes, ignore_index=True)

        return pd.DataFrame(columns=colunas if colunas is not None else parquet_file.columns)

    def _pedidosFiltradosPeriodo(self, dataIni, dataFim):
        """Pedidos do periodo com as colunas padrao, quantidades numericas, codItem e saldoPedido"""

        df_filtered = self._lerPedidosPorPeriodo(dataIni, dataFim, colunas=self.COLUNAS_PEDIDOS)

        # Selecionar colunas relevantes
        df_filtered = df_filtered.loc[:,
                      ['codPedido', 'codProduto', 'qtdePedida', 'qtdeFaturada', 'qtdeCancelada', 'qtdeSugerida',
                       'PrecoLiquido', 'codTipoNota']]

        # Convertendo colunas para numérico
        df_filtered['qtdeSugerida'] = pd.to_numeric(df_filtered['qtdeSugerida'], errors='coerce').fillna(0)
        df_filtered['qtdePedida'] = pd.to_numeric(df_filtered['qtdePedida'], errors='coerce').fillna(0)
        df_filtered['qtdeFaturada'] = pd.to_numeric(df_filtered['qtdeFaturada'], errors='coerce').fillna(0)
        df_filtered['qtdeCancelada'] = pd.to_numeric(df_filtered['qtdeCancelada'], errors='coerce').fillna(0)

        # Adicionando coluna 'codItem'
        df_filtered['codItem'] = df_filtered['codProduto']

        # Calculando saldo
        df_filtered['saldoPedido'] = df_filtered["qtdePedida"] - df_filtered["qtdeFaturada"] - df_filtered[
            "qtdeCancelada"]

        return df_filtered

    def consultaArquivoFastVendas(self):
        """Metodo utilizado para ler um arquivo do tipo parquet e converter em um DataFrame """

        # Convertendo a string para datetime
        dataFatIni = pd.to_datetime(self.dataInicial)
        dataFatFinal = pd.to_datetime(self.dataFinal)

        return self._pedidosFiltradosPeriodo(dataFatIni, dataFatFinal)

    def faturamentoPeriodo_Plano_PartesPeca(self):
        '''Metodo para obter o faturamento no periodo do plano , convertido em partes de peças (SEMIACABADOS)'''




        faturamento = self.faturamentoPeriodo_Plano()

        faturamentoPartes = pd.merge(faturamento,self.relacaoPartes,on='codItem')
        # Drop do codProduto
        faturamentoPartes.drop('codItem', axis=1, inplace=True)

        # Rename do redParte para codProduto
        faturamentoPartes.rename(columns={'redParte': 'codItem'}, inplace=True)
        faturamentoPartes.drop(['codProduto','codSeqTamanho','codSortimento'], axis=1, inplace=True)


        return faturamentoPartes

    def vendasPeriodo_Plano_PartesPeca(self):
        '''Metodo para obter o faturamento no periodo do plano , convertido em partes de peças (SEMIACABADOS)'''




        vendas = self.vendasPeriodo_Plano()

        vendasPartes = pd.merge(vendas,self.relacaoPartes,on='codItem')
        # Drop do codProduto
        vendasPartes.drop('codItem', axis=1, inplace=True)

        # Rename do redParte para codProduto
        vendasPartes.rename(columns={'redParte': 'codItem'}, inplace=True)
        vendasPartes.drop(['codProduto','codSeqTamanho','codSortimento'], axis=1, inplace=True)


        return vendasPartes

    def consultaArquivoFastVendasSku(self):
        """Metodo utilizado para ler um arquivo do tipo parquet e converter em um DataFrame """

        plano = PlanoClass.Plano(self.codigoPlano)

        tipoNotas = plano.pesquisarTipoNotasPlano()
        self.dataInicial, self.dataFinal = plano.pesquisarInicioFimFat()

        # Convertendo a string para datetime
        dataFatIni = pd.to_datetime(self.dataInicial)
        dataFatFinal = pd.to_datetime(self.dataFinal)

        # Le so as linhas do sku e do periodo (todas as colunas, pois o retorno e' detalhado)
        df_filtered = self._lerPedidosPorPeriodo(dataFatIni, dataFatFinal, colunas=None, codProduto=self.codsku)
        df_filtered = df_filtered.reset_index(drop=True)
        df_filtered.fillna(0,inplace=True)

        pedidos = pd.merge(df_filtered, tipoNotas, on='codTipoNota')

        # 3 - Filtrando os pedidos aprovados
        pedidos = pd.merge(pedidos, self._pedidosBloqueados, on='codPedido', how='left')
        pedidos['situacaobloq'].fillna('Liberado', inplace=True)
        pedidos = pedidos[pedidos['situacaobloq'] == 'Liberado']



        return pedidos

    def obterPedidosAbertoPlano_por_sku(self):
        '''Metodo que obtem os pedidos em abertos para um determinado codigoReduzido para checagem'''

        consulta = self.consultaArquivoFastVendasSku()

        return consulta

    def consultaArquivoFastVendasAnteriores(self):
        """Metodo utilizado para ler um arquivo do tipo parquet e converter em um DataFrame, retornando um DataFrame com as vendas
         nos 300 dias anteriores ao periodo de faturamento do plano atual"""


        if self.codigoPlano == None:
            return pd.Dataframe([{'status':False, 'Mensagem':'Plano nao encontrado' }])
        else:
            plano = PlanoClass.Plano(self.codigoPlano)
            #Obtendo a dataInicial e dataFinal do Plano
            self.dataInicial = plano.obterDataInicioFatPlano()

            # Convertendo a string para datetime
            dataFatIni = pd.to_datetime(self.dataInicial) - pd.Timedelta(days=200)
            dataFatFinal = pd.to_datetime(self.dataInicial)- pd.Timedelta(days=10)

            pedidos = self._pedidosFiltradosPeriodo(dataFatIni, dataFatFinal)
            pedidos['status'] = True
            # 3 - Filtrando os pedidos aprovados
            pedidos = pd.merge(pedidos, self._pedidosBloqueados, on='codPedido', how='left')
            pedidos['situacaobloq'].fillna('Liberado', inplace=True)
            pedidos = pedidos[pedidos['situacaobloq'] == 'Liberado']

            # 4 Filtrando somente os tipo de notas desejados

            tipoNotas = plano.pesquisarTipoNotasPlano()

            pedidos = pd.merge(pedidos, tipoNotas, on='codTipoNota')
            pedidos = pedidos.groupby("codItem").agg({"qtdeFaturada": "sum", "qtdePedida": "sum","saldoPedido":'sum','qtdeCancelada':'sum'}).reset_index()
            pedidos = pedidos.sort_values(by=['qtdeFaturada'], ascending=False)

            pedidos.rename(
                columns={'codProduto': 'codProdutoAnt', "qtdePedida": "qtdePedidaAnt", "qtdeFaturada": "qtdeFaturadaAnt",
                         "qtdeCancelada": "qtdeCanceladaAnt", "qtdeSugerida": "qtdeSugeridaAnt","saldoPedido" : "saldoPedidoAnt"
                         }, inplace=True)


            pedidosPartes = pd.merge(pedidos, self.relacaoPartes, on='codItem')
            pedidosPartes.drop('codItem', axis=1, inplace=True)
            pedidosPartes.rename(columns={'redParte': 'codItem'}, inplace=True)
            pedidosPartes.drop(['codProduto', 'codSeqTamanho', 'codSortimento'], axis=1, inplace=True)

            pedidos = pd.concat([pedidos, pedidosPartes], ignore_index=True)

            return pedidos
