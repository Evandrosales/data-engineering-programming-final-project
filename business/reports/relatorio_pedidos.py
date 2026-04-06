import logging
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from config.settings import Settings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


class RelatorioPedidosReport:

    def __init__(self, settings: Settings):
        self._settings = settings
        self._logger = logging.getLogger(self.__class__.__name__)

    def gerar_relatorio(self, df_pedidos: DataFrame, df_pagamentos: DataFrame) -> DataFrame:
        try:
            self._logger.info("Iniciando geração do relatório de pedidos.")

            self._logger.info("Calculando valor total dos pedidos.")
            df_pedidos_com_total = df_pedidos.withColumn(
                "VALOR_TOTAL",
                F.round(F.col("VALOR_UNITARIO") * F.col("QUANTIDADE"), 2)
            )

            self._logger.info("Filtrando pedidos do ano %s.", self._settings.ANO_FILTRO)
            df_pedidos_filtrado = df_pedidos_com_total.filter(
                F.year(F.col("DATA_CRIACAO")) == self._settings.ANO_FILTRO
            )

            self._logger.info(
                "Filtrando pagamentos com status=%s e fraude=%s.",
                self._settings.STATUS_PAGAMENTO,
                self._settings.FRAUDE,
            )
            df_pagamentos_filtrado = df_pagamentos.filter(
                (F.col("status") == self._settings.STATUS_PAGAMENTO) &
                (F.col("avaliacao_fraude.fraude") == self._settings.FRAUDE)
            )

            self._logger.info("Realizando join entre pedidos e pagamentos.")
            df_joined = df_pedidos_filtrado.join(
                df_pagamentos_filtrado,
                df_pedidos_filtrado["ID_PEDIDO"] == df_pagamentos_filtrado["id_pedido"],
                how="inner"
            )

            self._logger.info("Selecionando colunas do relatório.")
            df_relatorio = df_joined.select(
                df_pedidos_filtrado["ID_PEDIDO"].alias("id_pedido"),
                df_pedidos_filtrado["UF"].alias("uf"),
                df_pagamentos_filtrado["forma_pagamento"],
                df_pedidos_filtrado["VALOR_TOTAL"].alias("valor_total"),
                df_pedidos_filtrado["DATA_CRIACAO"].alias("data_criacao"),
            )

            self._logger.info("Ordenando relatório por uf, forma_pagamento e data_criacao.")
            df_relatorio = df_relatorio.orderBy(
                F.col("uf"),
                F.col("forma_pagamento"),
                F.col("data_criacao"),
            )

            self._logger.info(
                "Relatório gerado com sucesso. Total de registros: %s.",
                df_relatorio.count(),
            )
            return df_relatorio

        except Exception as error:
            self._logger.error("Erro ao gerar o relatório: %s", str(error))
            raise