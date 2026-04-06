import logging
from data_io.data_io import DataReader, DataWriter
from business.reports.relatorio_pedidos import RelatorioPedidosReport

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


class Pipeline:

    def __init__(
        self,
        reader: DataReader,
        writer: DataWriter,
        report: RelatorioPedidosReport,
    ):
        self._reader = reader
        self._writer = writer
        self._report = report
        self._logger = logging.getLogger(self.__class__.__name__)

    def executar(self) -> None:
        self._logger.info("=== Iniciando pipeline de relatório de pedidos ===")

        self._logger.info("Lendo dataset de pedidos.")
        df_pedidos = self._reader.read_pedidos()

        self._logger.info("Lendo dataset de pagamentos.")
        df_pagamentos = self._reader.read_pagamentos()

        self._logger.info("Aplicando lógica de negócio.")
        df_relatorio = self._report.gerar_relatorio(df_pedidos, df_pagamentos)

        self._logger.info("Gravando relatório em formato Parquet.")
        self._writer.write_parquet(df_relatorio)

        self._logger.info("=== Pipeline concluído com sucesso ===")