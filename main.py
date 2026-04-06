from config.settings import Settings
from session.spark_session import SparkSessionManager
from data_io.data_io import DataReader, DataWriter
from business.reports.relatorio_pedidos import RelatorioPedidosReport
from pipeline.pipeline import Pipeline


def main() -> None:
    settings = Settings()

    session_manager = SparkSessionManager(settings)
    spark = session_manager.get_session()

    reader = DataReader(spark, settings)
    writer = DataWriter(settings)

    report = RelatorioPedidosReport(settings)

    pipeline = Pipeline(reader, writer, report)
    pipeline.executar()

    session_manager.stop()
    

if __name__ == "__main__":
    main()