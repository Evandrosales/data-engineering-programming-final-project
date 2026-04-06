import os
import tempfile
from pyspark.sql import SparkSession
from config.settings import Settings


class SparkSessionManager:

    def __init__(self, settings: Settings):
        self._settings = settings
        self._session: SparkSession | None = None

    @staticmethod
    def _configurar_hadoop_home() -> None:
        if not os.environ.get("HADOOP_HOME"):
            hadoop_home = "C:/hadoop"
            os.environ["HADOOP_HOME"] = hadoop_home
            os.environ["PATH"] = hadoop_home + "/bin" + os.pathsep + os.environ.get("PATH", "")

    def get_session(self) -> SparkSession:
        if self._session is None:
            self._configurar_hadoop_home()
            warehouse_dir = os.path.join(tempfile.gettempdir(), "spark-warehouse")

            self._session = (
                SparkSession.builder
                .appName(self._settings.APP_NAME)
                .master(self._settings.SPARK_MASTER)
                .config("spark.sql.warehouse.dir", warehouse_dir.replace("\\", "/"))
                .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
                .getOrCreate()
            )
            self._session.sparkContext.setLogLevel("WARN")

        return self._session

    def stop(self) -> None:
        """Encerra a sessão Spark."""
        if self._session is not None:
            self._session.stop()
            self._session = None