from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField,
    StringType, FloatType, LongType, BooleanType, TimestampType
)
from config.settings import Settings


def _to_spark_path(path: str) -> str:
    return path.replace("\\", "/")


class DataReader:

    SCHEMA_PEDIDOS = StructType([
        StructField("ID_PEDIDO",      StringType(),    nullable=False),
        StructField("PRODUTO",        StringType(),    nullable=True),
        StructField("VALOR_UNITARIO", FloatType(),     nullable=True),
        StructField("QUANTIDADE",     LongType(),      nullable=True),
        StructField("DATA_CRIACAO",   TimestampType(), nullable=True),
        StructField("UF",             StringType(),    nullable=True),
        StructField("ID_CLIENTE",     LongType(),      nullable=True),
    ])

    SCHEMA_AVALIACAO_FRAUDE = StructType([
        StructField("fraude", BooleanType(), nullable=True),
        StructField("score",  FloatType(),   nullable=True),
    ])

    SCHEMA_PAGAMENTOS = StructType([
        StructField("id_pedido",        StringType(),            nullable=False),
        StructField("forma_pagamento",  StringType(),            nullable=True),
        StructField("valor_pagamento",  FloatType(),             nullable=True),
        StructField("status",           BooleanType(),           nullable=True),
        StructField("avaliacao_fraude", SCHEMA_AVALIACAO_FRAUDE, nullable=True),
    ])

    def __init__(self, spark: SparkSession, settings: Settings):
        self._spark = spark
        self._settings = settings

    def read_pedidos(self) -> DataFrame:
        path = _to_spark_path(self._settings.PEDIDOS_PATH)
        return (
            self._spark.read
            .schema(self.SCHEMA_PEDIDOS)
            .option("header", "true")
            .option("sep", self._settings.CSV_SEPARATOR)
            .option("compression", "gzip")
            .csv(path)
        )

    def read_pagamentos(self) -> DataFrame:
        path = _to_spark_path(self._settings.PAGAMENTOS_PATH)
        return (
            self._spark.read
            .schema(self.SCHEMA_PAGAMENTOS)
            .option("compression", "gzip")
            .json(path)
        )


class DataWriter:

    def __init__(self, settings: Settings):
        self._settings = settings

    def write_parquet(self, df: DataFrame) -> None:
        """Persiste o DataFrame no formato Parquet."""
        path = _to_spark_path(self._settings.OUTPUT_PATH)
        (
            df.write
            .mode("overwrite")
            .parquet(path)
        )