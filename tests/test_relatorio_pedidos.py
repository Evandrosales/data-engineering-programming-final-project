import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType,
    FloatType, LongType, BooleanType, TimestampType
)
from pyspark.sql import functions as F

from config.settings import Settings
from business.reports.relatorio_pedidos import RelatorioPedidosReport


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder
        .appName("TestRelatorioPedidos")
        .master("local[1]")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture()
def settings():
    return Settings()


@pytest.fixture()
def report(settings):
    return RelatorioPedidosReport(settings)


SCHEMA_PEDIDOS = StructType([
    StructField("ID_PEDIDO",      StringType(),    nullable=False),
    StructField("PRODUTO",        StringType(),    nullable=True),
    StructField("VALOR_UNITARIO", FloatType(),     nullable=True),
    StructField("QUANTIDADE",     LongType(),      nullable=True),
    StructField("DATA_CRIACAO",   TimestampType(), nullable=True),
    StructField("UF",             StringType(),    nullable=True),
    StructField("ID_CLIENTE",     LongType(),      nullable=True),
])

SCHEMA_AVALIACAO = StructType([
    StructField("fraude", BooleanType(), nullable=True),
    StructField("score",  FloatType(),   nullable=True),
])

SCHEMA_PAGAMENTOS = StructType([
    StructField("id_pedido",        StringType(),   nullable=False),
    StructField("forma_pagamento",  StringType(),   nullable=True),
    StructField("valor_pagamento",  FloatType(),    nullable=True),
    StructField("status",           BooleanType(),  nullable=True),
    StructField("avaliacao_fraude", SCHEMA_AVALIACAO, nullable=True),
])


def _make_pedidos(spark: SparkSession):
    data = [
        ("PED-001", "NOTEBOOK", 1500.0, 2, "2025-03-15T10:00:00", "SP", 1),
        ("PED-002", "CELULAR",  1000.0, 1, "2025-06-20T08:00:00", "RJ", 2),
        ("PED-003", "TABLET",   1100.0, 1, "2024-12-01T12:00:00", "MG", 3),
        ("PED-004", "MONITOR",   600.0, 3, "2025-01-10T09:00:00", "BA", 4),
    ]
    rows = [
        (pid, prod, vu, qt, F.lit(dt).cast(TimestampType()), uf, cli)
        for pid, prod, vu, qt, dt, uf, cli in data
    ]
    return spark.createDataFrame(
        [r[:6] + (r[6],) for r in [
            ("PED-001", "NOTEBOOK", 1500.0, 2, "2025-03-15T10:00:00", "SP", 1),
            ("PED-002", "CELULAR",  1000.0, 1, "2025-06-20T08:00:00", "RJ", 2),
            ("PED-003", "TABLET",   1100.0, 1, "2024-12-01T12:00:00", "MG", 3),
            ("PED-004", "MONITOR",   600.0, 3, "2025-01-10T09:00:00", "BA", 4),
        ]],
        schema=StructType([
            StructField("ID_PEDIDO",      StringType(),  False),
            StructField("PRODUTO",        StringType(),  True),
            StructField("VALOR_UNITARIO", FloatType(),   True),
            StructField("QUANTIDADE",     LongType(),    True),
            StructField("DATA_CRIACAO",   StringType(),  True),
            StructField("UF",             StringType(),  True),
            StructField("ID_CLIENTE",     LongType(),    True),
        ])
    ).withColumn("DATA_CRIACAO", F.col("DATA_CRIACAO").cast(TimestampType()))


def _make_pagamentos(spark: SparkSession):
    from pyspark.sql import Row
    rows = [
        Row(id_pedido="PED-001", forma_pagamento="PIX",            valor_pagamento=3000.0,
            status=False, avaliacao_fraude=Row(fraude=False, score=0.10)),
        Row(id_pedido="PED-002", forma_pagamento="BOLETO",         valor_pagamento=1000.0,
            status=False, avaliacao_fraude=Row(fraude=False, score=0.15)),
        Row(id_pedido="PED-003", forma_pagamento="CARTAO_CREDITO", valor_pagamento=1100.0,
            status=False, avaliacao_fraude=Row(fraude=True,  score=0.95)),
        Row(id_pedido="PED-004", forma_pagamento="PIX",            valor_pagamento=1800.0,
            status=True,  avaliacao_fraude=Row(fraude=False, score=0.05)),
    ]
    return spark.createDataFrame(rows, schema=SCHEMA_PAGAMENTOS)


class TestRelatorioPedidosReport:

    def test_relatorio_retorna_apenas_registros_recusados_legitimos(
        self, spark: SparkSession, report: RelatorioPedidosReport
    ):
        df_pedidos = _make_pedidos(spark)
        df_pagamentos = _make_pagamentos(spark)

        df_result = report.gerar_relatorio(df_pedidos, df_pagamentos)
        ids = [r["id_pedido"] for r in df_result.collect()]

        assert "PED-001" in ids, "PED-001 deveria estar no relatório"
        assert "PED-002" in ids, "PED-002 deveria estar no relatório"
        assert "PED-003" not in ids, "PED-003 não deveria estar (fraude=True)"
        assert "PED-004" not in ids, "PED-004 não deveria estar (status=True/aprovado)"

    def test_relatorio_exclui_pedidos_fora_do_ano_2025(
        self, spark: SparkSession, report: RelatorioPedidosReport
    ):
        df_pedidos = _make_pedidos(spark)
        df_pagamentos = _make_pagamentos(spark)

        df_result = report.gerar_relatorio(df_pedidos, df_pagamentos)
        ids = [r["id_pedido"] for r in df_result.collect()]

        assert "PED-003" not in ids

    def test_relatorio_contem_colunas_esperadas(
        self, spark: SparkSession, report: RelatorioPedidosReport
    ):
        df_pedidos = _make_pedidos(spark)
        df_pagamentos = _make_pagamentos(spark)

        df_result = report.gerar_relatorio(df_pedidos, df_pagamentos)
        colunas_esperadas = {"id_pedido", "uf", "forma_pagamento", "valor_total", "data_criacao"}

        assert colunas_esperadas.issubset(set(df_result.columns))

    def test_relatorio_calcula_valor_total_corretamente(
        self, spark: SparkSession, report: RelatorioPedidosReport
    ):
        df_pedidos = _make_pedidos(spark)
        df_pagamentos = _make_pagamentos(spark)

        df_result = report.gerar_relatorio(df_pedidos, df_pagamentos)
        row_ped001 = df_result.filter(F.col("id_pedido") == "PED-001").collect()

        assert len(row_ped001) == 1
        assert row_ped001[0]["valor_total"] == pytest.approx(3000.0, rel=1e-3)

    def test_relatorio_ordenacao(
        self, spark: SparkSession, report: RelatorioPedidosReport
    ):
        df_pedidos = _make_pedidos(spark)
        df_pagamentos = _make_pagamentos(spark)

        df_result = report.gerar_relatorio(df_pedidos, df_pagamentos)
        rows = df_result.collect()

        ufs = [r["uf"] for r in rows]
        assert ufs == sorted(ufs), "Relatório deve estar ordenado por UF"