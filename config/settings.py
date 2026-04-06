import os

# Resolve o caminho absoluto da raiz do projeto a partir deste arquivo (config/)
_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


class Settings:
    """Configurações centralizadas do projeto.

    Todos os caminhos são resolvidos com os.path.abspath para garantir
    compatibilidade entre Windows, Linux e macOS (evita paths com '..'
    que o Spark JVM não consegue resolver corretamente no Windows).
    """

    PEDIDOS_PATH: str = os.getenv(
        "PEDIDOS_PATH",
        os.path.abspath(os.path.join(_PROJECT_ROOT, "data", "pedidos"))
    )

    PAGAMENTOS_PATH: str = os.getenv(
        "PAGAMENTOS_PATH",
        os.path.abspath(os.path.join(_PROJECT_ROOT, "data", "pagamentos"))
    )

    OUTPUT_PATH: str = os.getenv(
        "OUTPUT_PATH",
        os.path.abspath(os.path.join(_PROJECT_ROOT, "output", "relatorio_pedidos"))
    )

    # Configurações do Spark
    APP_NAME: str = os.getenv("SPARK_APP_NAME", "RelatorioDeVendas")
    SPARK_MASTER: str = os.getenv("SPARK_MASTER", "local[*]")

    # Filtros de negócio
    ANO_FILTRO: int = int(os.getenv("ANO_FILTRO", "2025"))
    STATUS_PAGAMENTO: bool = False   # pagamentos recusados
    FRAUDE: bool = False             # classificados como legítimos

    # Formato de saída
    OUTPUT_FORMAT: str = "parquet"

    # Separador do CSV de pedidos
    CSV_SEPARATOR: str = ";"