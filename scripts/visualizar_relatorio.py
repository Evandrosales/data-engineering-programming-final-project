from pyspark.sql import SparkSession

class VisualizadorRelatorio:

    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("Visualizar Relatorio Pedidos") \
            .getOrCreate()

    def ler_e_exibir(self, caminho_parquet: str):
        try:
            print("Lendo arquivo parquet...")

            df = self.spark.read.parquet(caminho_parquet)

            print("\n📊 Relatório de Pedidos:\n")
            df.show(truncate=False)

            print(f"\nTotal de registros: {df.count()}")

        except Exception as e:
            print(f"Erro ao ler parquet: {e}")
        finally:
            self.spark.stop()


if __name__ == "__main__":
    caminho = "output/relatorio_pedidos"  # ajuste se necessário

    app = VisualizadorRelatorio()
    app.ler_e_exibir(caminho)