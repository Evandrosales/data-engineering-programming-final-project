# Relatório de Pedidos — Data Engineering Programming

Projeto PySpark desenvolvido como trabalho final da disciplina **Data Engineering Programming** (FIAP).

---

## 🎯 Objetivo

Gerar um relatório de pedidos de venda cujos pagamentos foram **recusados** (`status=false`) e que na avaliação de fraude foram classificados como **legítimos** (`fraude=false`), referentes ao ano de **2025**.

---

## 📁 Estrutura do Projeto

```
data-engineering-programming-final-project/
├── main.py                              # Aggregation Root (ponto de entrada)
├── pyproject.toml                       # Metadados e configuração do projeto
├── requirements.txt                     # Dependências
├── MANIFEST.in                          # Controle de empacotamento
├── README.md                            # Este arquivo
│
├── config/
│   ├── __init__.py
│   └── settings.py                      # Configurações centralizadas
│
├── session/
│   ├── __init__.py
│   └── spark_session.py                 # Gerenciamento da sessão Spark
│
├── data_io/
│   ├── __init__.py
│   └── data_io.py                       # Leitura (CSV/JSON) e escrita (Parquet)
│
├── business/
│   ├── __init__.py
│   └── reports/
│       ├── __init__.py
│       └── relatorio_pedidos.py         # Lógica de negócio do relatório
│
├── pipeline/
│   ├── __init__.py
│   └── pipeline.py                      # Orquestração do pipeline
│
├── tests/
│   ├── __init__.py
│   └── test_relatorio_pedidos.py        # Testes unitários (pytest)
│
└── data/                                # Datasets (não versionados no Git)
    ├── pedidos/
    │   └── pedidos-*.csv.gz
    └── pagamentos/
        └── pagamentos-*.json.gz
```

---

## 📦 Pré-requisitos

| Ferramenta | Versão mínima |
|------------|---------------|
| Python     | 3.10+         |
| Java (JDK) | 11 ou 17      |
| PySpark    | 4.1.1         |

> **Java é obrigatório** para executar o Spark. Verifique com `java -version`.

---

## ⚙️ Instalação

### 1. Clone o repositório

```bash
git clone https://github.com/Evandrosales/data-engineering-programming-final-project.git
cd data-engineering-programming-final-project
```

### 2. Crie e ative um ambiente virtual (recomendado)

```bash
python -m venv .venv

# Linux / macOS
source .venv/bin/activate

# Windows (PowerShell)
.venv\Scripts\activate
```

### 3. Instale as dependências

```bash
pip install -r requirements.txt
```

---

## 📂 Datasets

O projeto consome dois datasets externos que precisam ser baixados antes da execução.

### Linux / macOS

```bash
mkdir -p data

git clone https://github.com/infobarbosa/datasets-csv-pedidos.git data/pedidos-repo
cp -r data/pedidos-repo/data/pedidos data/pedidos

git clone https://github.com/infobarbosa/dataset-json-pagamentos.git data/pagamentos-repo
cp -r data/pagamentos-repo/data/pagamentos data/pagamentos
```

### Windows (PowerShell)

```powershell
New-Item -ItemType Directory -Force -Path data

git clone https://github.com/infobarbosa/datasets-csv-pedidos.git data\pedidos-repo
Copy-Item -Recurse data\pedidos-repo\data\pedidos data\pedidos

git clone https://github.com/infobarbosa/dataset-json-pagamentos.git data\pagamentos-repo
Copy-Item -Recurse data\pagamentos-repo\data\pagamentos data\pagamentos
```

---

## 🪟 Configuração adicional no Windows

O Spark no Windows requer o `winutils.exe` para acessar o sistema de arquivos local. Execute no PowerShell como Administrador:

```powershell
# Cria a pasta do Hadoop
New-Item -ItemType Directory -Force -Path "C:\hadoop\bin"

# Baixa winutils.exe e hadoop.dll (compatível com Hadoop 3.4.x)
Invoke-WebRequest -Uri "https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.6/bin/winutils.exe" -OutFile "C:\hadoop\bin\winutils.exe"
Invoke-WebRequest -Uri "https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.6/bin/hadoop.dll" -OutFile "C:\hadoop\bin\hadoop.dll"

# Define variáveis de ambiente permanentemente
setx HADOOP_HOME "C:\hadoop"
setx JAVA_HOME "C:\Program Files\Java\jdk-17"
setx PATH "$env:PATH;C:\hadoop\bin"
```

> **Feche e abra um novo terminal** após executar os comandos acima.

---

## 🚀 Execução

```bash
python main.py
```

### Variáveis de ambiente disponíveis

| Variável          | Padrão                       | Descrição                       |
|-------------------|------------------------------|---------------------------------|
| `PEDIDOS_PATH`    | `./data/pedidos`             | Caminho para os arquivos CSV    |
| `PAGAMENTOS_PATH` | `./data/pagamentos`          | Caminho para os arquivos JSON   |
| `OUTPUT_PATH`     | `./output/relatorio_pedidos` | Destino do relatório em Parquet |
| `SPARK_MASTER`    | `local[*]`                   | URL do master Spark             |
| `SPARK_APP_NAME`  | `RelatorioDeVendas`          | Nome da aplicação Spark         |
| `ANO_FILTRO`      | `2025`                       | Ano de referência do relatório  |

---

## ✅ Testes Unitários

```bash
# Executar todos os testes
pytest tests/ -v

# Executar com cobertura de código
pytest tests/ -v --cov=business --cov-report=term-missing
```

---

## 📤 Saída

O relatório é gravado em formato **Parquet** no caminho configurado em `OUTPUT_PATH`.

| Coluna            | Tipo      | Descrição                           |
|-------------------|-----------|-------------------------------------|
| `id_pedido`       | String    | Identificador do pedido             |
| `uf`              | String    | Estado (UF) onde o pedido foi feito |
| `forma_pagamento` | String    | Forma de pagamento                  |
| `valor_total`     | Float     | Valor total do pedido               |
| `data_criacao`    | Timestamp | Data de criação do pedido           |

Ordenação: `uf` → `forma_pagamento` → `data_criacao`

---

## 🏗️ Arquitetura e Padrões

- **Orientação a Objetos**: todos os componentes encapsulados em classes
- **Injeção de Dependências**: `main.py` é o Aggregation Root, instancia e injeta todas as dependências
- **Schemas Explícitos**: nenhum DataFrame usa inferência de schema (`StructType` explícito em todos)
- **Configuração Centralizada**: `config/settings.py` com suporte a variáveis de ambiente
- **Logging**: configurado com `logging.basicConfig` e utilizado em todas as etapas do pipeline
- **Tratamento de Erros**: `try/except` na lógica de negócio com registro via `logger.error`
- **Agnóstico à Plataforma**: paths configuráveis via variáveis de ambiente; compatível com Windows, Linux e macOS

---

## 👥 Integrantes

| Nome | RM |
|------|----|
| André da Silva Gomes Lima | 364124 |
| Evandro dos Santos Sales | 362411 |
| Felipe de Almeida Pereira | 361006 |
| Helen Fernandes Borges | 364154 |
| Matheus Pereira Condotta | 361638 |
| Roberto Ferreira Paulo | 362593 |

---

## 📚 Referências

- [Material de apoio do professor](https://github.com/infobarbosa/pyspark-poo)
- [Dataset de pagamentos](https://github.com/infobarbosa/dataset-json-pagamentos)
- [Dataset de pedidos](https://github.com/infobarbosa/datasets-csv-pedidos)
