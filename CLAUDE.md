# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

End-to-end data engineering pipeline for diesel engine emissions analysis. Simulates WHSC (World Harmonized Stationary Cycle) dynamometer test data, validates compliance against EPA Tier 4 Final and Euro VI standards, and exposes results via a Looker Studio dashboard backed by BigQuery.

## Common Commands

### Manual pipeline execution (sequential)
```bash
python generate_dyno_data.py       # Gera o CSV simulado
python upload_to_gcs.py            # Upload para GCS (retorna URI particionado)
python load_gcs_to_bigquery.py     # Carrega Bronze no BigQuery
                                   # Opcional: passe o GCS URI como argumento
                                   #   python load_gcs_to_bigquery.py gs://...

cd diesel_emissions
dbt run                            # Executa todos os modelos (Silver + Gold)
dbt test                           # Executa os testes de qualidade
dbt run --select stg_dyno_measurements   # Executa um modelo específico
dbt test --select emissions_compliance   # Testa um modelo específico
```

### Airflow (orquestrado)
```bash
cd airflow
docker compose up -d               # Inicia Airflow + PostgreSQL
# UI: http://localhost:8080  (admin / admin)
# DAG: diesel_emissions_pipeline, schedule: diário 06:00 UTC
docker compose down
```

### Dependências
```bash
pip install google-cloud-storage google-cloud-bigquery pyyaml dbt-core dbt-bigquery
```

## Arquitetura — Medallion

```
generate_dyno_data.py  →  GCS (bucket whsc-homolo, particionado por data)
                       →  BigQuery: diesel_raw.dyno_measurements     (Bronze — Ingestão)
                       →  dbt: diesel_silver.stg_dyno_measurements   (Silver — view)
                       →  dbt: diesel_silver.emissions_compliance     (Gold   — table)
                            └  diesel_silver.engine_performance        (Gold   — table)
                       →  Looker Studio dashboard
```

O DAG do Airflow (`airflow/dags/diesel_emissions_pipeline.py`) encadeia os 5 passos. As tasks Python usam `PythonOperator` importando as classes do pacote `pipeline/`. O GCS URI é passado da task de upload para a task de carga via XCom.

### Responsabilidade de cada camada

| Camada | Localização | Responsabilidade |
|--------|-------------|-----------------|
| **Bronze** | `diesel_raw.dyno_measurements` | Ingestão bruta — 24 colunas de medição, sem flags de compliance |
| **Silver** | `stg_dyno_measurements` (dbt, view) | Limpeza, arredondamento, BSFC, load_category e **cálculo dos flags de compliance** |
| **Gold** | `emissions_compliance` (dbt, table) | Emissões ponderadas WHSC × limites regulatórios, uma linha por teste |
| **Gold** | `engine_performance` (dbt, table) | Curvas de BSFC, rankings de NOx/eficiência por modo WHSC |

## Estrutura do pacote Python

```
pipeline/
├── __init__.py          expõe Settings e load_settings
├── settings.py          lê config.yaml, dataclasses imutáveis (frozen=True)
├── auth.py              cria clientes GCS e BigQuery autenticados
├── generator.py         DynoDataGenerator — gera CSV sem flags de compliance
├── gcs_uploader.py      GCSUploader — upload particionado, retorna URI real
└── bq_loader.py         BigQueryLoader — lê schema de schema/dyno_measurements.json

schema/
└── dyno_measurements.json   schema BigQuery (24 campos, sem booleanos de compliance)
```

Os scripts na raiz (`generate_dyno_data.py`, `upload_to_gcs.py`, `load_gcs_to_bigquery.py`) são entry points com 4–6 linhas que apenas instanciam e chamam a classe correspondente.

## Padrões de desenvolvimento

### Configuração — fonte única de verdade
Todas as variáveis de infraestrutura, parâmetros de simulação e **limites regulatórios** estão em `config.yaml`. Nenhum valor deve ser hardcoded em outro lugar.

- Python lê via `pipeline.settings.load_settings()` → objeto `Settings` imutável
- dbt lê via `vars` em `dbt_project.yml` (valores devem espelhar `config.yaml`)

Para alterar um limite regulatório (ex: NOx EPA): editar `config.yaml > regulatory_limits` e `dbt_project.yml > vars`. Nenhum SQL ou código Python precisa mudar.

### Python — orientado a objetos
Cada etapa do pipeline é uma classe com interface pública mínima:

| Classe | Arquivo | Interface pública |
|--------|---------|-------------------|
| `DynoDataGenerator` | `pipeline/generator.py` | `run() -> Path` |
| `GCSUploader` | `pipeline/gcs_uploader.py` | `run(csv_path=None) -> str` |
| `BigQueryLoader` | `pipeline/bq_loader.py` | `run(gcs_uri=None) -> None` |

Construtores recebem `Settings`. Métodos internos são prefixados com `_`.

### Lógica de negócio — centralizada
Flags de conformidade regulatória **não existem no Bronze** — são calculados exclusivamente na camada Silver (`stg_dyno_measurements.sql`) usando os vars do dbt. Isso garante que uma mudança de norma impacte um único ponto.

### dbt — materialização por camada
- `staging/` → `view` (Silver): sem custo de storage, sempre reflete o Bronze atual
- `marts/` → `table` (Gold): pré-computado para queries rápidas no Looker Studio

## Configuração GCP (`config.yaml`)

- Project: `whsc-homologacao`
- Bucket: `whsc-homolo` — prefixo `raw/dyno_emissions/year=.../month=.../day=.../`
- Dataset/tabela Bronze: `diesel_raw.dyno_measurements`
- Region: US
- Auth: Service Account JSON — caminho definido em `config.yaml`, arquivo gitignored

## Contexto de domínio

- **WHSC**: 13 modos operacionais, cada um com um `weight_factor` oficial (ISO 8178-4, soma = 1.00)
- **Limites regulatórios** (g/kWh):
  - EPA Tier 4 Final — NOx 0.40, CO 3.50, HC 0.19, PM 0.02
  - Euro VI — NOx 0.40, CO 4.00, HC 0.16, PM 0.01
- **Setup de teste**: motores ENG-001/002/003, células DYNO-A/B, 20 ciclos de teste
- O limite de PM Euro VI (0.01 g/kWh) é a restrição mais severa do dataset
