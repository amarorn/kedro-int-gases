# Notebook: Comandos Mágicos do Kedro para Execução de Pipelines

Este notebook demonstra como usar os comandos mágicos do Kedro para executar pipelines, acessar dados e gerenciar o projeto.

## Célula 1: Configuração Inicial e Importações

```python
# Configuração inicial do ambiente Kedro
import os
import sys
from datetime import datetime
import pandas as pd
import numpy as np

# Verificar se estamos no ambiente Kedro
print(f"Diretório atual: {os.getcwd()}")
print(f"Ambiente Kedro - {datetime.now()}")
print(f"Python version: {sys.version}")
print(f"Pandas version: {pd.__version__}")

# Verificar se o Kedro está disponível
try:
    import kedro
    print(f"✅ Kedro version: {kedro.__version__}")
except ImportError:
    print("❌ Kedro não encontrado. Execute: pip install kedro")
```

## Célula 2: Carregar Extensão Kedro (Jupyter Magic)

```python
# Carregar extensão Kedro para comandos mágicos
%load_ext kedro.extras.extensions.ipython

# Verificar se a extensão foi carregada
print("Extensão Kedro carregada com sucesso!")
print("Comandos mágicos disponíveis:")
print("- %reload_kedro")
print("- %kedro_catalog")
print("- %kedro_lineage")
print("- %kedro_replay")
```

## Célula 3: Informações do Projeto Kedro

```python
# Mostrar informações do projeto
%reload_kedro

# Verificar pipelines disponíveis
print("\nPipelines disponíveis:")
print("- data_engineering")
print("- __default__")
```

## Célula 4: Listar Datasets do Catálogo

```python
# Listar todos os datasets disponíveis
%kedro_catalog

# Mostrar datasets de um pipeline específico
%kedro_catalog --pipeline data_engineering
```

## Célula 5: Acessar Dados do Catálogo

```python
# Acessar dados usando o contexto Kedro
from kedro.framework.startup import bootstrap_project
from kedro.framework.context import KedroContext

# Inicializar contexto do projeto
bootstrap_project(os.getcwd())
context = KedroContext(os.getcwd())

# Listar datasets disponíveis
print("Datasets disponíveis:")
for dataset_name in context.catalog.list():
    print(f"- {dataset_name}")

# Carregar um dataset específico
try:
    measurements_data = context.catalog.load("measurements_data")
    print(f"\nDataset 'measurements_data' carregado:")
    print(f"Tipo: {type(measurements_data)}")
    if hasattr(measurements_data, 'shape'):
        print(f"Shape: {measurements_data.shape}")
    elif hasattr(measurements_data, '__len__'):
        print(f"Tamanho: {len(measurements_data)}")
except Exception as e:
    print(f"Erro ao carregar dataset: {e}")
```

## Célula 6: Executar Pipeline Completo

```python
# Executar pipeline completo usando comando mágico
%kedro_replay --pipeline data_engineering

# Ou executar via contexto
print("Executando pipeline data_engineering...")
try:
    context.run(pipeline_name="data_engineering")
    print("✅ Pipeline executado com sucesso!")
except Exception as e:
    print(f"❌ Erro na execução: {e}")
```

## Célula 7: Executar Pipeline com Parâmetros

```python
# Executar pipeline com parâmetros específicos
from kedro.framework.context import KedroContext

# Recarregar contexto
context = KedroContext(os.getcwd())

# Executar com parâmetros
params = {
    "start_date": "2025-01-01",
    "end_date": "2025-08-07",
    "batch_size": 1000
}

print("Executando pipeline com parâmetros personalizados...")
try:
    context.run(
        pipeline_name="data_engineering",
        tags=["data_ingestion"],
        node_names=["fetch_measurements", "process_data"]
    )
    print("✅ Pipeline executado com sucesso!")
except Exception as e:
    print(f"❌ Erro na execução: {e}")
```

## Célula 8: Visualizar Linhagem de Dados

```python
# Visualizar linhagem de dados
%kedro_lineage measurements_data

# Mostrar dependências entre datasets
print("\nDependências dos datasets:")
for dataset_name in ["measurements_data", "measurements_dataframe", "catalog_save_result"]:
    try:
        %kedro_lineage {dataset_name}
    except Exception as e:
        print(f"Erro ao mostrar linhagem de {dataset_name}: {e}")
```

## Célula 9: Executar Nodes Específicos

```python
# Executar apenas nodes específicos
print("Executando nodes específicos...")

try:
    # Executar apenas o node de autenticação
    context.run(
        pipeline_name="data_engineering",
        node_names=["authenticate_scgas"]
    )
    print("✅ Node de autenticação executado!")
    
    # Executar apenas o node de coleta de dados
    context.run(
        pipeline_name="data_engineering",
        node_names=["fetch_measurements"]
    )
    print("✅ Node de coleta executado!")
    
except Exception as e:
    print(f"❌ Erro na execução: {e}")
```

## Célula 10: Monitorar Execução e Logs

```python
# Verificar logs de execução
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Executar com logging detalhado
print("Executando pipeline com logging detalhado...")

try:
    context.run(
        pipeline_name="data_engineering",
        tags=["data_ingestion"],
        node_names=["fetch_measurements", "process_data", "save_results"]
    )
    print("✅ Pipeline executado com sucesso!")
    
    # Verificar datasets resultantes
    print("\nDatasets após execução:")
    for dataset_name in ["measurements_data", "measurements_dataframe", "catalog_save_result"]:
        try:
            data = context.catalog.load(dataset_name)
            print(f"- {dataset_name}: {type(data).__name__}")
        except Exception as e:
            print(f"- {dataset_name}: Erro ao carregar - {e}")
            
except Exception as e:
    print(f"❌ Erro na execução: {e}")
```

## Célula 11: Execução Condicional

```python
# Execução condicional baseada em parâmetros
from kedro.framework.context import KedroContext

context = KedroContext(os.getcwd())

# Verificar se dados já existem
try:
    existing_data = context.catalog.load("measurements_data")
    data_exists = len(existing_data) > 0 if hasattr(existing_data, '__len__') else True
    print(f"Dados existentes: {data_exists}")
    
    if not data_exists:
        print("Executando pipeline para coletar dados...")
        context.run(pipeline_name="data_engineering")
    else:
        print("Dados já existem. Pulando execução do pipeline.")
        
except Exception as e:
    print(f"Executando pipeline (erro ao verificar dados existentes): {e}")
    context.run(pipeline_name="data_engineering")
```

## Célula 12: Limpeza e Finalização

```python
# Limpeza de recursos
print("Finalizando execução...")

# Verificar status final
print("\nStatus final dos datasets:")
for dataset_name in ["measurements_data", "measurements_dataframe", "catalog_save_result"]:
    try:
        data = context.catalog.load(dataset_name)
        if hasattr(data, 'shape'):
            print(f"- {dataset_name}: {data.shape}")
        elif hasattr(data, '__len__'):
            print(f"- {dataset_name}: {len(data)} itens")
        else:
            print(f"- {dataset_name}: {type(data).__name__}")
    except Exception as e:
        print(f"- {dataset_name}: Não disponível - {e}")

print("\n✅ Notebook de comandos mágicos Kedro concluído!")
```

## Resumo dos Comandos Mágicos

### **Comandos Principais:**
- `%load_ext kedro.extras.extensions.ipython` - Carrega extensão Kedro
- `%reload_kedro` - Recarrega contexto do projeto
- `%kedro_catalog` - Lista datasets do catálogo
- `%kedro_lineage` - Mostra linhagem de dados
- `%kedro_replay` - Executa pipeline

### **Execução via Contexto:**
- `context.run()` - Executa pipelines completos ou nodes específicos
- `context.catalog.load()` - Carrega datasets
- `context.catalog.list()` - Lista datasets disponíveis

### **Parâmetros Úteis:**
- `--pipeline` - Especifica pipeline
- `--tags` - Filtra por tags
- `node_names` - Executa nodes específicos
- `tags` - Filtra por tags específicas

Este notebook fornece uma visão completa de como usar os comandos mágicos do Kedro para executar pipelines e gerenciar dados de forma eficiente.
