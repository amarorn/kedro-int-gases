# Notebook SCGAS para Databricks - Células

Este arquivo contém todas as células do notebook para execução no Databricks. Copie e cole cada célula em um notebook Jupyter no Databricks.

---

## Célula 1: Configuração Inicial

**Tipo**: Code
**Descrição**: Configuração inicial do ambiente Databricks

```python
# Configuração inicial do ambiente Databricks
import os
import sys
from datetime import datetime, timedelta
import json
import requests
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

print(f"Ambiente Databricks - {datetime.now()}")
print(f"Python version: {sys.version}")
print(f"Pandas version: {pd.__version__}")
print(f"PySpark version: {SparkSession.builder.getOrCreate().version}")
```

---

## Célula 2: Configurações da API

**Tipo**: Code
**Descrição**: Carrega configurações da API SCGAS

```python
# Configurações da API SCGAS
# Ajuste estas configurações conforme seu ambiente
API_CONFIG = {
    "base_url": "https://scada.scgas.com.br",
    "auth_url": "/api/Auth/Token",
    "measurement_url": "/api/Variable/History/Measurement",
    "headers": {
        "Content-Type": "application/json",
        "Accept": "*/*"
    },
    "timeout": 30
}

# Credenciais (configure via variáveis de ambiente ou secrets do Databricks)
CREDENTIALS = {
    "username": os.environ.get("SCGAS_USERNAME", "eficienciaenergeticavega@arcelormittal.com.br"),
    "password": os.environ.get("SCGAS_PASSWORD", "Tk5#4Ja2")
}

# Parâmetros da consulta
QUERY_PARAMS = {
    "idIntegracao": "VRTA-CLT-41-023-0001",
    "tagList": ["HistTotal"],
    "from": "2025-01-01T00:00:00.000-03:00",
    "to": "2025-08-07T00:00:00.000-03:00",
    "groupBy": "h",
    "calcBy": "val"
}

print("Configurações carregadas:")
print(f"Base URL: {API_CONFIG['base_url']}")
print(f"Usuário: {CREDENTIALS['username']}")
print(f"Parâmetros de consulta: {json.dumps(QUERY_PARAMS, indent=2)}")
```

---

## Célula 3: Função de Autenticação

**Tipo**: Code
**Descrição**: Função para autenticar na API SCGAS

```python
# Função de autenticação
def authenticate_scgas():
    """Autentica na API SCGAS e retorna o token de acesso."""
  
    auth_data = {
        "username": CREDENTIALS["username"],
        "password": CREDENTIALS["password"]
    }
  
    auth_url = f"{API_CONFIG['base_url']}{API_CONFIG['auth_url']}"
    print(f"URL de autenticação: {auth_url}")
  
    try:
        response = requests.post(
            auth_url, 
            json=auth_data, 
            headers=API_CONFIG['headers'],
            timeout=API_CONFIG['timeout']
        )
    
        if response.status_code == 200:
            token_json = response.json()
            print("✅ Autenticação bem-sucedida")
            return token_json["access_token"]
        else:
            raise Exception(f"❌ Erro ao autenticar: {response.status_code} - {response.text}")
        
    except Exception as e:
        print(f"❌ Erro na autenticação: {str(e)}")
        raise

# Teste de autenticação
print("Testando autenticação...")
auth_token = authenticate_scgas()
print(f"Token obtido: {auth_token[:50]}...")
```

---

## Célula 4: Coleta de Dados

**Tipo**: Code
**Descrição**: Função para coletar dados de medição

```python
# Função de coleta de dados
def collect_measurements(auth_token):
    """Coleta dados de medição usando o token de autenticação."""
  
    # Prepara headers com o token
    headers = API_CONFIG['headers'].copy()
    headers["Authorization"] = f"Bearer {auth_token}"
  
    data_url = f"{API_CONFIG['base_url']}{API_CONFIG['measurement_url']}"
  
    print(f"URL de coleta: {data_url}")
    print(f"Body da requisição: {json.dumps(QUERY_PARAMS, indent=2)}")
  
    try:
        response = requests.post(
            data_url,
            json=QUERY_PARAMS,
            headers=headers,
            timeout=API_CONFIG['timeout']
        )
    
        if response.status_code == 200:
            data = response.json()
            print(f"✅ Dados coletados com sucesso. Registros: {len(data) if isinstance(data, list) else 'N/A'}")
            return data
        else:
            raise Exception(f"❌ Erro ao coletar dados: {response.status_code} - {response.text}")
        
    except Exception as e:
        print(f"❌ Erro na coleta: {str(e)}")
        raise

# Coleta dos dados
print("Coletando dados de medição...")
measurements_data = collect_measurements(auth_token)
print(f"Tipo de dados retornados: {type(measurements_data)}")
if isinstance(measurements_data, list):
    print(f"Primeiro registro: {json.dumps(measurements_data[0], indent=2) if measurements_data else 'Lista vazia'}")
```

---

## Célula 5: Criação do DataFrame Pandas

**Tipo**: Code
**Descrição**: Função para criar DataFrame pandas

```python
# Função para criar DataFrame pandas
def create_pandas_dataframe(measurements_data):
    """Cria um DataFrame pandas a partir dos dados da API."""
  
    print("Processando dados da API...")
  
    if isinstance(measurements_data, list):
        # Processa cada item da lista
        processed_data = []
        for item in measurements_data:
            if isinstance(item, dict):
                processed_data.append({
                    "codVar": item.get("codVar", ""),
                    "tag": item.get("tag", ""),
                    "idIntegracao": item.get("idIntegracao", ""),
                    "unidade": item.get("unidade", ""),
                    "descricao": item.get("descricao", ""),
                    "data": item.get("data", ""),
                    "valorConv": item.get("valorConv", 0.0),
                    "valorConvFormat": item.get("valorConvFormat", 0.0),
                    "estacao": item.get("estacao", ""),
                    "codEst": item.get("codEst", ""),
                    "codMed": item.get("codMed", ""),
                    "intervaloLeituraMin": item.get("intervaloLeituraMin", 0)
                })
    
        df = pd.DataFrame(processed_data)
    else:
        # Se for um dicionário único
        df = pd.DataFrame([measurements_data])
  
    # Conversões de tipo
    if 'data' in df.columns:
        df['data'] = pd.to_datetime(df['data'], errors='coerce')
  
    numeric_columns = ['valorConv', 'valorConvFormat', 'codVar', 'codEst', 'codMed', 'intervaloLeituraMin']
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
  
    print(f"DataFrame pandas criado com {len(df)} linhas e {len(df.columns)} colunas")
    print(f"Colunas: {list(df.columns)}")
  
    return df

# Criação do DataFrame pandas
print("Criando DataFrame pandas...")
pandas_df = create_pandas_dataframe(measurements_data)
print("\nPrimeiras 5 linhas:")
display(pandas_df.head())

print("\nEstatísticas dos valores:")
if 'valorConv' in pandas_df.columns:
    print(f"Valor médio: {pandas_df['valorConv'].mean():,.2f} m³")
    print(f"Valor mínimo: {pandas_df['valorConv'].min():,.2f} m³")
    print(f"Valor máximo: {pandas_df['valorConv'].max():,.2f} m³")
```

---

## Célula 6: Criação do DataFrame Spark

**Tipo**: Code
**Descrição**: Função para criar DataFrame Spark

```python
# Função para criar DataFrame Spark
def create_spark_dataframe(measurements_data):
    """Cria um DataFrame Spark a partir dos dados da API."""
  
    print("Criando DataFrame Spark...")
  
    # Inicializa Spark
    spark = SparkSession.builder \
        .appName("SCGAS_Measurements_Databricks") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
  
    print(f"Sessão Spark inicializada: {spark.version}")
  
    # Define schema
    schema = StructType([
        StructField("codVar", StringType(), True),
        StructField("tag", StringType(), True),
        StructField("idIntegracao", StringType(), True),
        StructField("unidade", StringType(), True),
        StructField("descricao", StringType(), True),
        StructField("data", StringType(), True),
        StructField("valorConv", DoubleType(), True),
        StructField("valorConvFormat", DoubleType(), True),
        StructField("estacao", StringType(), True),
        StructField("codEst", StringType(), True),
        StructField("codMed", StringType(), True),
        StructField("intervaloLeituraMin", DoubleType(), True)
    ])
  
    # Processa dados
    if isinstance(measurements_data, list):
        processed_data = []
        for item in measurements_data:
            if isinstance(item, dict):
                processed_data.append((
                    str(item.get("codVar", "")),
                    str(item.get("tag", "")),
                    str(item.get("idIntegracao", "")),
                    str(item.get("unidade", "")),
                    str(item.get("descricao", "")),
                    str(item.get("data", "")),
                    float(item.get("valorConv", 0.0)),
                    float(item.get("valorConvFormat", 0.0)),
                    str(item.get("estacao", "")),
                    str(item.get("codEst", "")),
                    str(item.get("codMed", "")),
                    float(item.get("intervaloLeituraMin", 0))
                ))
    
        # Cria DataFrame Spark
        spark_df = spark.createDataFrame(processed_data, schema)
    else:
        # DataFrame com uma linha
        spark_df = spark.createDataFrame([measurements_data], schema)
  
    print(f"DataFrame Spark criado com {spark_df.count()} linhas")
    print("Schema do DataFrame:")
    spark_df.printSchema()
  
    return spark_df

# Criação do DataFrame Spark
spark_df = create_spark_dataframe(measurements_data)
print("\nPrimeiras 5 linhas do DataFrame Spark:")
display(spark_df.limit(5).toPandas())
```

---

## Célula 7: Análise Exploratória

**Tipo**: Code
**Descrição**: Executa análise exploratória com Spark SQL

```python
# Análise exploratória com Spark SQL
print("Análise exploratória com Spark SQL:")
print("=" * 50)

# Registra o DataFrame como view temporária
spark_df.createOrReplaceTempView("scgas_measurements")

# Estatísticas básicas
print("1. Estatísticas dos valores de medição:")
stats_query = """
SELECT 
    COUNT(*) as total_registros,
    AVG(valorConv) as valor_medio,
    MIN(valorConv) as valor_minimo,
    MAX(valorConv) as valor_maximo,
    STDDEV(valorConv) as desvio_padrao
FROM scgas_measurements
"""
stats_result = spark.sql(stats_query)
display(stats_result.toPandas())

# Análise por estação
print("\n2. Análise por estação:")
estacao_query = """
SELECT 
    estacao,
    COUNT(*) as total_registros,
    AVG(valorConv) as valor_medio,
    COUNT(DISTINCT codVar) as variaveis_unicas
FROM scgas_measurements 
GROUP BY estacao
ORDER BY total_registros DESC
"""
estacao_result = spark.sql(estacao_query)
display(estacao_result.toPandas())

# Análise temporal
print("\n3. Análise temporal:")
temporal_query = """
SELECT 
    DATE(data) as data_medicao,
    COUNT(*) as registros_dia,
    AVG(valorConv) as valor_medio_dia
FROM scgas_measurements 
WHERE data IS NOT NULL
GROUP BY DATE(data)
ORDER BY data_medicao
LIMIT 10
"""
temporal_result = spark.sql(temporal_query)
display(temporal_result.toPandas())
```

---

## Célula 8: Salvamento dos Dados

**Tipo**: Code
**Descrição**: Salva os dados no Databricks

```python
# Salvamento dos dados no Databricks
print("Salvando dados no Databricks...")
print("=" * 50)

# Define caminhos de destino (ajuste conforme seu ambiente)
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
base_path = f"/dbfs/FileStore/scgas/measurements_{timestamp}"

# Salva como Parquet (recomendado para Spark)
parquet_path = f"{base_path}/measurements.parquet"
print(f"Salvando DataFrame Spark como Parquet: {parquet_path}")
spark_df.write.mode("overwrite").parquet(parquet_path)
print("✅ DataFrame Spark salvo como Parquet")

# Salva como CSV (para compatibilidade)
csv_path = f"{base_path}/measurements.csv"
print(f"Salvando DataFrame pandas como CSV: {csv_path}")
pandas_df.to_csv(csv_path, index=False)
print("✅ DataFrame pandas salvo como CSV")

# Salva dados brutos como JSON
json_path = f"{base_path}/raw_data.json"
print(f"Salvando dados brutos como JSON: {json_path}")
with open(json_path, 'w') as f:
    json.dump(measurements_data, f, indent=2)
print("✅ Dados brutos salvos como JSON")

print(f"\n📁 Todos os arquivos salvos em: {base_path}")
print(f"📊 Total de registros processados: {len(measurements_data):,}")
print(f"⏰ Timestamp de execução: {datetime.now()}")
```

---

## Célula 9: Resumo Final

**Tipo**: Code
**Descrição**: Resumo final da execução

```python
# Resumo final da execução
print("🎯 RESUMO FINAL DA EXECUÇÃO NO DATABRICKS")
print("=" * 60)
print(f"✅ Autenticação: Sucesso")
print(f"✅ Coleta de dados: {len(measurements_data):,} registros")
print(f"✅ DataFrame pandas: {pandas_df.shape[0]:,} linhas x {pandas_df.shape[1]} colunas")
print(f"✅ DataFrame Spark: {spark_df.count():,} linhas")
print(f"✅ Salvamento: Concluído em {base_path}")
print(f"\n🚀 Pipeline executado com sucesso no Databricks!")
print(f"📈 Dados prontos para análise e processamento adicional.")
```

---

## Célula 10: Análises Adicionais (Opcional)

**Tipo**: Code
**Descrição**: Análises adicionais com Spark SQL

```python
# Análises adicionais com Spark SQL
print("🔍 Análises adicionais:")
print("=" * 50)

# Análise de tendências diárias
print("1. Tendências diárias:")
trend_query = """
SELECT 
    DATE_TRUNC('day', data) as dia,
    AVG(valorConv) as valor_medio_dia,
    COUNT(*) as registros,
    STDDEV(valorConv) as variabilidade
FROM scgas_measurements 
WHERE data IS NOT NULL
GROUP BY DATE_TRUNC('day', data)
ORDER BY dia
LIMIT 15
"""
trends = spark.sql(trend_query)
display(trends.toPandas())

# Análise de qualidade dos dados
print("\n2. Qualidade dos dados:")
quality_query = """
SELECT 
    COUNT(*) as total_registros,
    COUNT(CASE WHEN valorConv IS NOT NULL THEN 1 END) as registros_com_valor,
    COUNT(CASE WHEN data IS NOT NULL THEN 1 END) as registros_com_data,
    COUNT(CASE WHEN estacao IS NOT NULL THEN 1 END) as registros_com_estacao,
    COUNT(CASE WHEN valorConv = 0 THEN 1 END) as registros_zero
FROM scgas_measurements
"""
quality = spark.sql(quality_query)
display(quality.toPandas())

# Análise de distribuição dos valores
print("\n3. Distribuição dos valores:")
distribution_query = """
SELECT 
    CASE 
        WHEN valorConv < 1000000 THEN 'Baixo (< 1M)'
        WHEN valorConv < 5000000 THEN 'Médio (1M-5M)'
        WHEN valorConv < 10000000 THEN 'Alto (5M-10M)'
        ELSE 'Muito Alto (> 10M)'
    END as faixa_valor,
    COUNT(*) as quantidade,
    AVG(valorConv) as valor_medio_faixa
FROM scgas_measurements 
WHERE valorConv IS NOT NULL
GROUP BY 
    CASE 
        WHEN valorConv < 1000000 THEN 'Baixo (< 1M)'
        WHEN valorConv < 5000000 THEN 'Médio (1M-5M)'
        WHEN valorConv < 10000000 THEN 'Alto (5M-10M)'
        ELSE 'Muito Alto (> 10M)'
    END
ORDER BY quantidade DESC
"""
distribution = spark.sql(distribution_query)
display(distribution.toPandas())
```

---

## Célula 11: Visualizações (Opcional)

**Tipo**: Code
**Descrição**: Cria visualizações dos dados

```python
# Visualizações dos dados
print("📊 Criando visualizações:")
print("=" * 50)

try:
    import matplotlib.pyplot as plt
    import seaborn as sns
  
    # Configurações de visualização
    plt.style.use('default')
    sns.set_palette("husl")
  
    # Figura 1: Distribuição dos valores
    fig, axes = plt.subplots(2, 2, figsize=(15, 10))
  
    # Histograma dos valores
    axes[0,0].hist(pandas_df['valorConv'], bins=50, alpha=0.7, color='skyblue', edgecolor='black')
    axes[0,0].set_title('Distribuição dos Valores de Medição')
    axes[0,0].set_xlabel('Valor (m³)')
    axes[0,0].set_ylabel('Frequência')
  
    # Boxplot dos valores
    axes[0,1].boxplot(pandas_df['valorConv'])
    axes[0,1].set_title('Boxplot dos Valores de Medição')
    axes[0,1].set_ylabel('Valor (m³)')
  
    # Série temporal dos valores
    if 'data' in pandas_df.columns:
        pandas_df_sorted = pandas_df.sort_values('data')
        axes[1,0].plot(pandas_df_sorted['data'], pandas_df_sorted['valorConv'], alpha=0.7)
        axes[1,0].set_title('Evolução Temporal dos Valores')
        axes[1,0].set_xlabel('Data')
        axes[1,0].set_ylabel('Valor (m³)')
        axes[1,0].tick_params(axis='x', rotation=45)
  
    # Comparação valorConv vs valorConvFormat
    axes[1,1].scatter(pandas_df['valorConv'], pandas_df['valorConvFormat'], alpha=0.6)
    axes[1,1].plot([pandas_df['valorConv'].min(), pandas_df['valorConv'].max()], 
                    [pandas_df['valorConv'].min(), pandas_df['valorConv'].max()], 'r--', alpha=0.8)
    axes[1,1].set_title('Valor Convertido vs Valor Formatado')
    axes[1,1].set_xlabel('Valor Convertido (m³)')
    axes[1,1].set_ylabel('Valor Formatado (m³)')
  
    plt.tight_layout()
    plt.show()
  
    print("✅ Visualizações criadas com sucesso!")
  
except ImportError:
    print("⚠️ Matplotlib/Seaborn não disponível. Pulando visualizações.")
    print("Para visualizações, instale: pip install matplotlib seaborn")
```

---

## 📋 **Instruções de Uso**

1. **Crie um novo notebook** no Databricks
2. **Copie e cole cada célula** na ordem apresentada
3. **Execute as células sequencialmente**
4. **Ajuste as configurações** conforme necessário
5. **Monitore os logs** de execução

## ⚠️ **Observações Importantes**

- **Credenciais**: Configure via variáveis de ambiente ou secrets do Databricks
- **Cluster**: Use um cluster com Python 3.9+ e bibliotecas necessárias
- **Permissões**: Verifique permissões de escrita no DBFS
- **Timeout**: Ajuste conforme a velocidade da sua conexão

---

**Pipeline SCGAS para Databricks** - Células do Notebook
**Data**: 2025-08-25
**Versão**: 1.0
