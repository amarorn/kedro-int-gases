#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Pipeline SCGAS para Databricks
Script Python para execução no ambiente Databricks

Autor: Pipeline SCGAS
Data: 2025-08-25
"""

import os
import sys
from datetime import datetime, timedelta
import json
import requests
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

def setup_databricks_environment():
    """Configura o ambiente Databricks."""
    print("🚀 Configurando ambiente Databricks...")
    print(f"Timestamp: {datetime.now()}")
    print(f"Python version: {sys.version}")
    
    # Inicializa Spark
    spark = SparkSession.builder \
        .appName("SCGAS_Measurements_Databricks") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    print(f"✅ Spark inicializado: {spark.version}")
    return spark

def load_configuration():
    """Carrega configurações da API SCGAS."""
    print("📋 Carregando configurações...")
    
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
    
    print("✅ Configurações carregadas")
    return API_CONFIG, CREDENTIALS, QUERY_PARAMS

def authenticate_scgas(api_config, credentials):
    """Autentica na API SCGAS e retorna o token de acesso."""
    print("🔐 Autenticando na API SCGAS...")
    
    auth_data = {
        "username": credentials["username"],
        "password": credentials["password"]
    }
    
    auth_url = f"{api_config['base_url']}{api_config['auth_url']}"
    print(f"URL de autenticação: {auth_url}")
    
    try:
        response = requests.post(
            auth_url, 
            json=auth_data, 
            headers=api_config['headers'],
            timeout=api_config['timeout']
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

def collect_measurements(auth_token, api_config, query_params):
    """Coleta dados de medição usando o token de autenticação."""
    print("📊 Coletando dados de medição...")
    
    headers = api_config['headers'].copy()
    headers["Authorization"] = f"Bearer {auth_token}"
    
    data_url = f"{api_config['base_url']}{api_config['measurement_url']}"
    
    print(f"URL de coleta: {data_url}")
    print(f"Body da requisição: {json.dumps(query_params, indent=2)}")
    
    try:
        response = requests.post(
            data_url,
            json=query_params,
            headers=headers,
            timeout=api_config['timeout']
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

def create_pandas_dataframe(measurements_data):
    """Cria um DataFrame pandas a partir dos dados da API."""
    print("🐼 Criando DataFrame pandas...")
    
    if isinstance(measurements_data, list):
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
        df = pd.DataFrame([measurements_data])
    
    # Conversões de tipo
    if 'data' in df.columns:
        df['data'] = pd.to_datetime(df['data'], errors='coerce')
    
    numeric_columns = ['valorConv', 'valorConvFormat', 'codVar', 'codEst', 'codMed', 'intervaloLeituraMin']
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    print(f"✅ DataFrame pandas criado: {len(df)} linhas x {len(df.columns)} colunas")
    return df

def create_spark_dataframe(spark, measurements_data):
    """Cria um DataFrame Spark a partir dos dados da API."""
    print("🔥 Criando DataFrame Spark...")
    
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
        
        spark_df = spark.createDataFrame(processed_data, schema)
    else:
        spark_df = spark.createDataFrame([measurements_data], schema)
    
    print(f"✅ DataFrame Spark criado: {spark_df.count()} linhas")
    return spark_df

def save_data_databricks(spark_df, pandas_df, measurements_data, base_path):
    """Salva os dados no Databricks."""
    print(f"💾 Salvando dados em: {base_path}")
    
    # Salva como Parquet
    parquet_path = f"{base_path}/measurements.parquet"
    print(f"Salvando DataFrame Spark como Parquet: {parquet_path}")
    spark_df.write.mode("overwrite").parquet(parquet_path)
    print("✅ DataFrame Spark salvo como Parquet")
    
    # Salva como CSV
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

def run_analysis(spark_df):
    """Executa análise exploratória com Spark SQL."""
    print("📈 Executando análise exploratória...")
    
    # Registra como view temporária
    spark_df.createOrReplaceTempView("scgas_measurements")
    
    # Estatísticas básicas
    print("\n1. Estatísticas dos valores de medição:")
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
    stats_result.show()
    
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
    estacao_result.show()
    
    print("✅ Análise exploratória concluída")

def main():
    """Função principal do pipeline."""
    print("🎯 INICIANDO PIPELINE SCGAS NO DATABRICKS")
    print("=" * 60)
    
    try:
        # 1. Configuração do ambiente
        spark = setup_databricks_environment()
        
        # 2. Carregamento de configurações
        api_config, credentials, query_params = load_configuration()
        
        # 3. Autenticação
        auth_token = authenticate_scgas(api_config, credentials)
        
        # 4. Coleta de dados
        measurements_data = collect_measurements(auth_token, api_config, query_params)
        
        # 5. Criação dos DataFrames
        pandas_df = create_pandas_dataframe(measurements_data)
        spark_df = create_spark_dataframe(spark, measurements_data)
        
        # 6. Análise exploratória
        run_analysis(spark_df)
        
        # 7. Salvamento dos dados
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        base_path = f"/dbfs/FileStore/scgas/measurements_{timestamp}"
        save_data_databricks(spark_df, pandas_df, measurements_data, base_path)
        
        # 8. Resumo final
        print("\n🎯 RESUMO FINAL DA EXECUÇÃO")
        print("=" * 60)
        print(f"✅ Autenticação: Sucesso")
        print(f"✅ Coleta de dados: {len(measurements_data):,} registros")
        print(f"✅ DataFrame pandas: {pandas_df.shape[0]:,} linhas x {pandas_df.shape[1]} colunas")
        print(f"✅ DataFrame Spark: {spark_df.count():,} linhas")
        print(f"✅ Salvamento: Concluído em {base_path}")
        print(f"\n🚀 Pipeline executado com sucesso no Databricks!")
        
    except Exception as e:
        print(f"❌ Erro durante a execução: {str(e)}")
        raise
    finally:
        if 'spark' in locals():
            spark.stop()
            print("🛑 Sessão Spark encerrada")

if __name__ == "__main__":
    main()
