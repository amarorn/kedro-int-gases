import requests
from typing import Dict, Any
import pandas as pd
import json
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType

def authenticate_scgas(api_config: Dict[str, Any], credentials: Dict[str, Any]) -> str:
    """Autentica na API SCGAS e retorna o token de acesso."""
    
    auth_data = {
        "username": credentials["scgas_api"]["username"],
        "password": credentials["scgas_api"]["password"]
    }
    
    # Constrói a URL completa para autenticação
    auth_url = f"{api_config['api_scgas']['scgas']['base_url']}{api_config['api_scgas']['scgas']['authentication']['auth_url']}"
    print(f"URL de autenticação: {auth_url}")
    
    response = requests.post(
        auth_url, 
        json=auth_data, 
        headers=api_config['api_scgas']['scgas']['auth_headers']
    )
    
    if response.status_code == 200:
        token_json = response.json()
        print("Autenticação bem-sucedida")
        return token_json["access_token"]
    else:
        raise Exception(f"Erro ao autenticar: {response.status_code} - {response.text}")

def collect_measurements(auth_token: str, api_config: Dict[str, Any]) -> Dict[str, Any]:
    """Coleta dados de medição usando o token de autenticação."""
    
    # Prepara headers com o token
    headers = api_config['api_scgas']['scgas']['data_headers'].copy()
    headers["Authorization"] = f"Bearer {auth_token}"
    
    # Constrói a URL para coleta de dados
    data_url = f"{api_config['api_scgas']['scgas']['base_url']}{api_config['api_scgas']['scgas']['endpoints']['history_measurement']}"
    
    # Usa o body padrão da configuração
    request_body = api_config['api_scgas']['scgas']['measurement_request_body']
    
    print(f"URL de coleta: {data_url}")
    print(f"Body da requisição: {json.dumps(request_body, indent=2)}")
    
    response = requests.post(
        data_url,
        json=request_body,
        headers=headers
    )
    
    if response.status_code == 200:
        data = response.json()
        print(f"Dados coletados com sucesso. Registros: {len(data) if isinstance(data, list) else 'N/A'}")
        return data
    else:
        raise Exception(f"Erro ao coletar dados: {response.status_code} - {response.text}")

def create_dataframe(measurements_data: Dict[str, Any]) -> pd.DataFrame:
    """Cria um DataFrame pandas a partir dos dados da API."""
    
    print("Processando dados da API...")
    
    # Processa os dados da API
    if isinstance(measurements_data, list):
        # Se for uma lista, processa cada item
        processed_data = []
        for item in measurements_data:
            if isinstance(item, dict):
                # Extrai informações do item baseado na estrutura real da API
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
        
        # Cria DataFrame
        df = pd.DataFrame(processed_data)
    else:
        # Se for um dicionário único, cria DataFrame com uma linha
        df = pd.DataFrame([measurements_data])
    
    # Converte data para datetime se existir
    if 'data' in df.columns:
        df['data'] = pd.to_datetime(df['data'], errors='coerce')
    
    # Converte valores numéricos
    numeric_columns = ['valorConv', 'valorConvFormat', 'codVar', 'codEst', 'codMed', 'intervaloLeituraMin']
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    print(f"DataFrame criado com {len(df)} linhas e {len(df.columns)} colunas")
    print("Colunas do DataFrame:", list(df.columns))
    print("\nPrimeiras 5 linhas:")
    print(df.head())
    
    # Estatísticas básicas
    print("\nEstatísticas dos valores:")
    if 'valorConv' in df.columns:
        print(f"Valor médio: {df['valorConv'].mean():.2f}")
        print(f"Valor mínimo: {df['valorConv'].min():.2f}")
        print(f"Valor máximo: {df['valorConv'].max():.2f}")
    
    return df

def process_with_spark(measurements_dataframe: pd.DataFrame) -> Dict[str, Any]:
    """Processa os dados usando Spark do Databricks."""
    
    print("🔄 Iniciando processamento com Spark do Databricks...")
    
    try:
        # Obtém a SparkSession ativa (configurada pelo hook)
        spark = SparkSession.builder.getOrCreate()
        
        print(f"✅ Conectado ao Spark: {spark.version}")
        print(f"   Aplicação: {spark.conf.get('spark.app.name')}")
        
        # Converte DataFrame pandas para Spark
        spark_df = spark.createDataFrame(measurements_dataframe)
        
        print(f"📊 DataFrame Spark criado: {spark_df.count()} linhas, {len(spark_df.columns)} colunas")
        
        # Registra como tabela temporária para consultas SQL
        spark_df.createOrReplaceTempView("measurements_temp")
        
        # Exemplo de consulta SQL
        print("\n🔍 Executando consulta SQL...")
        result = spark.sql("""
            SELECT 
                estacao,
                COUNT(*) as total_medicoes,
                AVG(valorConv) as valor_medio,
                MIN(valorConv) as valor_minimo,
                MAX(valorConv) as valor_maximo
            FROM measurements_temp 
            WHERE valorConv IS NOT NULL
            GROUP BY estacao
            ORDER BY total_medicoes DESC
        """)
        
        print("📈 Resultado da agregação por estação:")
        result.show()
        
        # Converte resultado para formato serializável
        result_data = result.toPandas().to_dict('records')
        
        # Retorna dados processados em formato serializável
        return {
            "status": "success",
            "spark_version": spark.version,
            "total_records": spark_df.count(),
            "columns": list(spark_df.columns),
            "aggregation_results": result_data,
            "message": "Dados processados com sucesso usando Spark do Databricks"
        }
        
    except Exception as e:
        print(f"❌ Erro ao processar com Spark: {e}")
        print("   Retornando dados pandas processados...")
        
        # Fallback: retorna dados pandas em formato serializável
        return {
            "status": "fallback",
            "total_records": len(measurements_dataframe),
            "columns": list(measurements_dataframe.columns),
            "aggregation_results": [
                {
                    "estacao": measurements_dataframe['estacao'].iloc[0] if 'estacao' in measurements_dataframe.columns else "N/A",
                    "total_medicoes": len(measurements_dataframe),
                    "valor_medio": float(measurements_dataframe['valorConv'].mean()) if 'valorConv' in measurements_dataframe.columns else 0.0,
                    "valor_minimo": float(measurements_dataframe['valorConv'].min()) if 'valorConv' in measurements_dataframe.columns else 0.0,
                    "valor_maximo": float(measurements_dataframe['valorConv'].max()) if 'valorConv' in measurements_dataframe.columns else 0.0
                }
            ],
            "message": f"Usado fallback pandas devido a erro no Spark: {str(e)}"
        }