import requests
from typing import Dict, Any
import pandas as pd
import json
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
import logging
import re
from pathlib import Path

from kedro.io import AbstractDataSet
from kedro_datasets.spark.spark_dataset import get_spark
from pyspark.sql import DataFrame, SparkSession



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

def get_dbutils(
    spark: SparkSession,
)-> Any | Any | None:
    try:
        from pyspark.dbutils import DBUtils
        
        if "dbutils" not in locals():
            utils: Any = DBUtils(spark)
            return utils
        else:
            return locals().get("dbutils")
    except ImportError:
        return None
    
def get_spark_session(app_name: str) -> SparkSession:
    """Obtém uma SparkSession com o nome da aplicação."""
def save_to_databricks_catalog(measurements_data: Dict[str, Any], api_config: Dict[str, Any], databricks_catalog_config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Processa os dados JSON da API SCGAS e grava no catálogo shd_qas_internal_datalake.scgas_raw.
    Cria a tabela se ela não existir.
    """
    
    print("🔄 Iniciando processamento para catálogo Databricks...")
    
    try:
        # Obtém a SparkSession ativa (configurada pelo hook)
        spark = SparkSession.builder.getOrCreate()
        
        print(f"✅ Conectado ao Spark: {spark.version}")
        print(f"   Aplicação: {spark.conf.get('spark.app.name')}")
        
        # Obtém configurações do catálogo
        catalog_config = databricks_catalog_config.get("databricks_catalog", {})
        catalog_name = catalog_config.get("catalog_name", "shd_qas_internal_datalake")
        table_name = catalog_config.get("table_name", "scgas_raw")
        full_table_name = f"{catalog_name}.{table_name}"
        
        print(f"🎯 Configuração do catálogo: {catalog_name}.{table_name}")
        
        # Define o schema da tabela SCGAS baseado na configuração
        schema_config = catalog_config.get("schema", [])
        if schema_config:
            # Cria schema dinamicamente baseado na configuração
            fields = []
            for field_config in schema_config:
                field_name = field_config["name"]
                field_type = field_config["type"]
                nullable = field_config.get("nullable", True)
                
                # Mapeia tipos da configuração para tipos Spark
                if field_type == "string":
                    spark_type = StringType()
                elif field_type == "double":
                    spark_type = DoubleType()
                elif field_type == "integer":
                    spark_type = IntegerType()
                elif field_type == "timestamp":
                    spark_type = TimestampType()
                else:
                    spark_type = StringType()  # Default para string
                
                fields.append(StructField(field_name, spark_type, nullable))
            
            scgas_schema = StructType(fields)
        else:
            # Schema padrão se não houver configuração
            scgas_schema = StructType([
                StructField("codVar", StringType(), True),
                StructField("tag", StringType(), True),
                StructField("idIntegracao", StringType(), True),
                StructField("unidade", StringType(), True),
                StructField("descricao", StringType(), True),
                StructField("data", TimestampType(), True),
                StructField("valorConv", DoubleType(), True),
                StructField("valorConvFormat", DoubleType(), True),
                StructField("estacao", StringType(), True),
                StructField("codEst", StringType(), True),
                StructField("codMed", StringType(), True),
                StructField("intervaloLeituraMin", IntegerType(), True),
                StructField("data_coleta", TimestampType(), True),
                StructField("processamento_timestamp", TimestampType(), True)
            ])
        
        # Processa os dados da API
        processed_data = []
        current_timestamp = datetime.now()
        
        if isinstance(measurements_data, list):
            for item in measurements_data:
                if isinstance(item, dict):
                    processed_item = {
                        "codVar": item.get("codVar", ""),
                        "tag": item.get("tag", ""),
                        "idIntegracao": item.get("idIntegracao", ""),
                        "unidade": item.get("unidade", ""),
                        "descricao": item.get("descricao", ""),
                        "data": item.get("data", None),
                        "valorConv": float(item.get("valorConv", 0.0)) if item.get("valorConv") is not None else None,
                        "valorConvFormat": float(item.get("valorConvFormat", 0.0)) if item.get("valorConvFormat") is not None else None,
                        "estacao": item.get("estacao", ""),
                        "codEst": item.get("codEst", ""),
                        "codMed": item.get("codMed", ""),
                        "intervaloLeituraMin": int(item.get("intervaloLeituraMin", 0)) if item.get("intervaloLeituraMin") is not None else None,
                        "data_coleta": current_timestamp,
                        "processamento_timestamp": current_timestamp
                    }
                    processed_data.append(processed_item)
        else:
            # Se for um dicionário único
            processed_data = [measurements_data]
        
        print(f"📊 Dados processados: {len(processed_data)} registros")
        
        # Cria DataFrame Spark com o schema definido
        spark_df = spark.createDataFrame(processed_data, schema=scgas_schema)
        
        print(f"✅ DataFrame Spark criado: {spark_df.count()} linhas, {len(spark_df.columns)} colunas")
        
        # Verifica se o catálogo existe, se não, cria
        try:
            spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")
            print(f"✅ Catálogo {catalog_name} verificado/criado")
        except Exception as e:
            print(f"⚠️  Aviso ao verificar catálogo: {e}")
        
        # Verifica se o schema existe, se não, cria
        try:
            spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.default")
            print(f"✅ Schema default verificado/criado")
        except Exception as e:
            print(f"⚠️  Aviso ao verificar schema: {e}")
        
        # Obtém configurações de escrita
        write_options = catalog_config.get("write_options", {})
        write_mode = write_options.get("mode", "append")
        merge_schema = write_options.get("merge_schema", True)
        
        # Salva os dados na tabela (cria se não existir)
        try:
            # Usa saveAsTable para criar a tabela se não existir
            writer = spark_df.write.mode(write_mode)
            
            if merge_schema:
                writer = writer.option("mergeSchema", "true")
            
            # Aplica particionamento se configurado
            partition_by = write_options.get("partition_by", [])
            if partition_by:
                writer = writer.partitionBy(partition_by)
            
            writer.saveAsTable(full_table_name)
            
            print(f"✅ Dados salvos com sucesso na tabela {full_table_name}")
            
            # Verifica se a tabela foi criada e conta os registros
            table_info = spark.sql(f"SELECT COUNT(*) as total_registros FROM {full_table_name}")
            total_records = table_info.collect()[0]["total_registros"]
            
            print(f"📊 Total de registros na tabela: {total_records}")
            
            # Retorna informações de sucesso
            return {
                "status": "success",
                "catalog": catalog_name,
                "table": table_name,
                "full_table_name": full_table_name,
                "records_processed": len(processed_data),
                "total_records_in_table": total_records,
                "schema": [field.name for field in scgas_schema.fields],
                "write_mode": write_mode,
                "merge_schema": merge_schema,
                "message": f"Dados salvos com sucesso no catálogo {catalog_name}.{table_name}",
                "timestamp": current_timestamp.isoformat()
            }
            
        except Exception as e:
            print(f"❌ Erro ao salvar na tabela: {e}")
            raise Exception(f"Falha ao salvar dados na tabela {full_table_name}: {str(e)}")
        
    except Exception as e:
        print(f"❌ Erro no processamento para catálogo: {e}")
        return {
            "status": "error",
            "error": str(e),
            "message": "Falha no processamento para catálogo Databricks",
            "timestamp": datetime.now().isoformat()
        }