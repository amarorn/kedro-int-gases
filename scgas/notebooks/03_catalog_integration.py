# 🔗 Integração com Catálogo Databricks - SCGAS
# 
# Este arquivo demonstra como os dados coletados da API SCGAS são processados 
# e salvos no catálogo shd_qas_internal_datalake.scgas_raw.
# 
# Para usar como notebook, copie as células para um notebook Jupyter.

# ============================================================================
# CÉLULA 1: Importações e Setup
# ============================================================================

# Importa as funções necessárias
from scgas.pipelines.data_engineering.nodes import (
    authenticate_scgas, 
    collect_measurements, 
    save_to_databricks_catalog
)
import json
from datetime import datetime

print("✅ Funções importadas com sucesso")

# ============================================================================
# CÉLULA 2: Carregamento de Configurações
# ============================================================================

# Carrega configurações
from kedro.framework.context import KedroContext
from kedro.framework.startup import bootstrap_project

# Inicializa o contexto do Kedro
bootstrap_project(".")
context = KedroContext(".")

# Carrega configurações
api_config = context.config_loader["api_config"]
credentials = context.config_loader["credentials"]
databricks_catalog_config = context.config_loader["databricks_catalog"]

print("✅ Configurações carregadas")
print(f"API Base URL: {api_config['api_scgas']['scgas']['base_url']}")
print(f"Catálogo: {databricks_catalog_config['databricks_catalog']['catalog_name']}")
print(f"Tabela: {databricks_catalog_config['databricks_catalog']['table_name']}")

# ============================================================================
# CÉLULA 3: Autenticação na API SCGAS
# ============================================================================

# Autentica na API SCGAS
print("🔐 Autenticando na API SCGAS...")
auth_token = authenticate_scgas(api_config, credentials)
print(f"✅ Token obtido: {auth_token[:20]}...")

# ============================================================================
# CÉLULA 4: Coleta de Dados da API
# ============================================================================

# Coleta dados de medição
print("📡 Coletando dados da API SCGAS...")
measurements_data = collect_measurements(auth_token, api_config)

print(f"✅ Dados coletados: {len(measurements_data) if isinstance(measurements_data, list) else 'N/A'} registros")

# Mostra exemplo dos dados
if isinstance(measurements_data, list) and len(measurements_data) > 0:
    print("\n📊 Exemplo dos dados coletados:")
    print(json.dumps(measurements_data[0], indent=2, default=str))

# ============================================================================
# CÉLULA 5: Salvamento no Catálogo Databricks
# ============================================================================

# Salva dados no catálogo Databricks
print("💾 Salvando dados no catálogo Databricks...")
catalog_result = save_to_databricks_catalog(
    measurements_data, 
    api_config, 
    databricks_catalog_config
)

print("\n📊 Resultado do salvamento:")
print(json.dumps(catalog_result, indent=2, default=str))

# ============================================================================
# CÉLULA 6: Verificação dos Dados Salvos
# ============================================================================

# Verifica os dados salvos usando Spark SQL
if catalog_result.get("status") == "success":
    from pyspark.sql import SparkSession
    
    spark = SparkSession.builder.getOrCreate()
    
    # Consulta os dados salvos
    full_table_name = f"{catalog_result['catalog']}.{catalog_result['table']}"
    
    print(f"🔍 Consultando dados da tabela: {full_table_name}")
    
    # Conta total de registros
    count_result = spark.sql(f"SELECT COUNT(*) as total FROM {full_table_name}")
    total_count = count_result.collect()[0]["total"]
    print(f"📊 Total de registros na tabela: {total_count}")
    
    # Mostra estrutura da tabela
    print("\n🏗️ Estrutura da tabela:")
    schema_result = spark.sql(f"DESCRIBE {full_table_name}")
    schema_result.show()
    
    # Mostra amostra dos dados
    print("\n📋 Amostra dos dados:")
    sample_result = spark.sql(f"SELECT * FROM {full_table_name} LIMIT 5")
    sample_result.show(truncate=False)
    
    # Estatísticas por estação
    print("\n📈 Estatísticas por estação:")
    stats_result = spark.sql(f"""
        SELECT 
            estacao,
            COUNT(*) as total_medicoes,
            AVG(valorConv) as valor_medio,
            MIN(valorConv) as valor_minimo,
            MAX(valorConv) as valor_maximo
        FROM {full_table_name}
        WHERE valorConv IS NOT NULL
        GROUP BY estacao
        ORDER BY total_medicoes DESC
    """)
    stats_result.show()
else:
    print(f"❌ Falha no salvamento: {catalog_result.get('error', 'Erro desconhecido')}")

# ============================================================================
# CÉLULA 7: Execução do Pipeline Completo
# ============================================================================

# Executa o pipeline completo via Kedro
print("🚀 Executando pipeline completo...")
print("\nPara executar o pipeline completo, use o comando:")
print("kedro run --pipeline=data_engineering")

print("\nOu para executar apenas o salvamento no catálogo:")
print("kedro run --nodes=save_to_databricks_catalog_node")

print("\n📋 Nodes do pipeline:")
print("1. authenticate_scgas_node - Autenticação na API")
print("2. collect_measurements_node - Coleta de dados")
print("3. create_dataframe_node - Processamento pandas")
print("4. process_with_spark_node - Processamento Spark")
print("5. save_to_databricks_catalog_node - Salvamento no catálogo (NOVO!)")

# ============================================================================
# CÉLULA 8: Monitoramento e Logs
# ============================================================================

# Informações de monitoramento
print("📊 Informações de Monitoramento:")
print(f"\n🕒 Timestamp de execução: {datetime.now().isoformat()}")
print(f"🔗 Catálogo: {catalog_result.get('catalog', 'N/A')}")
print(f"📋 Tabela: {catalog_result.get('table', 'N/A')}")
print(f"📈 Registros processados: {catalog_result.get('records_processed', 'N/A')}")
print(f"💾 Total na tabela: {catalog_result.get('total_records_in_table', 'N/A')}")
print(f"✅ Status: {catalog_result.get('status', 'N/A')}")

if catalog_result.get("schema"):
    print(f"\n🏗️ Schema da tabela:")
    for field in catalog_result["schema"]:
        print(f"  - {field}")

print("\n🎉 Processo concluído com sucesso!")