# 🚀 Configuração do Databricks para o Projeto SCGAS

## 📋 Pré-requisitos

1. **Cluster Databricks ativo** no workspace
2. **Personal Access Token (PAT)** configurado
3. **Python 3.9+** instalado localmente

## 🔧 Passos para Configuração

### 1. **Encontrar o Cluster ID**

1. Acesse seu workspace Databricks: `https://adb-3588048019585666.6.azuredatabricks.net`
2. Vá para **Clusters** no menu lateral
3. Clique no cluster que deseja usar
4. Na aba **Configurações**, copie o **Cluster ID**

### 2. **Criar Personal Access Token**

1. No Databricks, clique no seu usuário (canto superior direito)
2. Selecione **User Settings**
3. Vá para **Access Tokens**
4. Clique em **Generate New Token**
5. Dê um nome (ex: "SCGAS Project")
6. Copie o token gerado

### 3. **Configurar o Arquivo Local**

Edite o arquivo `conf/local/databricks_cluster.yml`:

```yaml
databricks_cluster:
  cluster_id: "1234-567890-abcdef12"  # Substitua pelo seu Cluster ID
  personal_access_token: "dapi1234567890abcdef..."  # Substitua pelo seu PAT
  cluster_name: "Meu Cluster SCGAS"  # Nome para referência
```

### 4. **Instalar Dependências**

```bash
pip3 install -r requirements.txt
```

### 5. **Testar Conexão**

```bash
# Testar se o Databricks Connect está funcionando
python3 -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
print(f'Spark version: {spark.version}')
print(f'App name: {spark.conf.get(\"spark.app.name\")}')
"
```

## 🏗️ Estrutura do Projeto

```
scgas/
├── conf/
│   ├── base/
│   │   ├── databricks.yml          # Configuração base do Databricks
│   │   └── catalog.yml             # Catálogo de datasets
│   └── local/
│       └── databricks_cluster.yml  # Configuração local (não commitada)
├── src/scgas/
│   ├── hooks.py                    # Hook para conectar ao Databricks
│   └── pipelines/
│       └── data_engineering/
│           ├── nodes.py            # Nodes incluindo processamento Spark
│           └── pipeline.py         # Pipeline com 4 nodes
└── requirements.txt                 # Dependências incluindo databricks-connect
```

## 🚀 Executar o Pipeline

```bash
# Executar pipeline completo
kedro run --pipeline=data_engineering

# Executar apenas processamento Spark
kedro run --nodes=process_with_spark_node
```

## 📊 Nodes do Pipeline

1. **`authenticate_scgas_node`** - Autentica na API SCGAS
2. **`collect_measurements_node`** - Coleta dados da API
3. **`create_dataframe_node`** - Processa com pandas
4. **`process_with_spark_node`** - Processa com Spark do Databricks

## 🔍 Monitoramento

- **Logs**: Verifique os logs do Kedro para status da conexão
- **Databricks**: Acesse o workspace para ver execuções do cluster
- **Métricas**: Use `kedro viz` para visualizar o pipeline

## ⚠️ Troubleshooting

### **Erro de Conexão**
```bash
# Verificar se o cluster está ativo
# Verificar se o PAT é válido
# Verificar se o Cluster ID está correto
```

### **Erro de Versão**
```bash
# Verificar compatibilidade do PySpark
pip3 list | grep pyspark
pip3 list | grep databricks
```

### **Fallback Local**
Se houver erro no Databricks, o sistema usa PySpark local automaticamente.

## 📚 Recursos Adicionais

- [Databricks Connect Documentation](https://docs.databricks.com/dev-tools/databricks-connect.html)
- [PySpark Documentation](https://spark.apache.org/docs/latest/api/python/)
- [Kedro Documentation](https://docs.kedro.org/)
