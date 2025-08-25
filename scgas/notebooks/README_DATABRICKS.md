# Pipeline SCGAS para Databricks

Este diretório contém notebooks e scripts para executar o pipeline de coleta de dados da API SCGAS no ambiente Databricks.

## 📁 Arquivos Disponíveis

### 1. `02_databricks_pipeline.ipynb`
- **Notebook Jupyter** para execução interativa no Databricks
- **Células organizadas** para cada etapa do pipeline
- **Visualizações** e análises exploratórias
- **Execução passo a passo** com comentários detalhados

### 2. `databricks_scgas_pipeline.py`
- **Script Python standalone** para execução automatizada
- **Pipeline completo** em um único arquivo
- **Execução via job** ou notebook
- **Logs estruturados** e tratamento de erros

## 🚀 Como Executar no Databricks

### Opção 1: Notebook Interativo

1. **Faça upload** do arquivo `02_databricks_pipeline.ipynb` para seu workspace
2. **Anexe a um cluster** com as seguintes especificações:
   - **Runtime**: Databricks Runtime 10.4 LTS ou superior
   - **Python**: 3.9+
   - **Bibliotecas**: requests, pandas, pyspark (já incluídas)
3. **Execute as células** sequencialmente

### Opção 2: Script Python

1. **Faça upload** do arquivo `databricks_scgas_pipeline.py`
2. **Crie um notebook** e execute:
   ```python
   %run /path/to/databricks_scgas_pipeline.py
   ```
3. **Ou execute via job** do Databricks

### Opção 3: Job Automatizado

1. **Crie um novo job** no Databricks
2. **Adicione uma task** do tipo "Python Script"
3. **Configure o script** para executar `databricks_scgas_pipeline.py`
4. **Agende a execução** conforme necessário

## ⚙️ Configuração do Ambiente

### Variáveis de Ambiente

Configure as credenciais via **Secrets do Databricks**:

```python
# No notebook ou script
import os
from pyspark.sql import SparkSession

# Configurar secrets
spark.conf.set("spark.databricks.secrets.scope", "scgas-secrets")
spark.conf.set("spark.databricks.secrets.key", "scgas-credentials")

# Ou via variáveis de ambiente
os.environ["SCGAS_USERNAME"] = "seu_usuario"
os.environ["SCGAS_PASSWORD"] = "sua_senha"
```

### Secrets do Databricks

1. **Acesse** Admin Console > Secrets
2. **Crie um scope** chamado `scgas-secrets`
3. **Adicione as chaves**:
   - `scgas-username`: seu usuário da API
   - `scgas-password`: sua senha da API

## 📊 Estrutura de Dados

### Campos Coletados

| Campo | Tipo | Descrição |
|-------|------|-----------|
| `codVar` | String | Código da variável |
| `tag` | String | Tag de identificação |
| `idIntegracao` | String | ID de integração |
| `unidade` | String | Unidade de medida |
| `descricao` | String | Descrição da medição |
| `data` | Timestamp | Data/hora da medição |
| `valorConv` | Double | Valor convertido |
| `valorConvFormat` | Double | Valor formatado |
| `estacao` | String | Estação de medição |
| `codEst` | String | Código da estação |
| `codMed` | String | Código da medição |
| `intervaloLeituraMin` | Double | Intervalo de leitura |

### Formato de Saída

Os dados são salvos em três formatos:

1. **Parquet** (`measurements.parquet`) - Otimizado para Spark
2. **CSV** (`measurements.csv`) - Compatível com pandas
3. **JSON** (`raw_data.json`) - Dados brutos da API

## 🔧 Personalizações

### Parâmetros de Consulta

Edite a função `load_configuration()` para ajustar:

```python
QUERY_PARAMS = {
    "idIntegracao": "SEU_ID_INTEGRACAO",
    "tagList": ["SUA_TAG"],
    "from": "2025-01-01T00:00:00.000-03:00",
    "to": "2025-08-07T00:00:00.000-03:00",
    "groupBy": "h",  # h=horário, d=dia, m=mês
    "calcBy": "val"  # val=valor, avg=média
}
```

### Configurações da API

Ajuste URLs e timeouts:

```python
API_CONFIG = {
    "base_url": "https://sua-api.com",
    "timeout": 60,  # segundos
    "retry_attempts": 3
}
```

## 📈 Análises Disponíveis

### Análise Exploratória

O pipeline executa automaticamente:

1. **Estatísticas básicas** dos valores de medição
2. **Análise por estação** de monitoramento
3. **Análise temporal** dos dados
4. **Qualidade dos dados** e consistência

### Queries SQL Personalizadas

Use o DataFrame Spark para análises adicionais:

```python
# Exemplo: Análise de tendências
trend_query = """
SELECT 
    DATE_TRUNC('day', data) as dia,
    AVG(valorConv) as valor_medio_dia,
    COUNT(*) as registros
FROM scgas_measurements 
GROUP BY DATE_TRUNC('day', data)
ORDER BY dia
"""
trends = spark.sql(trend_query)
trends.show()
```

## 🚨 Troubleshooting

### Problemas Comuns

1. **Erro de autenticação**
   - Verifique credenciais
   - Confirme se a API está acessível

2. **Timeout na coleta**
   - Aumente o valor de `timeout`
   - Verifique conectividade de rede

3. **Erro de memória**
   - Aumente recursos do cluster
   - Processe dados em lotes menores

### Logs e Debug

O script gera logs detalhados:

```
🚀 Configurando ambiente Databricks...
✅ Spark inicializado: 3.4.1
🔐 Autenticando na API SCGAS...
✅ Autenticação bem-sucedida
📊 Coletando dados de medição...
✅ Dados coletados com sucesso. Registros: 2,544
```

## 📞 Suporte

Para dúvidas ou problemas:

1. **Verifique os logs** de execução
2. **Confirme as configurações** da API
3. **Teste a conectividade** de rede
4. **Verifique as permissões** do cluster

---

**Pipeline SCGAS para Databricks** - Versão 1.0  
**Data**: 2025-08-25  
**Autor**: Equipe de Engenharia de Dados
