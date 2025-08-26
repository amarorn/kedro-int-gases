# 🔗 Integração com Catálogo Databricks - SCGAS

## 📋 Visão Geral

Este documento descreve a implementação da funcionalidade para processar dados JSON da API SCGAS e salvá-los no catálogo `shd_qas_internal_datalake.scgas_raw` do Databricks.

## 🎯 Objetivos

- **Processar dados JSON** coletados da API SCGAS
- **Criar tabela estruturada** no catálogo Databricks
- **Implementar schema flexível** baseado em configuração
- **Garantir integridade** dos dados salvos
- **Fornecer monitoramento** e logs detalhados

## 🏗️ Arquitetura

### Pipeline Atualizado

```
API SCGAS → Autenticação → Coleta → Pandas → Spark → Catálogo Databricks
                                    ↓
                              save_to_databricks_catalog_node (NOVO!)
```

### Novos Componentes

1. **`save_to_databricks_catalog`** - Função principal de salvamento
2. **`databricks_catalog.yml`** - Configuração do catálogo
3. **`catalog_save_result`** - Dataset de saída com resultados
4. **Testes automatizados** - Validação da funcionalidade

## 📁 Estrutura de Arquivos

```
scgas/
├── src/scgas/pipelines/data_engineering/
│   ├── nodes.py                    # ✅ Atualizado com nova função
│   └── pipeline.py                 # ✅ Atualizado com novo node
├── conf/base/
│   ├── catalog.yml                 # ✅ Atualizado com novo dataset
│   └── databricks_catalog.yml      # 🆕 Nova configuração
├── tests/
│   └── test_catalog_integration.py # 🆕 Novos testes
└── notebooks/
    └── 03_catalog_integration.py   # 🆕 Exemplo de uso
```

## ⚙️ Configuração

### Arquivo `databricks_catalog.yml`

```yaml
databricks_catalog:
  catalog_name: "shd_qas_internal_datalake"
  table_name: "scgas_raw"
  
  schema:
    - name: "codVar"
      type: "string"
      nullable: true
      description: "Código da variável"
    # ... outros campos
    
  write_options:
    mode: "append"
    merge_schema: true
    partition_by: []
  
  performance:
    coalesce_partitions: true
    adaptive_query_execution: true
```

### Schema da Tabela

| Campo | Tipo | Nullable | Descrição |
|-------|------|----------|-----------|
| `codVar` | string | true | Código da variável |
| `tag` | string | true | Tag de identificação |
| `idIntegracao` | string | true | ID de integração |
| `unidade` | string | true | Unidade de medida |
| `descricao` | string | true | Descrição da medição |
| `data` | timestamp | true | Data/hora da medição |
| `valorConv` | double | true | Valor convertido |
| `valorConvFormat` | double | true | Valor formatado |
| `estacao` | string | true | Estação de medição |
| `codEst` | string | true | Código da estação |
| `codMed` | string | true | Código da medição |
| `intervaloLeituraMin` | integer | true | Intervalo de leitura |
| `data_coleta` | timestamp | true | Data/hora da coleta |
| `processamento_timestamp` | timestamp | true | Data/hora do processamento |

## 🚀 Como Usar

### 1. Execução do Pipeline Completo

```bash
# Executa todo o pipeline incluindo salvamento no catálogo
kedro run --pipeline=data_engineering
```

### 2. Execução Apenas do Salvamento

```bash
# Executa apenas o node de salvamento no catálogo
kedro run --nodes=save_to_databricks_catalog_node
```

### 3. Execução via Python

```python
from scgas.pipelines.data_engineering.nodes import save_to_databricks_catalog

# Salva dados no catálogo
result = save_to_databricks_catalog(
    measurements_data, 
    api_config, 
    databricks_catalog_config
)

print(f"Status: {result['status']}")
print(f"Registros processados: {result['records_processed']}")
```

## 📊 Monitoramento e Logs

### Logs de Execução

```
🔄 Iniciando processamento para catálogo Databricks...
✅ Conectado ao Spark: 3.4.0
   Aplicação: scgas-project
🎯 Configuração do catálogo: shd_qas_internal_datalake.scgas_raw
📊 Dados processados: 150 registros
✅ DataFrame Spark criado: 150 linhas, 14 colunas
✅ Catálogo shd_qas_internal_datalake verificado/criado
✅ Schema default verificado/criado
✅ Dados salvos com sucesso na tabela shd_qas_internal_datalake.scgas_raw
📊 Total de registros na tabela: 150
```

### Métricas de Saída

```json
{
  "status": "success",
  "catalog": "shd_qas_internal_datalake",
  "table": "scgas_raw",
  "full_table_name": "shd_qas_internal_datalake.scgas_raw",
  "records_processed": 150,
  "total_records_in_table": 150,
  "schema": ["codVar", "tag", "valorConv", ...],
  "write_mode": "append",
  "merge_schema": true,
  "message": "Dados salvos com sucesso no catálogo shd_qas_internal_datalake.scgas_raw",
  "timestamp": "2025-01-01T10:00:00"
}
```

## 🔍 Verificação dos Dados

### Consulta SQL para Verificar

```sql
-- Conta total de registros
SELECT COUNT(*) as total_registros 
FROM shd_qas_internal_datalake.scgas_raw;

-- Mostra estrutura da tabela
DESCRIBE shd_qas_internal_datalake.scgas_raw;

-- Amostra dos dados
SELECT * FROM shd_qas_internal_datalake.scgas_raw 
LIMIT 5;

-- Estatísticas por estação
SELECT 
    estacao,
    COUNT(*) as total_medicoes,
    AVG(valorConv) as valor_medio,
    MIN(valorConv) as valor_minimo,
    MAX(valorConv) as valor_maximo
FROM shd_qas_internal_datalake.scgas_raw
WHERE valorConv IS NOT NULL
GROUP BY estacao
ORDER BY total_medicoes DESC;
```

## 🧪 Testes

### Execução dos Testes

```bash
# Executa todos os testes
pytest

# Executa apenas testes do catálogo
pytest tests/test_catalog_integration.py -v

# Executa com cobertura
pytest --cov=src/scgas tests/test_catalog_integration.py
```

### Cobertura de Testes

- ✅ **Sucesso no salvamento** - Valida salvamento correto
- ✅ **Schema padrão** - Testa fallback para schema padrão
- ✅ **Tratamento de erro** - Valida tratamento de falhas
- ✅ **Validação de configuração** - Testa configurações inválidas

## ⚠️ Considerações Importantes

### 1. **Permissões do Databricks**
- Usuário deve ter permissão para criar catálogos e tabelas
- Cluster deve estar ativo e acessível
- Configuração de conectividade deve estar correta

### 2. **Configuração de Credenciais**
- Credenciais da API SCGAS devem estar configuradas
- Token de acesso ao Databricks deve ser válido
- Configurações locais devem estar em `conf/local/`

### 3. **Performance e Escalabilidade**
- Dados são salvos em modo `append` por padrão
- Schema é mesclado automaticamente (`mergeSchema: true`)
- Particionamento pode ser configurado se necessário

### 4. **Tratamento de Erros**
- Fallback para processamento pandas se Spark falhar
- Logs detalhados para debugging
- Retorno estruturado com status e mensagens

## 🔄 Próximos Passos

### Melhorias Planejadas

1. **Particionamento automático** por data
2. **Compressão otimizada** para diferentes tipos de dados
3. **Validação de dados** antes do salvamento
4. **Métricas de qualidade** dos dados salvos
5. **Notificações** para falhas ou sucessos

### Integrações Futuras

1. **Delta Lake** para versionamento de dados
2. **Unity Catalog** para governança avançada
3. **Streaming** para dados em tempo real
4. **MLflow** para experimentos de ML

## 📞 Suporte

### Logs e Debugging

- Verifique logs do Kedro para detalhes de execução
- Use `kedro viz` para visualizar o pipeline
- Consulte logs do cluster Databricks para erros de Spark

### Comandos Úteis

```bash
# Visualizar pipeline
kedro viz

# Ver configurações
kedro info

# Executar com debug
kedro run --pipeline=data_engineering --verbose

# Limpar dados intermediários
kedro catalog clean
```

---

**Versão**: 1.0.0  
**Data**: Janeiro 2025  
**Autor**: Equipe de Engenharia de Dados  
**Status**: ✅ Implementado e Testado