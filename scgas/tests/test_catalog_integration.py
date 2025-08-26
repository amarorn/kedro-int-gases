"""
Testes para a integração com o catálogo do Databricks
"""
import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime
from scgas.pipelines.data_engineering.nodes import save_to_databricks_catalog


class TestCatalogIntegration:
    """Testes para integração com catálogo Databricks"""
    
    @pytest.fixture
    def mock_measurements_data(self):
        """Dados de medição mock"""
        return [
            {
                "codVar": "TEMP001",
                "tag": "HistTotal",
                "idIntegracao": "VRTA-CLT-41-023-0001",
                "unidade": "°C",
                "descricao": "Temperatura Ambiente",
                "data": "2025-01-01T00:00:00.000-03:00",
                "valorConv": 25.5,
                "valorConvFormat": 25.5,
                "estacao": "Estacao Central",
                "codEst": "EST001",
                "codMed": "MED001",
                "intervaloLeituraMin": 60
            },
            {
                "codVar": "PRESS001",
                "tag": "HistTotal",
                "idIntegracao": "VRTA-CLT-41-023-0001",
                "unidade": "hPa",
                "descricao": "Pressão Atmosférica",
                "data": "2025-01-01T00:00:00.000-03:00",
                "valorConv": 1013.25,
                "valorConvFormat": 1013.25,
                "estacao": "Estacao Central",
                "codEst": "EST001",
                "codMed": "MED002",
                "intervaloLeituraMin": 60
            }
        ]
    
    @pytest.fixture
    def mock_api_config(self):
        """Configuração da API mock"""
        return {
            'api_scgas': {
                'scgas': {
                    'base_url': 'https://scada.scgas.com.br'
                }
            }
        }
    
    @pytest.fixture
    def mock_catalog_config(self):
        """Configuração do catálogo mock"""
        return {
            "databricks_catalog": {
                "catalog_name": "shd_qas_internal_datalake",
                "table_name": "scgas_raw",
                "schema": [
                    {"name": "codVar", "type": "string", "nullable": True},
                    {"name": "tag", "type": "string", "nullable": True},
                    {"name": "valorConv", "type": "double", "nullable": True},
                    {"name": "data_coleta", "type": "timestamp", "nullable": True}
                ],
                "write_options": {
                    "mode": "append",
                    "merge_schema": True
                }
            }
        }
    
    @patch('scgas.pipelines.data_engineering.nodes.SparkSession')
    def test_save_to_databricks_catalog_success(self, mock_spark_session, 
                                               mock_measurements_data, 
                                               mock_api_config, 
                                               mock_catalog_config):
        """Testa salvamento bem-sucedido no catálogo"""
        
        # Mock da SparkSession
        mock_spark = Mock()
        mock_spark_session.builder.getOrCreate.return_value = mock_spark
        mock_spark.version = "3.4.0"
        mock_spark.conf.get.return_value = "test-app"
        
        # Mock do DataFrame
        mock_df = Mock()
        mock_df.count.return_value = 2
        mock_df.columns = ["codVar", "tag", "valorConv", "data_coleta"]
        mock_spark.createDataFrame.return_value = mock_df
        
        # Mock das operações SQL
        mock_spark.sql.return_value.collect.return_value = [{"total_registros": 2}]
        
        # Executa a função
        result = save_to_databricks_catalog(
            mock_measurements_data, 
            mock_api_config, 
            mock_catalog_config
        )
        
        # Verifica resultado
        assert result["status"] == "success"
        assert result["catalog"] == "shd_qas_internal_datalake"
        assert result["table"] == "scgas_raw"
        assert result["records_processed"] == 2
        assert result["total_records_in_table"] == 2
        
        # Verifica se as operações foram chamadas
        mock_spark.sql.assert_called()
        mock_df.write.mode.assert_called_with("append")
    
    @patch('scgas.pipelines.data_engineering.nodes.SparkSession')
    def test_save_to_databricks_catalog_with_default_schema(self, mock_spark_session,
                                                           mock_measurements_data,
                                                           mock_api_config):
        """Testa salvamento com schema padrão quando não há configuração"""
        
        # Configuração sem schema
        catalog_config = {
            "databricks_catalog": {
                "catalog_name": "shd_qas_internal_datalake",
                "table_name": "scgas_raw"
            }
        }
        
        # Mock da SparkSession
        mock_spark = Mock()
        mock_spark_session.builder.getOrCreate.return_value = mock_spark
        mock_spark.version = "3.4.0"
        mock_spark.conf.get.return_value = "test-app"
        
        # Mock do DataFrame
        mock_df = Mock()
        mock_df.count.return_value = 2
        mock_df.columns = ["codVar", "tag", "valorConv"]
        mock_spark.createDataFrame.return_value = mock_df
        
        # Mock das operações SQL
        mock_spark.sql.return_value.collect.return_value = [{"total_registros": 2}]
        
        # Executa a função
        result = save_to_databricks_catalog(
            mock_measurements_data,
            mock_api_config,
            catalog_config
        )
        
        # Verifica resultado
        assert result["status"] == "success"
        assert "schema" in result
    
    @patch('scgas.pipelines.data_engineering.nodes.SparkSession')
    def test_save_to_databricks_catalog_error(self, mock_spark_session,
                                             mock_measurements_data,
                                             mock_api_config,
                                             mock_catalog_config):
        """Testa tratamento de erro no salvamento"""
        
        # Mock da SparkSession que falha
        mock_spark_session.builder.getOrCreate.side_effect = Exception("Connection failed")
        
        # Executa a função
        result = save_to_databricks_catalog(
            mock_measurements_data,
            mock_api_config,
            mock_catalog_config
        )
        
        # Verifica resultado de erro
        assert result["status"] == "error"
        assert "error" in result
        assert "Connection failed" in result["error"]
    
    def test_catalog_config_validation(self, mock_measurements_data, mock_api_config):
        """Testa validação da configuração do catálogo"""
        
        # Configuração inválida
        invalid_config = {
            "databricks_catalog": {
                "catalog_name": "",  # Nome vazio
                "table_name": None   # Nome nulo
            }
        }
        
        with patch('scgas.pipelines.data_engineering.nodes.SparkSession') as mock_spark_session:
            mock_spark = Mock()
            mock_spark_session.builder.getOrCreate.return_value = mock_spark
            mock_spark.version = "3.4.0"
            mock_spark.conf.get.return_value = "test-app"
            
            # Mock do DataFrame
            mock_df = Mock()
            mock_df.count.return_value = 2
            mock_df.columns = ["codVar", "tag"]
            mock_spark.createDataFrame.return_value = mock_df
            
            # Mock das operações SQL
            mock_spark.sql.return_value.collect.return_value = [{"total_registros": 2}]
            
            # Executa a função
            result = save_to_databricks_catalog(
                mock_measurements_data,
                mock_api_config,
                invalid_config
            )
            
            # Verifica que usa valores padrão quando a configuração é inválida
            assert result["status"] == "success"
            # Como a configuração tem valores vazios/nulos, a função deve usar valores padrão
            # ou tratar os valores inválidos de forma apropriada
            assert "catalog" in result
            assert "table" in result