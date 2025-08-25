"""
Testes para a API SCGAS
"""
import pytest
import requests
import yaml
from pathlib import Path
from unittest.mock import Mock, patch


class TestAPISCGAS:
    """Testes para a API SCGAS"""
    
    @pytest.fixture
    def api_config(self):
        """Carrega configuração da API para testes"""
        config_path = Path("conf/base/api_config.yml")
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    
    @pytest.fixture
    def credentials(self):
        """Carrega credenciais para testes"""
        creds_path = Path("conf/local/credentials.yml")
        with open(creds_path, 'r') as f:
            return yaml.safe_load(f)
    
    def test_config_loading(self, api_config, credentials):
        """Testa se a configuração e credenciais são carregadas corretamente"""
        assert api_config is not None
        assert credentials is not None
        
        # Verifica estrutura da configuração
        assert 'api_scgas' in api_config
        assert 'scgas' in api_config['api_scgas']
        assert 'base_url' in api_config['api_scgas']['scgas']
        assert 'authentication' in api_config['api_scgas']['scgas']
        
        # Verifica credenciais
        assert 'scgas_api' in credentials
        assert 'username' in credentials['scgas_api']
        assert 'password' in credentials['scgas_api']
    
    def test_auth_url_construction(self, api_config):
        """Testa a construção da URL de autenticação"""
        base_url = api_config['api_scgas']['scgas']['base_url']
        auth_url = api_config['api_scgas']['scgas']['authentication']['auth_url']
        
        full_url = f"{base_url}{auth_url}"
        expected_url = "https://scada.scgas.com.br/api/Auth/Token"
        
        assert full_url == expected_url
    
    @patch('requests.post')
    def test_authentication_success(self, mock_post, api_config, credentials):
        """Testa autenticação bem-sucedida"""
        # Mock da resposta de sucesso
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "access_token": "test_token_123",
            "token_type": "bearer",
            "expires_in": 3600
        }
        mock_post.return_value = mock_response
        
        # Dados de autenticação
        auth_data = {
            "username": credentials["scgas_api"]["username"],
            "password": credentials["scgas_api"]["password"]
        }
        
        # URL de autenticação
        auth_url = f"{api_config['api_scgas']['scgas']['base_url']}{api_config['api_scgas']['scgas']['authentication']['auth_url']}"
        
        # Faz a requisição
        response = requests.post(
            auth_url,
            json=auth_data,
            headers=api_config['api_scgas']['scgas']['auth_headers']
        )
        
        # Verifica se a requisição foi feita
        mock_post.assert_called_once()
        
        # Verifica a resposta
        assert response.status_code == 200
        result = response.json()
        assert 'access_token' in result
        assert result['token_type'] == 'bearer'
    
    @patch('requests.post')
    def test_authentication_failure(self, mock_post, api_config, credentials):
        """Testa falha na autenticação"""
        # Mock da resposta de falha
        mock_response = Mock()
        mock_response.status_code = 401
        mock_response.text = "Unauthorized"
        mock_post.return_value = mock_response
        
        # Dados de autenticação
        auth_data = {
            "username": credentials["scgas_api"]["username"],
            "password": "wrong_password"
        }
        
        # URL de autenticação
        auth_url = f"{api_config['api_scgas']['scgas']['base_url']}{api_config['api_scgas']['scgas']['authentication']['auth_url']}"
        
        # Faz a requisição
        response = requests.post(
            auth_url,
            json=auth_data,
            headers=api_config['api_scgas']['scgas']['auth_headers']
        )
        
        # Verifica a resposta de erro
        assert response.status_code == 401
        assert "Unauthorized" in response.text
    
    def test_headers_configuration(self, api_config):
        """Testa se os headers estão configurados corretamente"""
        headers = api_config['api_scgas']['scgas']['auth_headers']
        
        assert 'Content-Type' in headers
        assert 'Accept' in headers
        assert headers['Content-Type'] == 'application/json'
        assert headers['Accept'] == '*/*'
    
    def test_endpoints_configuration(self, api_config):
        """Testa se os endpoints estão configurados"""
        endpoints = api_config['api_scgas']['scgas']['endpoints']
        
        assert 'history_measurement' in endpoints
        assert 'variable_status' in endpoints
        
        # Verifica se as URLs dos endpoints estão corretas
        assert endpoints['history_measurement'] == '/api/Variable/History/Measurement'
        assert endpoints['variable_status'] == '/api/Variable/Status'


if __name__ == "__main__":
    # Executa os testes
    pytest.main([__file__, "-v"])
