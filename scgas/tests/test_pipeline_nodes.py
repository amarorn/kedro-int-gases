"""
Testes para os nodes do pipeline de engenharia de dados
"""
import pytest
from unittest.mock import Mock, patch
from scgas.pipelines.data_engineering.nodes import authenticate_scgas, collect_measurements


class TestPipelineNodes:
    """Testes para os nodes do pipeline"""
    
    @pytest.fixture
    def mock_api_config(self):
        """Configuração mock da API"""
        return {
            'base_url': 'https://scada.scgas.com.br',
            'authentication': {
                'auth_url': '/api/Auth/Token'
            },
            'auth_headers': {
                'Content-Type': 'application/json',
                'Accept': '*/*'
            },
            'data_headers': {
                'Content-Type': 'application/json',
                'Accept': '*/*'
            },
            'endpoints': {
                'history_measurement': '/api/Variable/History/Measurement'
            }
        }
    
    @pytest.fixture
    def mock_credentials(self):
        """Credenciais mock"""
        return {
            'username': 'test@example.com',
            'password': 'test_password'
        }
    
    @patch('requests.post')
    def test_authenticate_scgas_success(self, mock_post, mock_api_config, mock_credentials):
        """Testa autenticação bem-sucedida"""
        # Mock da resposta
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'access_token': 'test_token_123',
            'token_type': 'bearer'
        }
        mock_post.return_value = mock_response
        
        # Executa a função
        result = authenticate_scgas(mock_api_config, mock_credentials)
        
        # Verifica resultado
        assert result['access_token'] == 'test_token_123'
        assert result['token_type'] == 'bearer'
        
        # Verifica se a requisição foi feita corretamente
        mock_post.assert_called_once()
        call_args = mock_post.call_args
        
        # Verifica URL
        expected_url = 'https://scada.scgas.com.br/api/Auth/Token'
        assert call_args[0][0] == expected_url
        
        # Verifica dados
        assert call_args[1]['json']['username'] == 'test@example.com'
        assert call_args[1]['json']['password'] == 'test_password'
    
    @patch('requests.post')
    def test_authenticate_scgas_failure(self, mock_post, mock_api_config, mock_credentials):
        """Testa falha na autenticação"""
        # Mock da resposta de erro
        mock_response = Mock()
        mock_response.status_code = 401
        mock_response.text = 'Unauthorized'
        mock_post.return_value = mock_response
        
        # Executa e verifica se levanta exceção
        with pytest.raises(Exception) as exc_info:
            authenticate_scgas(mock_api_config, mock_credentials)
        
        assert 'Erro ao autenticar: 401' in str(exc_info.value)
    
    @patch('requests.get')
    def test_collect_measurements_success(self, mock_get, mock_api_config):
        """Testa coleta de dados bem-sucedida"""
        # Mock da resposta
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'data': [
                {'variable': 'temp', 'value': 25.5, 'timestamp': '2025-08-25T01:00:00Z'},
                {'variable': 'pressure', 'value': 1013.25, 'timestamp': '2025-08-25T01:00:00Z'}
            ]
        }
        mock_get.return_value = mock_response
        
        # Token de teste
        auth_token = 'test_token_123'
        
        # Executa a função
        result = collect_measurements(auth_token, mock_api_config)
        
        # Verifica resultado
        assert 'data' in result
        assert len(result['data']) == 2
        assert result['data'][0]['variable'] == 'temp'
        
        # Verifica se a requisição foi feita corretamente
        mock_get.assert_called_once()
        call_args = mock_get.call_args
        
        # Verifica URL
        expected_url = 'https://scada.scgas.com.br/api/Variable/History/Measurement'
        assert call_args[0][0] == expected_url
        
        # Verifica headers com token
        headers = call_args[1]['headers']
        assert headers['Authorization'] == 'Bearer test_token_123'
    
    @patch('requests.get')
    def test_collect_measurements_failure(self, mock_get, mock_api_config):
        """Testa falha na coleta de dados"""
        # Mock da resposta de erro
        mock_response = Mock()
        mock_response.status_code = 403
        mock_response.text = 'Forbidden'
        mock_get.return_value = mock_response
        
        # Token de teste
        auth_token = 'test_token_123'
        
        # Executa e verifica se levanta exceção
        with pytest.raises(Exception) as exc_info:
            collect_measurements(auth_token, mock_api_config)
        
        assert 'Erro ao coletar dados: 403' in str(exc_info.value)
    
    def test_headers_copy_in_collect_measurements(self, mock_api_config):
        """Testa se os headers são copiados corretamente"""
        with patch('requests.get') as mock_get:
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.json.return_value = {'data': []}
            mock_get.return_value = mock_response
            
            auth_token = 'test_token_123'
            collect_measurements(auth_token, mock_api_config)
            
            # Verifica se os headers originais não foram modificados
            assert 'Authorization' not in mock_api_config['data_headers']
            
            # Verifica se a requisição foi feita com headers corretos
            call_args = mock_get.call_args
            headers = call_args[1]['headers']
            assert headers['Authorization'] == 'Bearer test_token_123'
            assert headers['Content-Type'] == 'application/json'


if __name__ == "__main__":
    # Executa os testes
    pytest.main([__file__, "-v"])
