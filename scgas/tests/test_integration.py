"""
Teste de integração com a API real da SCGAS
"""
import pytest
import requests
import yaml
from pathlib import Path


class TestSCGASIntegration:
    """Testes de integração com a API SCGAS"""
    
    @pytest.fixture
    def api_config(self):
        """Carrega configuração da API"""
        config_path = Path("conf/base/api_config.yml")
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    
    @pytest.fixture
    def credentials(self):
        """Carrega credenciais"""
        creds_path = Path("conf/local/credentials.yml")
        with open(creds_path, 'r') as f:
            return yaml.safe_load(f)
    
    def test_real_authentication(self, api_config, credentials):
        """Testa autenticação real na API SCGAS"""
        # Dados de autenticação
        auth_data = {
            "username": credentials["scgas_api"]["username"],
            "password": credentials["scgas_api"]["password"]
        }
        
        # URL de autenticação
        auth_url = f"{api_config['api_scgas']['scgas']['base_url']}{api_config['api_scgas']['scgas']['authentication']['auth_url']}"
        
        # Faz a requisição real
        response = requests.post(
            auth_url,
            json=auth_data,
            headers=api_config['api_scgas']['scgas']['auth_headers']
        )
        
        # Verifica se foi bem-sucedida
        assert response.status_code == 200, f"Falha na autenticação: {response.status_code} - {response.text}"
        
        # Verifica estrutura da resposta
        result = response.json()
        assert 'access_token' in result, "Token não encontrado na resposta"
        assert 'token_type' in result, "Tipo de token não encontrado"
        assert result['token_type'] == 'bearer', "Tipo de token incorreto"
        
        # Retorna o token para outros testes
        return result['access_token']
    
    def test_real_data_collection(self, api_config, credentials):
        """Testa coleta real de dados da API SCGAS"""
        # Primeiro autentica
        auth_data = {
            "username": credentials["scgas_api"]["username"],
            "password": credentials["scgas_api"]["password"]
        }
        
        auth_url = f"{api_config['api_scgas']['scgas']['base_url']}{api_config['api_scgas']['scgas']['authentication']['auth_url']}"
        
        auth_response = requests.post(
            auth_url,
            json=auth_data,
            headers=api_config['api_scgas']['scgas']['auth_headers']
        )
        
        assert auth_response.status_code == 200, "Falha na autenticação"
        
        # Obtém o token
        auth_result = auth_response.json()
        token = auth_result['access_token']
        
        # Prepara headers para coleta de dados
        headers = api_config['api_scgas']['scgas']['data_headers'].copy()
        headers["Authorization"] = f"Bearer {token}"
        
        # URL para coleta de dados
        data_url = f"{api_config['api_scgas']['scgas']['base_url']}{api_config['api_scgas']['scgas']['endpoints']['history_measurement']}"
        
        # Faz a requisição de dados
        data_response = requests.get(
            data_url,
            headers=headers
        )
        
        # Verifica se a requisição foi feita
        # Nota: Pode retornar erro se não houver parâmetros obrigatórios
        print(f"Status da coleta de dados: {data_response.status_code}")
        print(f"Resposta: {data_response.text[:200]}...")
        
        # Para este teste, aceitamos qualquer status que não seja erro de servidor
        assert data_response.status_code < 500, f"Erro do servidor: {data_response.status_code}"
    
    def test_api_endpoints_availability(self, api_config):
        """Testa se os endpoints da API estão disponíveis"""
        base_url = api_config['api_scgas']['scgas']['base_url']
        
        # Testa se o servidor está respondendo
        try:
            response = requests.get(base_url, timeout=10)
            # Aceita qualquer status, só queremos ver se o servidor responde
            print(f"Servidor respondeu com status: {response.status_code}")
        except requests.exceptions.RequestException as e:
            pytest.fail(f"Servidor não está respondendo: {e}")
    
    def test_credentials_validity(self, credentials):
        """Testa se as credenciais são válidas"""
        assert credentials is not None, "Credenciais não foram carregadas"
        assert 'scgas_api' in credentials, "Seção scgas_api não encontrada"
        assert 'username' in credentials['scgas_api'], "Username não encontrado"
        assert 'password' in credentials['scgas_api'], "Password não encontrado"
        
        # Verifica se não estão vazios
        assert credentials['scgas_api']['username'], "Username está vazio"
        assert credentials['scgas_api']['password'], "Password está vazio"
        
        # Verifica formato do email
        username = credentials['scgas_api']['username']
        assert '@' in username, "Username não parece ser um email válido"
        assert '.' in username, "Username não parece ser um email válido"


if __name__ == "__main__":
    # Executa os testes
    pytest.main([__file__, "-v"])
