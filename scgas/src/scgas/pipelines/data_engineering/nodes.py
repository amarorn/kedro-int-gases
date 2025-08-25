import requests
from typing import Dict, Any

def authenticate_scgas(api_config: Dict[str, Any], credentials: Dict[str, Any]) -> Dict[str, Any]:
    """Autentica na API SCGAS e retorna o token de acesso."""
    
    auth_data = {
        "username": credentials["username"],
        "password": credentials["password"]
    }
    
    # Constrói a URL completa para autenticação
    auth_url = f"{api_config['base_url']}{api_config['authentication']['auth_url']}"
    print(auth_url)
    
    response = requests.post(
        auth_url, 
        json=auth_data, 
        headers=api_config['auth_headers']
    )
    
    if response.status_code == 200:
        print(response.json())
        return response.json()
    else:
        raise Exception(f"Erro ao autenticar: {response.status_code} - {response.text}")

def collect_measurements(auth_token: str, api_config: Dict[str, Any]) -> Dict[str, Any]:
    """Coleta dados de medição usando o token de autenticação."""
    
    # Prepara headers com o token
    headers = api_config['data_headers'].copy()
    headers["Authorization"] = f"Bearer {auth_token}"
    
    # Constrói a URL para coleta de dados
    data_url = f"{api_config['base_url']}{api_config['endpoints']['history_measurement']}"
    
    response = requests.get(
        data_url,
        headers=headers
    )
    
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Erro ao coletar dados: {response.status_code} - {response.text}")