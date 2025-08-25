import requests
from typing import Dict, Any
import pandas as pd
import json
from datetime import datetime

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