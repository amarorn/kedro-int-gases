from kedro.pipeline import Pipeline, node
from .nodes import authenticate_scgas, collect_measurements

def create_pipeline(**kwargs) -> Pipeline:
    """Cria o pipeline de engenharia de dados para API SCGAS."""
    
    return Pipeline(
        [
            node(
                func=authenticate_scgas,
                inputs=["api_config", "credentials"],
                outputs="auth_token",
                name="authenticate_scgas_node",
                tags=["authentication", "api"],
            ),
            node(
                func=collect_measurements,
                inputs=["auth_token", "api_config"],
                outputs="measurements_data",
                name="collect_measurements_node",
                tags=["data_collection", "api"],
            ),
        ]
    )