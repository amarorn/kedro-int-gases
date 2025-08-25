from kedro.pipeline import Pipeline, node
from scgas.pipelines.data_engineering.nodes import authenticate_scgas, collect_measurements, create_dataframe

def create_pipeline(**kwargs) -> Pipeline:
    
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
            node(
                func=create_dataframe,
                inputs=["measurements_data"],
                outputs="measurements_df",
                name="create_dataframe_node",
                tags=["data_processing", "pandas"],
            ),
        ]
    )