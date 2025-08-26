from kedro.pipeline import Pipeline, node
from .nodes import authenticate_scgas, collect_measurements, create_dataframe, process_with_spark, save_to_databricks_catalog

def create_pipeline(**kwargs) -> Pipeline:
    """Cria o pipeline de engenharia de dados para API SCGAS com Databricks."""
    
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
                outputs="measurements_dataframe",
                name="create_dataframe_node",
                tags=["data_processing", "pandas"],
            ),
            node(
                func=process_with_spark,
                inputs=["measurements_dataframe"],
                outputs="measurements_spark_df",
                name="process_with_spark_node",
                tags=["data_processing", "spark", "databricks"],
            ),
            node(
                func=save_to_databricks_catalog,
                inputs=["measurements_data", "api_config", "databricks_catalog"],
                outputs="catalog_save_result",
                name="save_to_databricks_catalog_node",
                tags=["data_storage", "catalog", "databricks"],
            ),
        ]
    )