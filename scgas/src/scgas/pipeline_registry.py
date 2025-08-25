"""Project pipelines."""

from kedro.framework.project import find_pipelines
from kedro.pipeline import Pipeline


def register_pipelines() -> dict[str, Pipeline]:
    """Register the project's pipelines.

    Returns:
        A mapping from pipeline names to ``Pipeline`` objects.
    """
    pipelines = find_pipelines()
    
    # Adiciona o pipeline de engenharia de dados
    from scgas.pipelines.data_engineering.pipeline import create_pipeline as create_data_engineering_pipeline
    pipelines["data_engineering"] = create_data_engineering_pipeline()
    
    # Pipeline padrão combina todos os pipelines
    pipelines["__default__"] = sum(pipelines.values())
    return pipelines
