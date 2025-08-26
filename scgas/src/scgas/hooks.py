from kedro.framework.hooks import hook_impl
from pyspark import SparkConf
from pyspark.sql import SparkSession
import os


class DatabricksSparkHooks:
    @hook_impl
    def after_context_created(self, context) -> None:
        """Inicializa uma SparkSession conectada ao Databricks usando o config
        definido no projeto.
        """
        
        try:
            # Carrega a configuração do Databricks
            databricks_config = context.config_loader["databricks"]
            cluster_config = context.config_loader["databricks_cluster"]
            
            # Configura variáveis de ambiente para o Databricks Connect
            os.environ["DATABRICKS_HOST"] = databricks_config["workspace"]["host"]
            os.environ["DATABRICKS_TOKEN"] = cluster_config["databricks_cluster"]["personal_access_token"]
            os.environ["DATABRICKS_CLUSTER_ID"] = cluster_config["databricks_cluster"]["cluster_id"]
            
            # Configuração do Spark para Databricks
            spark_conf = SparkConf()
            
            # Configurações específicas do Databricks
            spark_conf.set("spark.databricks.service.enabled", "true")
            spark_conf.set("spark.databricks.service.port", "15001")
            spark_conf.set("spark.databricks.service.server.enabled", "true")
            
            # Configurações de performance
            spark_conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
            spark_conf.set("spark.sql.execution.arrow.pyspark.fallback.enabled", "true")
            spark_conf.set("spark.driver.maxResultSize", "3g")
            spark_conf.set("spark.sql.adaptive.enabled", "true")
            spark_conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
            
            # Inicializa a SparkSession conectada ao Databricks
            spark_session_conf = (
                SparkSession.builder.appName(f"scgas-{context.project_path.name}")
                .config(conf=spark_conf)
                .remote(f"sc://{databricks_config['workspace']['host']}:{cluster_config['databricks_cluster']['cluster_id']}")
            )
            
            _spark_session = spark_session_conf.getOrCreate()
            _spark_session.sparkContext.setLogLevel("WARN")
            
            print(f"✅ SparkSession conectada ao Databricks: {databricks_config['workspace']['host']}")
            print(f"   Cluster ID: {cluster_config['databricks_cluster']['cluster_id']}")
            
        except Exception as e:
            print(f"⚠️  Erro ao conectar ao Databricks: {e}")
            print("   Spark não será inicializado automaticamente.")
            print("   Use SparkSession.builder.getOrCreate() nos seus nodes quando necessário.")
