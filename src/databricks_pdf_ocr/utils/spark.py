from pyspark.sql import SparkSession


def get_spark():
    """
    Get a Spark session that works both locally with Databricks Connect
    and in Databricks runtime environments.

    Returns:
        SparkSession: Active Spark session
    """
    try:
        from databricks.connect import DatabricksSession

        print("Attempting to create serverless Databricks session")
        return DatabricksSession.builder.serverless().getOrCreate()
    except ImportError:
        print("Databricks Connect not available, trying local Spark session")
        return SparkSession.builder.getOrCreate()
    except Exception as ex:
        print(f"Error creating serverless Databricks session: {str(ex)}")
        print("Falling back to local Spark session")
        return SparkSession.builder.getOrCreate()


def get_spark_session(app_name: str = "DatabricksPDFOCR") -> SparkSession:
    """
    Get a Spark session with proper configuration for PDF OCR processing.
    
    Args:
        app_name: Name for the Spark application
        
    Returns:
        SparkSession: Configured Spark session
    """
    try:
        from databricks.connect import DatabricksSession
        
        print(f"Creating Databricks session for app: {app_name}")
        return DatabricksSession.builder.serverless().getOrCreate()
    except ImportError:
        print(f"Databricks Connect not available, creating local Spark session for app: {app_name}")
        return (
            SparkSession.builder
            .appName(app_name)
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .getOrCreate()
        )
    except Exception as ex:
        print(f"Error creating Databricks session: {str(ex)}")
        print(f"Falling back to local Spark session for app: {app_name}")
        return (
            SparkSession.builder
            .appName(app_name)
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .getOrCreate()
        )


def create_volume(spark: SparkSession, catalog: str, schema: str, volume_name: str) -> None:
    """
    Create a managed volume if it doesn't exist.
    
    Args:
        spark: Spark session
        catalog: Catalog name
        schema: Schema name
        volume_name: Volume name
    """
    try:
        # Check if volume exists
        existing_volumes = spark.sql(f"SHOW VOLUMES IN {catalog}.{schema}").collect()
        volume_exists = any(row.volume_name == volume_name for row in existing_volumes)
        
        if not volume_exists:
            print(f"Creating volume {catalog}.{schema}.{volume_name}")
            spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{volume_name}")
            print(f"Volume {catalog}.{schema}.{volume_name} created successfully")
        else:
            print(f"Volume {catalog}.{schema}.{volume_name} already exists")
            
    except Exception as e:
        print(f"Error creating volume: {str(e)}")
        raise
