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
