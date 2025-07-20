import logging
from databricks.connect import DatabricksSession

logger = logging.getLogger(__name__)


def get_spark_session():
    """Return a Spark session.
    If a session named 'spark' already exists in globals() or locals(), return it.
    Otherwise, attempt to create one using the standard builder, falling back to serverless if needed.
    """
    if "spark" in globals():
        logger.debug("Spark session found in globals; using existing session.")
        return globals()["spark"]

    if "spark" in locals():
        logger.debug("Spark session found in locals; using existing session.")
        return locals()["spark"]

    try:
        session = DatabricksSession.builder.getOrCreate()
        logger.debug("Created new standard Databricks Spark session.")
        return session
    except Exception as ex:
        logger.debug(
            "Failed to create standard session due to: %s. Falling back to serverless Spark session.",
            ex,
        )
        session = DatabricksSession.builder.serverless().getOrCreate()
        logger.debug("Using Databricks serverless Spark session.")
        return session
