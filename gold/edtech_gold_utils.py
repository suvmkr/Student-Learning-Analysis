from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def get_spark_session(app_name="MyApp"):
    spark = SparkSession.getActiveSession()
    
    if spark is not None:
        return spark
    else:
        return SparkSession.builder \
            .appName(app_name) \
            .getOrCreate()

spark = get_spark_session('edtech-gold')

silver_schema = "edtech_project.edtech_silver"
gold_schema = "edtech_project.edtech_gold"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {gold_schema}")

TABLES = {
    "engagement": f"{silver_schema}.student_course_engagement",
    "risk": f"{silver_schema}.enrollment_risk_profile",
    "sessions": f"{silver_schema}.learning_sessions",
    "discussion": f"{silver_schema}.discussion_posts_parsed",
    "enrollments": f"{silver_schema}.student_enrollments",
    "catalog": f"{silver_schema}.course_catalog",
}

def require_table(table_name: str) -> None:
    if not spark.catalog.tableExists(table_name):
        raise ValueError(f"Required table does not exist: {table_name}")

def has_col(df, col_name: str) -> bool:
    return col_name in df.columns

def col_or_null(df, col_name: str, dtype: str = "string"):
    return F.col(col_name).cast(dtype) if has_col(df, col_name) else F.lit(None).cast(dtype)

def pct_norm(c):
    """
    Normalizes a percentage/rate column to a 0-100 scale.
    If values are already 0-100, they are left unchanged.
    If values are 0-1, they are multiplied by 100.
    """
    return (
        F.when(c.isNull(), F.lit(None).cast("double"))
         .when(c <= 1, c * 100)
         .otherwise(c.cast("double"))
    )

def overwrite_delta_table(df, table_name: str, partition_cols=None):
    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    writer = df.write.format("delta").mode("overwrite")
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.saveAsTable(table_name)
    try:
        if "course_id" in df.columns:
            spark.sql(f"OPTIMIZE {table_name} ZORDER BY (course_id)")
    except Exception:
        pass