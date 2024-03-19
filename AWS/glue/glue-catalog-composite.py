# Import section
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SQLContext

# PySpark Config Section
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

#ETL Job Parameters Section
# Source databases prefix
database_prefix = "teddy_retailers"

# Target S3 Bucket
target_s3_bucket = "s3://test-glue-bucket-td"

#Target catalog database
catalog_database_name = "test-database-glue"

def get_sources_to_extract(db_prefix):
    dynamic_frame_catalog = glueContext.create_dynamic_frame.from_options(
        connection_type="teradata",
        connection_options={
            "dbtable": "DBC.TablesV",
            "connectionName": "Teradata connection default",
            "query": f"(SELECT DataBaseName, TableName, TableKind FROM DBC.TablesV WHERE DataBaseName Like '{db_prefix}%' AND (TableKind = 'T' OR TableKind = 'O'))",
        },
        transformation_ctx= "sources_read",
    )
    
    # Convert DynamicFrame to DataFrame for easier manipulation
    df = dynamic_frame_catalog.toDF()

    # Collect the DataFrame to a list of Row objects
    return df.collect()

# Job function abstraction
def process_table(table_name, transformation_ctx_prefix, catalog_database, catalog_table_name):
    dynamic_frame = glueContext.create_dynamic_frame.from_options(
        connection_type="teradata",
        connection_options={
            "dbtable": table_name,
            "connectionName": "Teradata connection default",
            "query": f"SELECT TOP 1 * FROM {table_name}", # This line can be modified to ingest the full table or rows that fulfill an specific condition
        },
        transformation_ctx=transformation_ctx_prefix + "_read",
    )

    s3_sink = glueContext.getSink(
        path=target_s3_bucket,
        connection_type="s3",
        updateBehavior="UPDATE_IN_DATABASE",
        partitionKeys=[],
        compression="snappy",
        enableUpdateCatalog=True,
        transformation_ctx=transformation_ctx_prefix + "_s3",
    )
    # Dynamically set catalog table name based on function parameter
    s3_sink.setCatalogInfo(
        catalogDatabase=catalog_database, catalogTableName=catalog_table_name
    )
    s3_sink.setFormat("csv")
    s3_sink.writeFrame(dynamic_frame)


# Job execution section

sources_list = get_sources_to_extract(database_prefix)

for row in sources_list:
    table_name = row.TableName  # Adjust attribute access based on actual column names
    database_name = row.DataBaseName  # Adjust attribute access based on actual column names
    full_table_name = f"{database_name}.{table_name}"
    transformation_ctx_prefix = f"{database_name}_{table_name}"
    catalog_table_name = f"{database_name}_{table_name}_catalog"
    catalog_database = catalog_database_name

    # Call your process_table function for each table
    process_table(full_table_name, transformation_ctx_prefix, catalog_database, catalog_table_name)


job.commit()