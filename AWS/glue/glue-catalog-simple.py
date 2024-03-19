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
# Source database
database_name = "teddy_retailers_inventory"

# Source tables
table_names = ["source_catalog","source_stock"]

# Target S3 Bucket
target_s3_bucket = "s3://test-glue-bucket-td"

#Target catalog database 
catalog_database_name = "test-glue-database" 


# Job function abstraction
def process_table(table_name, transformation_ctx_prefix, catalog_database, catalog_table_name):
    dynamic_frame = glueContext.create_dynamic_frame.from_options(
        connection_type="teradata",
        connection_options={
            "dbtable": table_name,
            "connectionName": "Teradata connection default",
            "query": f"SELECT TOP 1 * FROM {table_name}",
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
for table_name in table_names:
    full_table_name = f"{database_name}.{table_name}"  
    transformation_ctx_prefix = f"{database_name}_{table_name}"  
    catalog_table_name = f"{table_name}_catalog"  
    # Call your process_table function for each table
    process_table(full_table_name, transformation_ctx_prefix, catalog_database_name, catalog_table_name)

job.commit()

