import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1678097391347 = glueContext.create_dynamic_frame.from_catalog(
    database="amal-projects-de-youtube-cleansed-glue-catalog-database",
    table_name="cleansed_statistics_reference_data",
    transformation_ctx="AWSGlueDataCatalog_node1678097391347",
)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1678097328676 = glueContext.create_dynamic_frame.from_catalog(
    database="amal-projects-de-youtube-cleansed-glue-catalog-database",
    table_name="cleansed_statistics",
    transformation_ctx="AWSGlueDataCatalog_node1678097328676",
)

# Script generated for node Join
Join_node1678101198020 = Join.apply(
    frame1=AWSGlueDataCatalog_node1678097328676,
    frame2=AWSGlueDataCatalog_node1678097391347,
    keys1=["category_id"],
    keys2=["id"],
    transformation_ctx="Join_node1678101198020",
)

# Script generated for node Amazon S3
AmazonS3_node1678101305304 = glueContext.getSink(
    path="s3://amal-projects-de-youtube-analytics",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=["region", "category_id"],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1678101305304",
)
AmazonS3_node1678101305304.setCatalogInfo(
    catalogDatabase="amal-projects-de-youtube-analytics-glue-catalog-database",
    catalogTableName="joined_for_analytics",
)
AmazonS3_node1678101305304.setFormat("glueparquet")
AmazonS3_node1678101305304.writeFrame(Join_node1678101198020)
job.commit()
