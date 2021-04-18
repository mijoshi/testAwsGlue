import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['TempDir','JOB_NAME'])

## Initialize the GlueContext and SparkContext
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
#get the name of the job via command line
job.init(args['JOB_NAME'], args)

## Read the data from Amazon S3 and have their structure in the data catalog.
datasource1 = glueContext.create_dynamic_frame.from_catalog(database = "db_demo1", table_name = "tbl_syn_source_1_csv", transformation_ctx = "datasource1")

datasource2 = glueContext.create_dynamic_frame.from_catalog(database = "db_demo1", table_name = "tbl_syn_source_2_csv", transformation_ctx = "datasource2")

## Apply transformation, join the tables
join1 = Join.apply(frame1 = datasource1, frame2 = datasource2, keys1 = "statecode", keys2 = "code", transformation_ctx = "join1")

## Write the transformed data into Amazon Redshift
datasink1 = glueContext.write_dynamic_frame.from_jdbc_conf(frame = join1, catalog_connection = "my-redshift-1", connection_options = {"dbtable": "sch_demo_1.tbl_joined", "database": "dev"}, redshift_tmp_dir = args["TempDir"], transformation_ctx = "datasink1")
job.commit()
