import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read step trainer landing from Glue catalog
step_trainer_landing = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="step_trainer_landing"
)

# Read customers curated from Glue catalog
customers_curated = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customers_curated"
)

# Register as temp views
step_trainer_landing.toDF().createOrReplaceTempView("step_trainer_landing")
customers_curated.toDF().createOrReplaceTempView("customers_curated")

# Inner join on serial number, keep only step trainer columns
step_trainer_trusted_df = spark.sql("""
    SELECT DISTINCT
        s.sensorreadingtime,
        s.serialnumber,
        s.distancefromobject
    FROM step_trainer_landing s
    INNER JOIN customers_curated c
        ON s.serialnumber = c.serialnumber
""")

# Convert to DynamicFrame
step_trainer_trusted_dynamic = DynamicFrame.fromDF(
    step_trainer_trusted_df, glueContext, "step_trainer_trusted"
)

# Write to S3 and create/update Glue catalog table
sink = glueContext.getSink(
    connection_type="s3",
    path="s3://cole-stewart-d609-udacity/step_trainer/trusted/",
    enableUpdateCatalog=True,
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[]
)
sink.setCatalogInfo(
    catalogDatabase="stedi",
    catalogTableName="step_trainer_trusted"
)
sink.setFormat("json")
sink.writeFrame(step_trainer_trusted_dynamic)

job.commit()