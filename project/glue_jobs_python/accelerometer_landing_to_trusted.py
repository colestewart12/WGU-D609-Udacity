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

accelerometer_landing = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={
        "paths": ["s3://cole-stewart-d609-udacity/accelerometer/landing/"],
        "recurse": True
    },
    format="json"
)

customer_trusted = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted"
)

accelerometer_landing.toDF().createOrReplaceTempView("accelerometer_landing")
customer_trusted.toDF().createOrReplaceTempView("customer_trusted")

accelerometer_trusted_df = spark.sql("""
    SELECT DISTINCT
        a.user,
        a.timestamp,
        a.x,
        a.y,
        a.z
    FROM accelerometer_landing a
    INNER JOIN customer_trusted c
        ON a.user = c.email
""")

accelerometer_trusted_dynamic = DynamicFrame.fromDF(
    accelerometer_trusted_df, glueContext, "accelerometer_trusted"
)

sink = glueContext.getSink(
    connection_type="s3",
    path="s3://cole-stewart-d609-udacity/accelerometer/trusted/",
    enableUpdateCatalog=True,
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[]
)
sink.setCatalogInfo(
    catalogDatabase="stedi",
    catalogTableName="accelerometer_trusted"
)
sink.setFormat("json")
sink.writeFrame(accelerometer_trusted_dynamic)

job.commit()