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

customer_landing = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_landing"
)

customer_landing.toDF().createOrReplaceTempView("customer_landing")

customer_trusted_df = spark.sql("""
    SELECT * FROM customer_landing
    WHERE sharewithresearchasofdate IS NOT NULL
    AND sharewithresearchasofdate != 0
""")

customer_trusted_dynamic = DynamicFrame.fromDF(
    customer_trusted_df, glueContext, "customer_trusted"
)

sink = glueContext.getSink(
    connection_type="s3",
    path="s3://cole-stewart-d609-udacity/customer/trusted/",
    enableUpdateCatalog=True,
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[]
)
sink.setCatalogInfo(
    catalogDatabase="stedi",
    catalogTableName="customer_trusted"
)
sink.setFormat("json")
sink.writeFrame(customer_trusted_dynamic)

job.commit()