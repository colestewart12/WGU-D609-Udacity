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

customer_trusted = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted"
)

accelerometer_trusted = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_trusted"
)

customer_trusted.toDF().createOrReplaceTempView("customer_trusted")
accelerometer_trusted.toDF().createOrReplaceTempView("accelerometer_trusted")

customers_curated_df = spark.sql("""
    SELECT DISTINCT
        c.customername,
        c.email,
        c.phone,
        c.birthday,
        c.serialnumber,
        c.registrationdate,
        c.lastupdatedate,
        c.sharewithresearchasofdate,
        c.sharewithpublicasofdate,
        c.sharewithfriendsasofdate
    FROM customer_trusted c
    INNER JOIN accelerometer_trusted a
        ON c.email = a.user
""")

customers_curated_dynamic = DynamicFrame.fromDF(
    customers_curated_df, glueContext, "customers_curated"
)

sink = glueContext.getSink(
    connection_type="s3",
    path="s3://cole-stewart-d609-udacity/customer/curated/",
    enableUpdateCatalog=True,
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[]
)
sink.setCatalogInfo(
    catalogDatabase="stedi",
    catalogTableName="customers_curated"
)
sink.setFormat("json")
sink.writeFrame(customers_curated_dynamic)

job.commit()