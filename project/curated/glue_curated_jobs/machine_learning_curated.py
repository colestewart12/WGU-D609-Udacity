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

step_trainer_trusted = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="step_trainer_trusted"
)

accelerometer_trusted = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_trusted"
)

step_trainer_trusted.toDF().createOrReplaceTempView("step_trainer_trusted")
accelerometer_trusted.toDF().createOrReplaceTempView("accelerometer_trusted")

machine_learning_curated_df = spark.sql("""
    SELECT DISTINCT
        s.sensorreadingtime,
        s.serialnumber,
        s.distancefromobject,
        a.user,
        a.timestamp,
        a.x,
        a.y,
        a.z
    FROM step_trainer_trusted s
    INNER JOIN accelerometer_trusted a
        ON s.sensorreadingtime = a.timestamp
""")

machine_learning_curated_dynamic = DynamicFrame.fromDF(
    machine_learning_curated_df, glueContext, "machine_learning_curated"
)

sink = glueContext.getSink(
    connection_type="s3",
    path="s3://cole-stewart-d609-udacity/machine_learning/curated/",
    enableUpdateCatalog=True,
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[]
)
sink.setCatalogInfo(
    catalogDatabase="stedi",
    catalogTableName="machine_learning_curated"
)
sink.setFormat("json")
sink.writeFrame(machine_learning_curated_dynamic)

job.commit()