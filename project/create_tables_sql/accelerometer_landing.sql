CREATE EXTERNAL TABLE IF NOT EXISTS accelerometer_landing (
    user        STRING,
    timestamp   BIGINT,
    x           FLOAT,
    y           FLOAT,
    z           FLOAT
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES ('ignore.malformed.json' = 'TRUE')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://cole-stewart-d609-udacity/accelerometer/landing/'
TBLPROPERTIES ('classification' = 'json');