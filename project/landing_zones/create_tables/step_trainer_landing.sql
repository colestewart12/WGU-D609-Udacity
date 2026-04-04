CREATE EXTERNAL TABLE IF NOT EXISTS step_trainer_landing (
    sensorReadingTime   BIGINT,
    serialNumber        STRING,
    distanceFromObject  INT
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES ('ignore.malformed.json' = 'TRUE')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://cole-stewart-d609-udacity/step_trainer/landing/'
TBLPROPERTIES ('classification' = 'json');