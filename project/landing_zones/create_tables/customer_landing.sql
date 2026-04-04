CREATE EXTERNAL TABLE IF NOT EXISTS customer_landing (
    customerName        STRING,
    email               STRING,
    phone               STRING,
    birthDay            STRING,
    serialNumber        STRING,
    registrationDate    BIGINT,
    lastUpdateDate      BIGINT,
    shareWithResearchAsOfDate   BIGINT,
    shareWithPublicAsOfDate     BIGINT,
    shareWithFriendsAsOfDate    BIGINT
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES ('ignore.malformed.json' = 'TRUE')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://cole-stewart-d609-udacity/customer/landing/'
TBLPROPERTIES ('classification' = 'json');