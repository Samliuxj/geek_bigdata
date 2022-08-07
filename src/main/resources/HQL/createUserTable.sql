CREATE TABLE `t_user`(
  `user_id` int COMMENT '用户id',
  `sex` string COMMENT '性别',
  `age` int COMMENT '年龄',
  `occupation` string COMMENT '职业',
  `zip_code` bigint COMMENT '邮编')
ROW FORMAT SERDE
  'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe'
WITH SERDEPROPERTIES (
  'field.delim'='::')
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  '/samliu/homework2/data/user'  -- hdfs file path
;