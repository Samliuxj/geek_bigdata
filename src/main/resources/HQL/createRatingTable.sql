CREATE TABLE `t_rating`(
  `user_id` int COMMENT '用户id',
  `movie_id` bigint COMMENT '电影id',
  `rate` int COMMENT '评分',
  `times` string COMMENT '评分时间')
ROW FORMAT SERDE
  'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe'
WITH SERDEPROPERTIES (
  'field.delim'='::')
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  '/samliu/homework2/data/rating' -- hdfs file path
;