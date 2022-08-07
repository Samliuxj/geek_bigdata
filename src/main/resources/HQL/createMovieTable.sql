CREATE TABLE `t_movie`(
  `movie_id` bigint COMMENT '电影id',
  `movie_name` string COMMENT '电影名字',
  `movie_type` string COMMENT '电影类型')
ROW FORMAT SERDE
  'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe'
WITH SERDEPROPERTIES (
  'field.delim'='::') -- delimiter ::
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  '/samliu/homework2/data/movie' -- hdfs file path
;