-- 题目一
SELECT u.age as age, avg(r.rate) as avg_rate
FROM t_movie m
JOIN t_rating r ON m.movie_id = r.movie_id
JOIN t_user u ON u.user_id = r.user_id
WHERE m.movie_id = 2116
GROUP BY u.age
ORDER BY u.age;

-- 题目二
select m.movie_name as name, avg(r.rate) as avg_rate, count(m.movie_name) as cnt
from t_rating r
join t_user u on r.user_id=u.user_id
join t_movie m on r.movie_id=m.movie_id
where u.sex="M"
group by m.movie_name
having cnt > 50
order by avg_rate desc
limit 10;
--找出影评次数最多的女士所给出最高分的 10 部电影的平均影评分，
--展示电影名和平均影评分（可使用多行 SQL）。
SELECT t_movie.movie_id
FROM t_movie
JOIN t_rating ON t_movie.movie_id = t_rating.movie_id
where t_rating.user_id in (select t_user.user_id
from t_user
join t_rating on t_user.user_id=t_rating.user_id
where t_user.sex = "F"
group by t_user.user_id
order by count(1) desc limit 1)
order by t_rating.rate desc limit 10

select t_movie.movie_name, avg(t_rating.rate) as avg_rate
from t_rating join t_movie on
t_rating.movie_id=t_movie.movie_id
 where t_movie.movie_id in
 (#above sql)
group by t_movie.movie_name;