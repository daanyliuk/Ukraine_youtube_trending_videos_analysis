-- SQL Dialect: SQLite
-- Стиль і синтаксис
-- Використовуються ключові слова ROUND, AVG, COUNT, CASE WHEN, WITH (CTE), GROUP BY, ORDER BY, HAVING.







-- #1 Top categories by the number of trending videos & their average views, likes, and comments.
select 
	video_category_id 
	, count(video_id) as trending_videos
	, round(avg(video_view_count),0) as avg_view
	, round(avg(video_like_count),0) as avg_likes
	, round(avg(video_comment_count),0) as avg_comments
from youtube_trending_videos_global ytvg 
where video_category_id is not null
group by video_category_id
order by trending_videos desc
;

-- #1.1 Correlation between views and likes.
SELECT
  round((AVG(video_view_count * video_like_count) - AVG(video_view_count) * AVG(video_like_count)) /
  (SQRT(AVG(video_view_count * video_view_count) - AVG(video_view_count) * AVG(video_view_count)) *
   SQRT(AVG(video_like_count * video_like_count) - AVG(video_like_count) * AVG(video_like_count))),1) AS correlation
from youtube_trending_videos_global ytvg
where video_category_id is not null 
;

-- #2 Performance by Duration, AVG Views, Likes, and Comments per category.
select 
	 case 	
		when video_duration_sec < 60 then 'less than 1 min'
		when video_duration_sec between 60 and 300 then '1-5 min'
		when video_duration_sec between 301 and 600 then '5-10 min'
		when video_duration_sec between 601 and 1800 then '10-30 min'
		when video_duration_sec between 1801 and 3600 then '30-60 min'
		when video_duration_sec between 3601 and 7200 then '1-2 hours'
		when video_duration_sec between 7201 and 10800 then '2-3 hours'
		when video_duration_sec between 10801 and 21600 then '3-6 hours'
		when video_duration_sec > 21600  then '6 hours +'
	end as time_category
	, count(*) as videos_count
	, round(avg(video_view_count),0) as avg_views
	, round(avg(video_like_count),0) as avg_likes
	, round(avg(video_comment_count),0) as avg_comments
from youtube_trending_videos_global ytvg 
group by time_category
order by 2 desc
;

-- #3 Minimum subscribers, views, and videos to get trending status
		select 
			channel_country
			, min(channel_video_count) min_video_count
			, min(channel_subscriber_count) min_subscriber_count
			, min(channel_view_count) min_view_count
		from youtube_project.youtube_trending_videos_global_copy ytvgc 
		where channel_country !=''
		group by channel_country 
	 	having min(channel_view_count) != 0
		order by min_view_count
;

-- #4. The breakdown of YouTube video activity across different categories and days of the week
with cte1 as(
		select *
			, case 
			strftime('%w',video_published_at)
			when '0' then 'Sunday'
			when '1' then 'Monday'
			when '2' then 'Tuesday'
			when '3' then 'Wednesday'
			when '4' then 'Thursday'
			when '5' then 'Friday'
			when '6' then 'Saturday'
			end as day_name
		from youtube_trending_videos_global ytvg
		where video_category_id is not null
)

select 
	video_category_id
	, day_name
	, count(video_id) as videos_count
	, round(avg(video_view_count),0) as avg_views
	, round(avg(video_like_count),0) as avg_likes
	, round(avg(video_comment_count),0) as avg_comments
from cte1 
group by video_category_id, day_name
order by video_category_id, avg_views desc
;

-- #4.1 AVG time(in days) to get trending status
with cte1 as(
			select * 
				, round(julianday(video_trending__date) - julianday(video_published_at),0) day_diff
			from youtube_trending_videos_global ytvg 
)

select 
	video_category_id
	, round(avg(day_diff),0) as avg_day_time_to_trend
from cte1
group by video_category_id
having video_category_id is not null
order by avg_day_time_to_trend 
;

-- #5. Trends in video engagement metrics across different countries.
select 
    channel_country
    , COUNT(*) as trending_video_count
    , ROUND(AVG(video_view_count), 0) as avg_view_count
    , ROUND(AVG(channel_subscriber_count), 0) as avg_channel_subsc_count
    , ROUND(AVG(video_like_count), 0) as avg_like_count
    , ROUND(AVG(video_like_count * 1.0 / NULLIF(video_view_count, 0)), 2) as avg_like_view_ratio
from youtube_trending_videos_global ytvg
where channel_country IS NOT NULL
group by channel_country
having trending_video_count > 100
	and channel_country != ''
order by avg_like_view_ratio desc, trending_video_count desc
limit 20
; 


