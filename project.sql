-- SQL Dialect: SQLite
-- Стиль і синтаксис
-- Використовуються ключові слова ROUND, AVG, COUNT, CASE WHEN, WITH (CTE), GROUP BY, ORDER BY, HAVING, row_number.







-- #1 Аналіз структури категорій відео українських трендів( кількість відео, середня кількість переглядів, медіанна кількість переглядів, коефіцієнт залученості)
with ranked_videos as(
	select 
		video_category_id
		, video_view_count
		, video_like_count
		, video_comment_count
		, row_number() over(partition by video_category_id order by video_view_count) as rn
		, count(*) over(partition by video_category_id) as total_count
from youtube_trending_videos_global
where video_trending_country = 'Ukraine'
	and video_category_id is not null
					)
					
select 
	video_category_id as video_category_name
	, count(*) as num_of_videos
	, round(avg(video_view_count),0) as avg_views
	, avg(case 
			when rn between total_count/2.0 and total_count/ 2.0 + 1 then video_view_count 
	end) as median_views
	, round(avg((video_like_count + video_comment_count) * 1.0 / nullif(video_view_count,0)), 4) as avg_engagement_rate
from ranked_videos
group by video_category_id
order by num_of_videos desc
;


-- #2 Створюємо buckets і по них знаходимо к-сть відео, серендю к-сть переглядів, медіанну к-сть переглядів, середню к-сть лайків та коментарів.
with buckets as(
			select 
				*
				, case 	
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
			from youtube_trending_videos_global
			where video_trending_country = 'Ukraine'
				and video_category_id is not null
)

, ranked_table as(
				select *
					, row_number() over(partition by time_category order by video_view_count) as rn
					, count(*) over(partition by time_category) as total_count
				from buckets 
)

select 
	time_category
	, count(*) as num_of_videos
	, round(avg(video_view_count),0) as avg_views
	, avg(case 
			when rn between total_count/2.0 and total_count/ 2.0 + 1 then video_view_count 
	end) as median_views
	, round(avg(video_like_count),0) as avg_likes
	, round(avg(video_comment_count),0) as avg_comments
from ranked_table
group by time_category
order by num_of_videos desc
;




-- #3 По кожному дню знаходимо час публікації, к-сть відео та середню к-сть переглядів

select 
	 case cast(strftime('%w',video_published_at) as integer)
		when 1 then '1. Monday'
		when 2 then '2. Tuesday'
		when 3 then '3. Wednesday'
		when 4 then '4. Thursday'
		when 5 then '5. Friday'
		when 6 then '5. Saturday'
		else 'Sunday'
	end as day_name
	, strftime('%H:00:00', datetime(video_published_at, '+2 hours')) as kyiv_publish_hours
	, count(*) as num_of_videos
	, round(avg(video_view_count),0) as avg_views
from youtube_trending_videos_global ytvg 
where video_trending_country = 'Ukraine'
	and video_category_id is not null
group by day_name, kyiv_publish_hours
order by day_name, num_of_videos desc

;




-- #4. Ділимо відео на 2 категорії :
-- 1) Shorts ( до 1 хв )
-- 2) Long ( все інше )
-- Далі рахуємо кількість днів до набуття статусу трендового.

select 
    case 
        when video_duration_sec < 60 then 'Shorts'
        else 'Long-form'
    end as video_type
    , (julianday(video_trending__date) - julianday(date(video_published_at))) as days_to_trending
    , video_view_count
    , video_category_id
from youtube_trending_videos_global
where video_trending_country = 'Ukraine'
    and video_category_id is not null
;
    

-- #5. Формуємо перший запит. Для кожного каналу знаходимо максимальну кількість підписників, рівень залученості та середню к-сть переглядів.
-- Далі через case формуємо 3 групи ( Large, medium, small ) і для них рахуємо к-сть каналів, середню к-сть перегялдів та середній рівень залученості.
-- В даному кейсі ми нівелюємо вплив каналів з великою к-стю відео на загальний ER за допомогою розрахунку спочатку ER для кожного каналу, а тільки потім вже середній по групах.



with channel_stats as (
    select
        channel_id
        , max(channel_subscriber_count) as subscriber_count -- для більш точного рохрахунку беремо максимальну кількість підписників через те, що зазвичай підписники ростуть з часом.
        , round(sum(video_like_count + video_comment_count) * 1.0 / nullif(sum(video_view_count),0),4) as channel_eng_rate
        , round(avg(video_view_count),0) as channel_avg_views
    from youtube_trending_videos_global
    where video_trending_country = 'Ukraine'
      and video_category_id is not null
      and channel_subscriber_count is not null
    group by channel_id
)

select 
    case 
        when subscriber_count < 100000 then '1. Small'
        when subscriber_count >= 100000 and subscriber_count < 1000000 then '2. Medium'
        else '3. Large'
    end as channel_size
    , count(*) as number_of_channels
    , round(avg(channel_avg_views),0) as avg_views
    , round(avg(channel_eng_rate),4) as avg_eng_rate
from channel_stats
group by channel_size
order by channel_size
;






