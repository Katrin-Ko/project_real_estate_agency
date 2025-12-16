/* Проект : анализ данных для агентства недвижимости
 * Часть 2. ad hoc задачи
 * 
 * Автор:Кононова Екатерина Андреевна
*/

-- Задача 1: Время активности объявлений
-- Определим аномальные значения (выбросы) по значению перцентилей:
WITH limits AS (
    SELECT
        PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY total_area) AS total_area_limit,
        PERCENTILE_DISC(0.99) WITHIN GROUP (ORDER BY rooms) AS rooms_limit,
        PERCENTILE_DISC(0.99) WITHIN GROUP (ORDER BY balcony) AS balcony_limit,
        PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY ceiling_height) AS ceiling_height_limit_h,
        PERCENTILE_CONT(0.01) WITHIN GROUP (ORDER BY ceiling_height) AS ceiling_height_limit_l
    FROM real_estate.flats
),
-- Найдём id объявлений, которые не содержат выбросы, также оставим пропущенные данные:
filtered_id AS(
    SELECT id
    FROM real_estate.flats
    WHERE
        total_area < (SELECT total_area_limit FROM limits)
        AND (rooms < (SELECT rooms_limit FROM limits) OR rooms IS NULL)
        AND (balcony < (SELECT balcony_limit FROM limits) OR balcony IS NULL)
        AND ((ceiling_height < (SELECT ceiling_height_limit_h FROM limits)
            AND ceiling_height > (SELECT ceiling_height_limit_l FROM limits)) OR ceiling_height IS NULL)
    ),
-- Используем id объявлений (СТЕ filtered_id), которые не содержат выбросы при анализе данных
-- Собираем данные для исследования
research_data AS (
SELECT a.id,
CASE 
	WHEN c.city = 'Санкт-Петербург' THEN 'Санкт-Петербург' ELSE 'ЛенОбл' END AS region,
CASE 
	WHEN a.days_exposition BETWEEN 1 AND 30 THEN '1-30'
	WHEN a.days_exposition BETWEEN 31 AND 90 THEN '31-90'
	WHEN a.days_exposition BETWEEN 91 AND 180 THEN '91-180'
	WHEN a.days_exposition > 180 THEN '181+'
	ELSE 'non category'
	END AS activity_days,
f.total_area,
f.rooms,
f.balcony,
f.floor,
f.ceiling_height,
a.last_price / NULLIF(f.total_area, 0) AS price_m2
FROM real_estate.flats AS f
	 JOIN real_estate.advertisement AS A ON f.id=a.id 
	 JOIN real_estate.city AS c ON c.city_id=f.city_id
	 JOIN real_estate.type AS t ON t.type_id=f.type_id
WHERE a.id IN (SELECT id FROM filtered_id) 
	AND EXTRACT(YEAR FROM a.first_day_exposition) BETWEEN 2015 AND 2018 
	AND t.TYPE = 'город'),
-- Основной запрос
total AS (
SELECT region,
activity_days,
COUNT (id) AS count_id,
ROUND (COUNT(*) * 100.0 /SUM(COUNT(*)) OVER (PARTITION BY region)::NUMERIC,2) AS pct_in_region,
ROUND (AVG(price_m2)::NUMERIC,2) AS avg_pricem2,
ROUND (AVG(total_area)::NUMERIC,2) AS avg_total_area,
PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY rooms)AS mediana_rooms,
PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY balcony)AS mediana_balcony,
PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY floor)AS mediana_floor,
ROUND (AVG(ceiling_height)::NUMERIC,2) AS avg_ceiling_height
FROM research_data
GROUP BY region, activity_days)
--Основной запрос
SELECT region AS "Регион",
activity_days AS "Дни активности объявлений",
count_id AS "Количество объявлений",
pct_in_region AS "% объявлений от их общего числа",
avg_pricem2 AS "Средняя стоимость за кв.метр",
avg_total_area AS "Средняя площадь квартиры",
mediana_rooms AS "Медиана комнат",
mediana_balcony AS "Медиана балконов",
mediana_floor AS "Медиана этажей",
avg_ceiling_height AS "Среднее высота потолка"
FROM total;
       

-- Задача 2: Сезонность объявлений
-- Определим аномальные значения (выбросы) по значению перцентилей:
WITH limits AS (
    SELECT
        PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY total_area) AS total_area_limit,
        PERCENTILE_DISC(0.99) WITHIN GROUP (ORDER BY rooms) AS rooms_limit,
        PERCENTILE_DISC(0.99) WITHIN GROUP (ORDER BY balcony) AS balcony_limit,
        PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY ceiling_height) AS ceiling_height_limit_h,
        PERCENTILE_CONT(0.01) WITHIN GROUP (ORDER BY ceiling_height) AS ceiling_height_limit_l
    FROM real_estate.flats
),
-- Найдём id объявлений, которые не содержат выбросы, также оставим пропущенные данные:
filtered_id AS(
    SELECT id
    FROM real_estate.flats
    WHERE
        total_area < (SELECT total_area_limit FROM limits)
        AND (rooms < (SELECT rooms_limit FROM limits) OR rooms IS NULL)
        AND (balcony < (SELECT balcony_limit FROM limits) OR balcony IS NULL)
        AND ((ceiling_height < (SELECT ceiling_height_limit_h FROM limits)
            AND ceiling_height > (SELECT ceiling_height_limit_l FROM limits)) OR ceiling_height IS NULL)
    ),
-- Используем id объявлений (СТЕ filtered_id), которые не содержат выбросы при анализе данных
-- Для каждого объявления за 2015–2018 годы найдем месяц выставления объявления на продажу и месяц его снятия
stat_mounf AS (
SELECT a.id,
EXTRACT (YEAR FROM a.first_day_exposition ) AS YEAR,
EXTRACT (MONTH FROM a.first_day_exposition ) AS published_month,
CASE WHEN a.days_exposition IS NOT NULL THEN EXTRACT(MONTH FROM a.first_day_exposition + INTERVAL '1 day' * a.days_exposition) END AS removed_month,
a.last_price / f.total_area AS price_m2,
f.total_area 
FROM real_estate.advertisement AS a
	 JOIN real_estate.flats AS f ON a.id=f.id
	 JOIN real_estate.city AS c ON c.city_id=f.city_id    
	 JOIN real_estate.type AS t ON t.type_id=f.type_id    
WHERE a.id IN (SELECT id FROM filtered_id) 
	AND EXTRACT(YEAR FROM a.first_day_exposition) BETWEEN 2015 AND 2018 
	AND t.type  = 'город' 
	AND f.total_area >0 
	AND a.first_day_exposition IS NOT NULL ),
-- При агрегации данных посчитаем количество объявлений для месяцев публикаций объявлений, среднюю стоимость квадратного метра и среднюю площадь недвижимости
research_published_month AS (
SELECT published_month AS MONTH,
count(*) AS count_ads,
ROUND (AVG(price_m2)::NUMERIC,2) AS avg_price_m2,
ROUND (AVG(total_area)::NUMERIC,2) AS avg_total_area
FROM stat_mounf
GROUP BY published_month),
--При агрегации данных посчитаем количество объявлений для месяцев снятия с публикаций объявлений, среднюю стоимость квадратного метра и среднюю площадь недвижимости
research_removed_month AS (
SELECT removed_month AS MONTH,
count(id) AS count_ads_removed,
ROUND (AVG(price_m2)::NUMERIC,2) AS avg_price_m2_removed,
ROUND (AVG(total_area)::NUMERIC,2) AS avg_total_area_removed
FROM stat_mounf
WHERE removed_month IS NOT NULL
GROUP BY removed_month),
-- Основной запрос
total AS (
SELECT coalesce(p.MONTH,r.MONTH) AS MONTH,
p.count_ads,
r.count_ads_removed,
COALESCE(r.count_ads_removed, 0) - COALESCE(p.count_ads, 0) AS diff_ads_vs_publ,
p.avg_price_m2,
r.avg_price_m2_removed,
p.avg_total_area,
r.avg_total_area_removed
FROM research_published_month AS p
FULL JOIN research_removed_month AS r ON p.month = r.month
ORDER BY MONTH),
-- Ранжирование по месяцам
ranked AS (
SELECT *,
RANK() OVER (ORDER BY count_ads DESC) AS published_rank,
RANK() OVER (ORDER BY count_ads_removed DESC) AS removed_rank
FROM total)
--Основной запрос
SELECT MONTH AS "Месяц",
count_ads AS "Кол-во опубликованных объявлений",
count_ads_removed AS "Кол-во снятых с продажи объявлений",
diff_ads_vs_publ AS "Разница снятые/опубликованные объявления",
avg_price_m2 AS "Средняя цена за кв.метр опубликованных объявлений",
avg_price_m2_removed AS "Средняя цена за кв.метр снятых с продажи объявлений",
avg_total_area AS "Средняя площадь опубликованных объявлений",
avg_total_area_removed AS "Средняя площадь снятых с продажи объявлений",
published_rank AS "Ранг по публикациям",
removed_rank AS "Ранг по снятию"
FROM ranked
ORDER BY MONTH;