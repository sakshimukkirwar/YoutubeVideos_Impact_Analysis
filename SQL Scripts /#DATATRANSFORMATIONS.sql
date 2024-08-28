#DATATRANSFORMATIONS

#deleting duplicates
CREATE TABLE youtube_videos_clean AS
SELECT DISTINCT * FROM youtube_videos;

#added_publish_year_and_time
ALTER TABLE youtube_videos_clean ADD COLUMN publish_year INTEGER, publish_month INTEGER, publish_day INTEGER;

UPDATE youtube_videos_clean
SET publish_year = YEAR(publish_time),
    publish_month = MONTH(publish_time),
    publish_day = DAY(publish_time);

ALTER TABLE youtube_videos_clean
ALTER COLUMN category_id SET DATA TYPE NUMERIC USING category_id::NUMERIC;

#merging_category_id_and_creating_new_table 

CREATE TABLE YTvideos_categories AS
SELECT a.*, b.category_name
FROM youtube_videos_clean a
LEFT JOIN video_categories b
ON TO_NUMBER(a.category_id) = b.category_id AND a.region = b.region;







