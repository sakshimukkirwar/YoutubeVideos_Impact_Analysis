CREATE
OR REPLACE FILE FORMAT my_csv_format TYPE = 'CSV' FIELD_DELIMITER = ',' FIELD_OPTIONALLY_ENCLOSED_BY = '"' ESCAPE_UNENCLOSED_FIELD = NONE;
COPY INTO YOUTUBE_REGIONS_DATASET.ALLREGIONS_VIDEOS_CSV.YOUTUBE_VIDEOS (
    video_id,
    trending_date,
    title,
    channel_title,
    category_id,
    publish_time,
    tags,
    views,
    likes,
    dislikes,
    comment_count,
    thumbnail_link,
    comments_disabled,
    ratings_disabled,
    video_error_or_removed,
    description
)
FROM
    @YOUTUBE_REGIONS_DATASET.ALLREGIONS_VIDEOS_CSV.ALLREGIONS_VIDEOS_CSV/USvideos.csv FILE_FORMAT = my_csv_format ON_ERROR = 'CONTINUE';
UPDATE
    YOUTUBE_REGIONS_DATASET.ALLREGIONS_VIDEOS_CSV.YOUTUBE_VIDEOS
SET
    region = 'USA'
WHERE
    region IS NULL
    AND video_id IN (
        SELECT
            video_id
        FROM
            youtube_videos
        WHERE
            region IS NULL
    );
select
    *
from
    youtube_videos
where
    region = 'USA'