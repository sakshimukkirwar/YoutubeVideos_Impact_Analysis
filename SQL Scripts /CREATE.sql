CREATE
OR REPLACE FILE FORMAT my_json_format TYPE = JSON;
CREATE
OR REPLACE TABLE json_staging (json_data VARIANT);
COPY INTO json_staging(json_data)
FROM
    @YOUTUBE_REGIONS_DATASET.ALLREGIONS_VIDEOS_CSV.CATEGORY_ID_JSON/DE_category_id.json FILE_FORMAT = (TYPE = 'JSON');
select
    *
from
    json_staging
INSERT INTO
    YOUTUBE_REGIONS_DATASET.ALLREGIONS_VIDEOS_CSV.VIDEO_CATEGORIES (category_id, category_name, region)
SELECT
    value:id::INTEGER,
    value:snippet.title::STRING,
    'GERMANY'
FROM
    YOUTUBE_REGIONS_DATASET.ALLREGIONS_VIDEOS_CSV.JSON_STAGING,
    LATERAL FLATTEN(input => json_data:items);
TRUNCATE TABLE json_staging;
select
    *
from
    video_categories
where
    region = 'GERMANY'