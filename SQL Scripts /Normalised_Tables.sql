--- 1. creating normalised table channels table

CREATE TABLE channels (
    channel_id VARCHAR(255) PRIMARY KEY,
    channel_title VARCHAR(255)
);

-- Populating the channels table
INSERT INTO channels (channel_id, channel_title)
SELECT DISTINCT channel_id, channel_title FROM youtube_videos_clean;

-- Updating youtube_videos_clean to use channel_id
ALTER TABLE youtube_videos_clean DROP COLUMN channel_title;


---2. creating normalised table video_tags

CREATE TABLE video_tags (
    video_id VARCHAR(16777216),  -- Make sure this matches the exact type and size as in youtube_videos_clean
    tag VARCHAR(16777216),
    PRIMARY KEY (video_id, tag),
    FOREIGN KEY (video_id) REFERENCES youtube_videos_clean(video_id)
);
---populate video_tags
INSERT INTO YOUTUBE_REGIONS_DATASET.ALLREGIONS_VIDEOS_CSV.VIDEO_TAGS (VIDEO_ID, TAG)
SELECT
    VIDEO_ID,
    TRIM(SPLIT_PART(value, '\"', 2)) AS TAG  -- Adjusting to split by both double quote and grab the actual tag content
FROM 
    YOUTUBE_REGIONS_DATASET.ALLREGIONS_VIDEOS_CSV.YOUTUBE_VIDEOS_CLEAN,
    LATERAL FLATTEN(INPUT => SPLIT(TAGS, '|'))  -- Splitting tags by the pipe character
WHERE 
    TAGS IS NOT NULL AND TAGS != '[none]';

----3. creating table publish_details
Create TABLE publish_details (
    video_id VARCHAR(16777216),
    publish_time varchar,
    PRIMARY KEY (video_id),
    FOREIGN KEY (video_id) REFERENCES youtube_videos_clean(video_id)
);

-- Populating publish_details
INSERT INTO publish_details
SELECT video_id, publish_time
FROM youtube_videos_clean;

------4. creating table video_statistics
CREATE TABLE video_statistics (
    video_id VARCHAR(16777216),
    views VARCHAR,
    likes VARCHAR,
    dislikes VARCHAR,
    comment_count VARCHAR,
    PRIMARY KEY (video_id),
    FOREIGN KEY (video_id) REFERENCES youtube_videos_clean(video_id)
);

INSERT INTO video_statistics
SELECT video_id, views, likes, dislikes, comment_count
FROM youtube_videos_clean;

--- 5. creating table video_status

CREATE TABLE video_status (
    video_id VARCHAR(16777216),
    comments_disabled VARCHAR,
    ratings_disabled VARCHAR,
    video_error_or_removed VARCHAR,
    PRIMARY KEY (video_id),
    FOREIGN KEY (video_id) REFERENCES youtube_videos_clean(video_id)
);

-- Populating video_status
INSERT INTO video_status
SELECT video_id, comments_disabled, ratings_disabled, video_error_or_removed
FROM youtube_videos_clean;
