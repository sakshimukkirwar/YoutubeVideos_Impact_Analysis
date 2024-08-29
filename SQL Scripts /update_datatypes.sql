-- Step 1: Create new temporary columns with INTEGER data type
ALTER TABLE YOUTUBE_REGIONS_DATASET.ALLREGIONS_VIDEOS_CSV.VIDEO_STATISTICS ADD COLUMN VIEWS_INT INTEGER;
ALTER TABLE YOUTUBE_REGIONS_DATASET.ALLREGIONS_VIDEOS_CSV.VIDEO_STATISTICS ADD COLUMN LIKES_INT INTEGER;
ALTER TABLE YOUTUBE_REGIONS_DATASET.ALLREGIONS_VIDEOS_CSV.VIDEO_STATISTICS ADD COLUMN DISLIKES_INT INTEGER;
ALTER TABLE YOUTUBE_REGIONS_DATASET.ALLREGIONS_VIDEOS_CSV.VIDEO_STATISTICS ADD COLUMN COMMENT_COUNT_INT INTEGER;

-- Step 2: Convert data from VARCHAR to INTEGER using TRY_CAST to avoid errors
UPDATE YOUTUBE_REGIONS_DATASET.ALLREGIONS_VIDEOS_CSV.VIDEO_STATISTICS
SET VIEWS_INT = TRY_CAST(VIEWS AS INTEGER),
    LIKES_INT = TRY_CAST(LIKES AS INTEGER),
    DISLIKES_INT = TRY_CAST(DISLIKES AS INTEGER),
    COMMENT_COUNT_INT = TRY_CAST(COMMENT_COUNT AS INTEGER);

-- Step 3: Drop original VARCHAR columns
ALTER TABLE YOUTUBE_REGIONS_DATASET.ALLREGIONS_VIDEOS_CSV.VIDEO_STATISTICS DROP COLUMN VIEWS, LIKES, DISLIKES, COMMENT_COUNT;


-- Step 4: Rename the new INTEGER columns to the original column names
ALTER TABLE YOUTUBE_REGIONS_DATASET.ALLREGIONS_VIDEOS_CSV.VIDEO_STATISTICS RENAME COLUMN VIEWS_INT TO VIEWS;
ALTER TABLE YOUTUBE_REGIONS_DATASET.ALLREGIONS_VIDEOS_CSV.VIDEO_STATISTICS RENAME COLUMN LIKES_INT TO LIKES;
ALTER TABLE YOUTUBE_REGIONS_DATASET.ALLREGIONS_VIDEOS_CSV.VIDEO_STATISTICS RENAME COLUMN DISLIKES_INT TO DISLIKES;
ALTER TABLE YOUTUBE_REGIONS_DATASET.ALLREGIONS_VIDEOS_CSV.VIDEO_STATISTICS RENAME COLUMN COMMENT_COUNT_INT TO COMMENT_COUNT;

-- Step 1: Create a new temporary column with TIMESTAMP data type
ALTER TABLE YOUTUBE_REGIONS_DATASET.ALLREGIONS_VIDEOS_CSV.PUBLISH_DETAILS ADD COLUMN PUBLISH_TIME_TS TIMESTAMP_NTZ;

-- Step 2: Convert data from VARCHAR to TIMESTAMP using TRY_CAST
UPDATE YOUTUBE_REGIONS_DATASET.ALLREGIONS_VIDEOS_CSV.PUBLISH_DETAILS
SET PUBLISH_TIME_TS = TRY_CAST(PUBLISH_TIME AS TIMESTAMP_NTZ);

-- Step 3: Drop original VARCHAR column
ALTER TABLE YOUTUBE_REGIONS_DATASET.ALLREGIONS_VIDEOS_CSV.PUBLISH_DETAILS DROP COLUMN PUBLISH_TIME;

-- Step 4: Rename the new TIMESTAMP column to the original column name
ALTER TABLE YOUTUBE_REGIONS_DATASET.ALLREGIONS_VIDEOS_CSV.PUBLISH_DETAILS RENAME COLUMN PUBLISH_TIME_TS TO PUBLISH_TIME;


-- Step 1: Create new temporary columns with BOOLEAN data type
ALTER TABLE YOUTUBE_REGIONS_DATASET.ALLREGIONS_VIDEOS_CSV.VIDEO_STATUS ADD COLUMN COMMENTS_DISABLED_BOOL BOOLEAN;
ALTER TABLE YOUTUBE_REGIONS_DATASET.ALLREGIONS_VIDEOS_CSV.VIDEO_STATUS ADD COLUMN RATINGS_DISABLED_BOOL BOOLEAN;
ALTER TABLE YOUTUBE_REGIONS_DATASET.ALLREGIONS_VIDEOS_CSV.VIDEO_STATUS ADD COLUMN VIDEO_ERROR_OR_REMOVED_BOOL BOOLEAN;

-- Step 2: Convert data from VARCHAR to BOOLEAN using TRY_CAST
UPDATE YOUTUBE_REGIONS_DATASET.ALLREGIONS_VIDEOS_CSV.VIDEO_STATUS
SET COMMENTS_DISABLED_BOOL = TRY_CAST(COMMENTS_DISABLED AS BOOLEAN),
    RATINGS_DISABLED_BOOL = TRY_CAST(RATINGS_DISABLED AS BOOLEAN),
    VIDEO_ERROR_OR_REMOVED_BOOL = TRY_CAST(VIDEO_ERROR_OR_REMOVED AS BOOLEAN);

-- Step 3: Drop original VARCHAR columns
ALTER TABLE YOUTUBE_REGIONS_DATASET.ALLREGIONS_VIDEOS_CSV.VIDEO_STATUS DROP COLUMN COMMENTS_DISABLED, RATINGS_DISABLED, VIDEO_ERROR_OR_REMOVED;

-- Step 4: Rename the new BOOLEAN columns to the original column names
ALTER TABLE YOUTUBE_REGIONS_DATASET.ALLREGIONS_VIDEOS_CSV.VIDEO_STATUS RENAME COLUMN COMMENTS_DISABLED_BOOL TO COMMENTS_DISABLED;
ALTER TABLE YOUTUBE_REGIONS_DATASET.ALLREGIONS_VIDEOS_CSV.VIDEO_STATUS RENAME COLUMN RATINGS_DISABLED_BOOL TO RATINGS_DISABLED;
ALTER TABLE YOUTUBE_REGIONS_DATASET.ALLREGIONS_VIDEOS_CSV.VIDEO_STATUS RENAME COLUMN VIDEO_ERROR_OR_REMOVED_BOOL TO VIDEO_ERROR_OR_REMOVED;
