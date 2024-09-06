# YouTube Videos Impact Analysis Dashboard 📊

This project involves building an interactive dashboard using **Streamlit** to analyze and visualize YouTube video performance metrics. The dashboard integrates with **Snowflake** to retrieve datasets of YouTube video statistics across regions and categories. It aims to provide insightful visualizations about video performance, top-performing channels, and the impact of video age on engagement.

## Technologies Used

- **Snowflake**: Used as the data warehouse to store and query YouTube video datasets.
- **Streamlit**: A Python-based app framework used to build the interactive dashboard.
- **SQL**: Used for querying and managing the video datasets in Snowflake.
- **Pandas**: For data manipulation and analysis.

## Datasets

The project uses YouTube video data stored in Snowflake with the following tables:

- **YOUTUBE_VIDEOS_CLEAN**: Cleaned dataset of YouTube videos containing columns such as video ID, publish time, views, likes, dislikes, comment count, etc.
- **VIDEO_STATISTICS**: Contains video statistics by regions such as views, likes, dislikes, etc.
- **PUBLISH_DETAILS**: Includes publish times and other metadata for YouTube videos.

## Folder Structure

- **Raw Data/**: Contains the raw datasets extracted from Snowflake.
- **SQL Scripts/**: SQL scripts used to create and query the tables in Snowflake.
- **streamlitapp.py**: The main Python file to run the Streamlit dashboard.

## Streamlit App

https://github.com/user-attachments/assets/86e02df9-82d9-40d4-9bb1-53b2f9e6a59f

## Key Features of the Dashboard

### 1. Overall Video Performance Metrics by Category
Displays key performance indicators (KPIs) such as total views, total likes, total dislikes, and total comments across various YouTube categories. Users can select a category from a dropdown menu to view metrics for a specific content type.

### 2. Top Performing Channels by Year
Users can select a year to view the top-performing YouTube channels for that particular year. Metrics such as total views, likes, dislikes, and comments are displayed for the top channel of the selected year.

## Future Enhancements

- Add filtering options for video performance based on geographic regions.
- Integrate tag analysis to display popular tags for top-performing videos.
- Implement trend analysis to track changes in views and engagement over time.
