import streamlit as st
import pandas as pd
from snowflake.snowpark.context import get_active_session

# Initialize the Snowflake session
session = get_active_session()

# Function to load data from Snowflake
def load_data(query):
    df = session.sql(query).to_pandas()
    return df

# Load data from Snowflake into DataFrames
channels_df = load_data("SELECT * FROM CHANNELS")
publish_details_df = load_data("SELECT * FROM PUBLISH_DETAILS")
video_categories_df = load_data("SELECT * FROM VIDEO_CATEGORIES")
video_statistics_df = load_data("SELECT * FROM VIDEO_STATISTICS")
video_status_df = load_data("SELECT * FROM VIDEO_STATUS")
video_tags_df = load_data("SELECT * FROM VIDEO_TAGS")
youtube_videos_clean_df = load_data("SELECT * FROM YOUTUBE_VIDEOS_CLEAN")

st.title("YouTube Videos Impact Analysis Dashboard")
#############################################################################
# Ensure VIDEO_ID is numeric if not already
video_statistics_df['VIDEO_ID'] = pd.to_numeric(video_statistics_df['VIDEO_ID'], errors='coerce')

# Convert CATEGORY_ID to string in both DataFrames to ensure they can be merged
youtube_videos_clean_df['CATEGORY_ID'] = youtube_videos_clean_df['CATEGORY_ID'].astype(str)
video_categories_df['CATEGORY_ID'] = video_categories_df['CATEGORY_ID'].astype(str)

# Merge category information into youtube_videos_clean_df
youtube_videos_clean_df = youtube_videos_clean_df.merge(
    video_categories_df[['CATEGORY_ID', 'CATEGORY_NAME']], 
    on='CATEGORY_ID', 
    how='left'
)

# Calculate KPIs by category
category_kpis = youtube_videos_clean_df.groupby('CATEGORY_NAME').agg(
    total_views=('VIEWS', 'sum'),
    total_likes=('LIKES', 'sum'),
    total_dislikes=('DISLIKES', 'sum'),
    total_comments=('COMMENT_COUNT', 'sum'),
    total_videos=('VIDEO_ID', 'nunique')
).reset_index()

# Display KPIs for each category using a selectbox
st.markdown("### Video Performance Metrics by Category")

selected_category = st.selectbox("Select Category", options=category_kpis['CATEGORY_NAME'].unique())

# Filter the category-specific KPIs
category_data = category_kpis[category_kpis['CATEGORY_NAME'] == selected_category]

if not category_data.empty:
    # Convert values to numeric if they are not already
    total_views = pd.to_numeric(category_data['total_views'].iloc[0], errors='coerce')
    total_likes = pd.to_numeric(category_data['total_likes'].iloc[0], errors='coerce')
    total_dislikes = pd.to_numeric(category_data['total_dislikes'].iloc[0], errors='coerce')
    total_comments = pd.to_numeric(category_data['total_comments'].iloc[0], errors='coerce')
    total_videos = pd.to_numeric(category_data['total_videos'].iloc[0], errors='coerce')

    # Convert large numbers to a more readable format
    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("Total Views", f"{total_views:,.0f}")
    col2.metric("Total Likes", f"{total_likes:,.0f}")
    col3.metric("Total Dislikes", f"{total_dislikes:,.0f}")
    col4.metric("Total Comments", f"{total_comments:,.0f}")
    col5.metric("Total Videos", f"{total_videos:,}")
                                     
#############################################################################

# Display most common tags
st.markdown("### Most Common Tags")

top_tags = video_tags_df['TAG'].value_counts().head(10)
st.write(top_tags)
###################


# Display top performing videos by views
st.markdown("### Top Performing Videos by Views")

top_videos = youtube_videos_clean_df[['TITLE', 'VIEWS']].sort_values(by='VIEWS', ascending=False).head(10)

st.bar_chart(top_videos.set_index('TITLE')['VIEWS'])

#########################

# Display video views by region (assuming you have latitude and longitude)
st.markdown("### Regional Video Performance")

# Simulating latitude and longitude data
region_views = youtube_videos_clean_df.groupby('REGION')['VIEWS'].sum().reset_index()
region_views['lat'] = [37.7749, 34.0522, 40.7128]  # Example latitudes for regions
region_views['lon'] = [-122.4194, -118.2437, -74.0060]  # Example longitudes for regions

st.map(region_views[['lat', 'lon']])

