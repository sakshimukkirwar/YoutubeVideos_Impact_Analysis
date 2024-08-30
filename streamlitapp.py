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

st.title("YouTube Videos Impact Analysis Dashboard :balloon:")
#############################################################################

# Ensure VIDEO_ID is numeric if not already
video_statistics_df['VIDEO_ID'] = pd.to_numeric(video_statistics_df['VIDEO_ID'], errors='coerce')

# Convert CATEGORY_ID to string in both DataFrames to ensure they can be merged
youtube_videos_clean_df['CATEGORY_ID'] = youtube_videos_clean_df['CATEGORY_ID'].astype(str)
video_categories_df['CATEGORY_ID'] = video_categories_df['CATEGORY_ID'].astype(str)

# Convert numeric columns to numeric and fill NaNs with 0
numeric_columns = ['VIEWS', 'LIKES', 'DISLIKES', 'COMMENT_COUNT']
for col in numeric_columns:
    youtube_videos_clean_df[col] = pd.to_numeric(youtube_videos_clean_df[col], errors='coerce').fillna(0)

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
    total_views = pd.to_numeric(category_data['total_views'].iloc[0], errors='coerce') / 1_000_000
    total_likes = pd.to_numeric(category_data['total_likes'].iloc[0], errors='coerce') / 1_000_000
    total_dislikes = pd.to_numeric(category_data['total_dislikes'].iloc[0], errors='coerce') / 1_000_000
    total_comments = pd.to_numeric(category_data['total_comments'].iloc[0], errors='coerce') / 1_000_000
    total_videos = pd.to_numeric(category_data['total_videos'].iloc[0], errors='coerce')

    # Convert large numbers to a more readable format in millions
    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("Total Views (in millions)", f"{total_views:.2f}M")
    col2.metric("Total Likes (in millions)", f"{total_likes:.2f}M")
    col3.metric("Total Dislikes (in millions)", f"{total_dislikes:.2f}M")
    col4.metric("Total Comments (in millions)", f"{total_comments:.2f}M")
    col5.metric("Total Videos", f"{total_videos:,}")

                                     
#############################################################################

# Merge youtube_videos_clean_df with channels_df to get CHANNEL_TITLE
youtube_videos_clean_df = youtube_videos_clean_df.merge(
    channels_df[['CHANNEL_ID', 'CHANNEL_TITLE']],
    on='CHANNEL_ID', 
    how='left'
)
# Attempt to convert PUBLISH_TIME to datetime, inferring format
youtube_videos_clean_df['PUBLISH_YEAR'] = pd.to_datetime(
    youtube_videos_clean_df['PUBLISH_TIME'], 
    errors='coerce',  # will turn bad parse into NaT
    infer_datetime_format=True  # try to guess the format
).dt.year

# Check if PUBLISH_YEAR was successfully created
if youtube_videos_clean_df['PUBLISH_YEAR'].isna().all():
    st.error("Failed to parse the 'PUBLISH_TIME' to extract 'PUBLISH_YEAR'. Check the data format.")
else:
    # Add a slider for year selection
    st.markdown("### Select Year to View Top Performing Channel")
    min_year = int(youtube_videos_clean_df['PUBLISH_YEAR'].min())
    max_year = int(youtube_videos_clean_df['PUBLISH_YEAR'].max())
    selected_year = st.slider("Year", min_year, max_year, step=1)

    # Filter videos by the selected year
    filtered_videos = youtube_videos_clean_df[youtube_videos_clean_df['PUBLISH_YEAR'] == selected_year]

    # Check if CHANNEL_TITLE exists in filtered_videos DataFrame
    if 'CHANNEL_TITLE' in filtered_videos.columns:
        # Calculate the top-performing channels
        top_channels = filtered_videos.groupby('CHANNEL_TITLE').agg(
            total_views=('VIEWS', 'sum'),
            total_likes=('LIKES', 'sum'),
            total_dislikes=('DISLIKES', 'sum'),
            total_comments=('COMMENT_COUNT', 'sum')
        ).reset_index()

        # Sort by the total views or another metric to find the top-performing channel
        top_channels_sorted = top_channels.sort_values(by='total_views', ascending=False).head(1)

        # Display the top-performing channel
        st.markdown(f"### Top Performing Channel for {selected_year}")
        if not top_channels_sorted.empty:
            channel_info = top_channels_sorted.iloc[0]
            st.write(f"**Channel:** {channel_info['CHANNEL_TITLE']}")
            st.write(f"**Total Views:** {channel_info['total_views']:,}")
            st.write(f"**Total Likes:** {channel_info['total_likes']:,}")
            st.write(f"**Total Dislikes:** {channel_info['total_dislikes']:,}")
            st.write(f"**Total Comments:** {channel_info['total_comments']:,}")
        else:
            st.write("No data available for the selected year.")
    else:
        st.error("The column 'CHANNEL_TITLE' is not available in the data after merging. Please check the data source.")

    # Example bar chart for top performing videos by views
    st.markdown("### Top Performing Videos by Views")
    top_videos = youtube_videos_clean_df[['TITLE', 'VIEWS']].sort_values(by='VIEWS', ascending=False).head(10)
    st.bar_chart(top_videos.set_index('TITLE')['VIEWS'])

############################################

from datetime import datetime

# Calculate the age of each video
youtube_videos_clean_df['PUBLISH_TIME'] = pd.to_datetime(youtube_videos_clean_df['PUBLISH_TIME'])
youtube_videos_clean_df['video_age_days'] = (datetime.now() - youtube_videos_clean_df['PUBLISH_TIME']).dt.days

# Group videos by age
video_age_distribution = youtube_videos_clean_df['video_age_days'].value_counts().sort_index()

# Display a bar chart of video ages
st.markdown("### Impact of Video Age on Engagement")

st.bar_chart(video_age_distribution)
##########################################################################


