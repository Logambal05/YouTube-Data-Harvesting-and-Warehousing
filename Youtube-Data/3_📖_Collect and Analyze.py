import streamlit as st
import googleapiclient.discovery
import pandas as pd
import pymongo
import pymysql
import numpy as np

apikey ="AIzaSyCxXoWlEWO03FmcyJlNuRMyLcyXuOneKkk"
api_service_name="youtube"
api_version = "v3"

youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=apikey)

# mongodb connection
client = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = client["YouTube_warehousing"]
mycollection = mydb.YouTube_Data

# sql connection
myconnection = pymysql.connect(host = '127.0.0.1',user='root',passwd='Logi@2908')
cur = myconnection.cursor()
cur.execute("create database if not exists YouTube_DataAnalyzes")
myconnection = pymysql.connect(host = '127.0.0.1',user='root',passwd='Logi@2908',database = "YouTube_DataAnalyzes")
cur = myconnection.cursor()

def get_channel_details(channel_id):
    b = []
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id)
    response = request.execute()

    for item in response.get("items", []):
        details = {
            "channel_name": item['snippet']['title'],
            "channel_id": channel_id,
            "channel_description": item['snippet']['description'],
            "channel_joined": response['items'][0]['snippet']['publishedAt'],
            "channel_kind": item['kind'],
            "channel_SubCount": int(item['statistics']['subscriberCount']),
            "channel_VideoCount": int(item['statistics']['videoCount']),
            "channel_viewcount": int(item['statistics']['viewCount']),
            "overall_playlistid":item['contentDetails']['relatedPlaylists']['uploads']
        }
        b.append(details)

    return b

def Get_Playlist_Details(c_ids):
    b = []
    request = youtube.playlists().list(
        part="snippet",
        channelId=c_ids,
        maxResults=50)
    response = request.execute()
    for item in response.get("items", []):
        Playlist_Details= dict(playlist_id=item["id"],
                               channel_id=item["snippet"]["channelId"],  # The channel ID you specified
                               playlist_title=item["snippet"]["title"])
        b.append(Playlist_Details)
    return b

def get_video_ids(overall_playlist):
    request = youtube.playlistItems().list(
            part="contentDetails",
            playlistId=overall_playlist,
            maxResults=50)

    response = request.execute()

    video_ids=[]

    for i in range(len(response["items"])):
        video_ids.append(response["items"][i]["contentDetails"]["videoId"])

    next_page_token=response.get("nextPageToken")
    more_pages=True

    while more_pages:
        if next_page_token is None:
            more_pages=False
        else:
            request = youtube.playlistItems().list(
              part="contentDetails",
              playlistId=overall_playlist,
              maxResults=50,
              pageToken=next_page_token)

        response = request.execute()

        for i in range(len(response["items"])):
            video_ids.append(response["items"][i]["contentDetails"]["videoId"])

        next_page_token=response.get("nextPageToken")

    return (video_ids)

def Get_Video_Details(video_ids):
    b = []
    for i in range(0, len(video_ids), 50):
        request = youtube.videos().list(
            part="snippet,statistics,contentDetails",
            id=','.join(video_ids[i:i+50])
        )
        response = request.execute()

        for video in response.get("items", 0):#(key,deafult)
            video_details = dict(video_id= video['id'],
                                 video_name= video['snippet']['title'],
                                 video_description= video['snippet']['description'],
                                 channel_id= video['snippet']['channelId'],
                                 video_joined= video['snippet']['publishedAt'],
                                 video_duration=video["contentDetails"]["duration"],
                                 video_Views= int(video['statistics'].get('viewCount', 0)),  # Handle missing viewCount
                                 video_likes= int(video['statistics'].get('likeCount', 0)),  # Handle missing likeCount
                                 video_dislikes= int(video['statistics'].get('dislikeCount', 0)),  # Handle missing dislikeCount
                                 video_comments = int(video['statistics'].get('commentCount', 0)),  # Handle missing commentCount
                                 video_fav= int(video['statistics'].get('favoriteCount', 0)),  # Handle missing favoriteCount
                                 thumbnail_default= video['snippet']['thumbnails']['default']['url'])  # Store the default thumbnail URL
            b.append(video_details)
    return b



def Get_Comment_Details(video_ids):
    b = []
    max_comments = 500  # Set the maximum number of comments 

    for video_id in video_ids:
        try:
            nextPageToken = None  # Initialize the token for pagination

            while len(b) < max_comments:
                request = youtube.commentThreads().list(
                    part="snippet",
                    videoId=video_id,
                    textFormat="plainText",
                    maxResults=min(100, max_comments - len(b)),  # Limit the number of comments per request
                    pageToken=nextPageToken) # Use the token for pagination

                response = request.execute()

                for comment in response.get("items", []):
                    comment_snippet = comment["snippet"]["topLevelComment"]["snippet"]
                    b.append({
                        "video_id": video_id,
                        "channel_id":comment_snippet["channelId"],
                        "comment_id": comment["id"],
                        "comment_author": comment_snippet["authorDisplayName"],
                        "comment_text": comment_snippet["textDisplay"],
                        "comment_published_date":comment_snippet["publishedAt"]})
                   
                nextPageToken = response.get("nextPageToken")  # Get the next page token

                if not nextPageToken:
                    break  # Exit the loop if there are no more pages

        except:
            pass

    return b
def main (channel_id):
    channel_data = get_channel_details(channel_id)
    playlist_data = Get_Playlist_Details(channel_id)
    overall_playlist=channel_data[0].get("overall_playlistid")
    videos_ids = get_video_ids(overall_playlist)
    video_data = Get_Video_Details(videos_ids)
    comment_data = Get_Comment_Details(videos_ids)

    Whole_ChannelDetails = {"channel_data": channel_data,
                          "playlist_data": playlist_data,
                          "video_data": video_data,
                          "comment_data":comment_data}
    return Whole_ChannelDetails

def push_mongo(Channel_ID):
    if Channel_ID:
        mycollection = mydb.YouTube_Data
        info = mycollection.find_one({"channel_data.channel_id": Channel_ID})
        if not info:  # Check if the list is empty (no matching documents found)
            channel_info = main(Channel_ID)
            mycollection.insert_one(channel_info)
            st.success("Channel Information Has Been Successfully Stored In The Database.")
              
        else:
            st.warning("Channel Information Already Exists In The Database.")
            return
        

def retrieve(Channel_ID):
    b = []
    information = mycollection.find({"channel_data.channel_id": Channel_ID})
    for i in information:
        b.append(i)
    return b


Option = st.sidebar.selectbox("Stages Of The Web", ("Data Sculptor", "Data Voyage", "Query Craft"))
Channel_ID = st.sidebar.text_input("Share The Channel's Unique Identifier(channel_id)")
# Check if the selected option is "Data Sculptor"
if Option == "Data Sculptor":
    suboption = st.selectbox("YouTube Content Hub", ("Profile Of A Specific Channel", "Playlist Overview for a Specific Channel", "Specific Channel's Video Details", "Comment Insights for a Particular Channel"))
    if Option=="Data Sculptor" and st.button("Fetch Our Channel Details By Click Me"):
        if suboption == "Profile Of A Specific Channel" and Channel_ID and Option=="Data Sculptor":
            channel_data = get_channel_details(Channel_ID)
            st.write(channel_data)
        
        elif suboption == "Playlist Overview for a Specific Channel":
            playlist_data = Get_Playlist_Details(Channel_ID)
            st.write(playlist_data)
        
        elif suboption == "Specific Channel's Video Details":
            channel_data = get_channel_details(Channel_ID)
            ov = channel_data[0].get("overall_playlistid")
            video_ids = get_video_ids(ov)
            video_data = Get_Video_Details(video_ids)
            st.write(video_data)
        
        elif suboption == "Comment Insights for a Particular Channel":
            channel_data = get_channel_details(Channel_ID)
            ov = channel_data[0].get("overall_playlistid")
            video_ids = get_video_ids(ov)
            comment_data = Get_Comment_Details(video_ids)
            st.write(comment_data)
        
    buttons=st.button("Push to MongoDB Database")
    if buttons:
        push_mongo(Channel_ID)
      
#Data Migration
if Option == "Data Voyage":
    retrive_button=st.button("Retrive Channel Inforamtion") 
    if retrive_button:
        if not Channel_ID:
            st.warning("Please Enter Your Channel ID!")
        if retrive_button and Channel_ID:
            data = retrieve(Channel_ID)
            channel_df = pd.DataFrame(data[0]["channel_data"])
            playlist_df = pd.DataFrame(data[0]["playlist_data"])
            video_df = pd.DataFrame(data[0]["video_data"])
            comment_df = pd.DataFrame(data[0]["comment_data"])
            st.dataframe(channel_df) 
            st.dataframe(playlist_df) 
            st.dataframe(video_df)
            st.dataframe(comment_df)       
            st.success("Channel data is Retrived Successfully")
        else:
            st.warning("Channel not found in the database")
       
        
        success = True
# Insert data into the "channeldetails" table
        if not channel_df.empty:
            cur.execute("CREATE TABLE IF NOT EXISTS ChannelDetails(channel_name VARCHAR(255),channel_id VARCHAR(255),channel_description TEXT,channel_joined DATETIME,channel_kind VARCHAR(255),channel_SubCount INT,channel_VideoCount INT,channel_viewcount INT,overall_playlistid VARCHAR(255))")
            sql = "INSERT INTO ChannelDetails(channel_name,channel_id,channel_description,channel_joined,channel_kind,channel_SubCount,channel_VideoCount,channel_viewcount,overall_playlistid) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            for i in range(len(channel_df)):
                channel_df['channel_joined'] = pd.to_datetime(channel_df['channel_joined'])
                cur.execute(sql,tuple(channel_df.iloc[i]))
            myconnection.commit()  
        else:
            success = False  # Set the flag to False if this operation fails

# Insert data into the "playlistdetails" table
        if not playlist_df.empty:
            cur.execute("CREATE TABLE IF NOT EXISTS playlistDetails(playlist_id VARCHAR(255),channel_id VARCHAR(255),playlist_title VARCHAR(255))")
            sql = "INSERT INTO playlistDetails(playlist_id,channel_id,playlist_title) VALUES (%s,%s,%s)"
            for i in range(len(playlist_df)):
                cur.execute(sql,tuple(playlist_df.iloc[i]))
            myconnection.commit()
        else:
            success = False  # Set the flag to False if this operation fails

# Insert data into the "videodetails" table
        if not video_df.empty:
            cur.execute("CREATE TABLE IF NOT EXISTS videodetails(video_id VARCHAR(255),video_name VARCHAR(255),video_description TEXT,channel_id VARCHAR(255),video_joined DATETIME,video_duration DECIMAL, video_Views INT, video_likes INT, video_dislikes INT,video_comments INT,video_fav INT,thumbnail_default VARCHAR(255) )")
            sql = "INSERT INTO videodetails(video_id,video_name,video_description,channel_id,video_joined,video_duration,video_Views,video_likes,video_dislikes,video_comments,video_fav,thumbnail_default) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            for i in range(len(video_df)):
                video_df['video_joined'] = pd.to_datetime(video_df['video_joined'])
                video_df["video_duration"] = pd.to_timedelta(video_df["video_duration"]).dt.total_seconds()
                cur.execute(sql,tuple(video_df.iloc[i]))
            myconnection.commit()
        else:
            success = False  # Set the flag to False if this operation fails


# Insert data into the "commentdetails" table
        if not comment_df.empty:
            cur.execute("CREATE TABLE IF NOT EXISTS commentdetails(video_id VARCHAR(255),channel_id VARCHAR (255),comment_id VARCHAR(255),comment_author VARCHAR(255),comment_text TEXT,comment_published_date DATETIME)")
            sql = "INSERT INTO commentdetails(video_id,channel_id,comment_id,comment_author,comment_text,comment_published_date) VALUES (%s,%s,%s,%s,%s,%s)"
            for i in range(len(comment_df)):
                comment_df['comment_published_date'] = pd.to_datetime(comment_df['comment_published_date'])
                cur.execute(sql,tuple(comment_df.iloc[i]))
            myconnection.commit()
        else:
            success = False  # Set the flag to False if this operation fails

        if success:
            st.success("All data has been successfully pushed to the database.")
        else:
            st.warning("Some or all data insertions were unsuccessful.")

# SQL Query Sloving Part
if Option=="Query Craft":
    Questions = st.selectbox("SQL problems? We've got answers.",[None,"1.What are the names of all the videos and their corresponding channels?",
                  "2.Which channels have the most number of videos, and how many videos do they have?",
                  "3.What are the top 10 most viewed videos and their respective channels?",
                  "4.How many comments were made on each video, and what are their corresponding video names?",
                  "5.Which videos have the highest number of likes, and what are their corresponding channel names?",
                  "6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
                  "7.What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
                  "8.What are the names of all the channels that have published videos in the year 2022?",
                  "9.What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                  "10.Which videos have the highest number of comments, and what are their corresponding channel names?"])

    # Query Questions:
    # none
    if Questions==None:
        pass
    #1.What are the names of all the videos and their corresponding channels?
    if Questions =="1.What are the names of all the videos and their corresponding channels?":
        que1 = pd.read_sql_query("SELECT distinct video_name,channel_name FROM channeldetails JOIN videodetails ON channeldetails.channel_id = videodetails.channel_id",myconnection)
        st.dataframe(que1)

# 2.Which channels have the most number of videos, and how many videos do they have?
    if Questions =="2.Which channels have the most number of videos, and how many videos do they have?":
        que2 = pd.read_sql_query("select  distinct channel_name,channel_videoCount from channeldetails order by channel_videoCount desc",myconnection)
        st.dataframe(que2)

# 3.What are the top 10 most viewed videos and their respective channels?
    if Questions =="3.What are the top 10 most viewed videos and their respective channels?":
        que3 = pd.read_sql_query("select distinct channel_name,video_id,video_name,video_views FROM channeldetails JOIN videodetails ON channeldetails.channel_id = videodetails.channel_id order by videodetails.video_views desc limit 10",myconnection)
        st.dataframe(que3)

# 4.How many comments were made on each video, and what are their corresponding video names?
    if Questions=="4.How many comments were made on each video, and what are their corresponding video names?":
        que4 = pd.read_sql_query("select video_name,video_comments from videodetails",myconnection)
        st.dataframe(que4)

# 5.Which videos have the highest number of likes, and what are their corresponding channel names?
    if Questions=="5.Which videos have the highest number of likes, and what are their corresponding channel names?":
        que5 = pd.read_sql_query("select channel_name,video_likes FROM channeldetails JOIN videodetails ON channeldetails.channel_id = videodetails.channel_id order by videodetails.video_likes desc",myconnection)
        st.dataframe(que5)

# 6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?
    if Questions =="6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?":
        que6 = pd.read_sql_query("select video_name,video_likes,video_dislikes from videodetails",myconnection)
        st.dataframe(que6)

# 7.What is the total number of likes and dislikes for each video, and what are their corresponding video names?
    if Questions=="7.What is the total number of likes and dislikes for each video, and what are their corresponding video names?":
        que7 = pd.read_sql_query("select channel_name,channel_viewcount from channeldetails",myconnection)
        st.dataframe(que7)

# 8.What are the names of all the channels that have published videos in the year 2022?
    if Questions =="8.What are the names of all the channels that have published videos in the year 2022?":
        que8 = pd.read_sql_query("select channel_name,video_joined from channeldetails join videodetails on channeldetails.channel_id = videodetails.channel_id WHERE YEAR(videodetails.video_joined) = 2022",myconnection)
        st.dataframe(que8)

# 9.What is the average duration of all videos in each channel, and what are their corresponding channel names?
    if Questions =="9.What is the average duration of all videos in each channel, and what are their corresponding channel names?":
        que9 = pd.read_sql_query("select channel_name, AVG(video_duration) AS average_duration from channeldetails join videodetails on channeldetails.channel_id = videodetails.channel_id group by channel_name",myconnection)
        st.dataframe(que9)

# 10.Which videos have the highest number of comments, and what are their corresponding channel names?"
    if Questions== "10.Which videos have the highest number of comments, and what are their corresponding channel names?":
        que10 =pd.read_sql_query("select distinct channel_name,video_name,video_comments FROM channeldetails JOIN videodetails ON channeldetails.channel_id = videodetails.channel_id order by videodetails.video_comments desc",myconnection)
        st.dataframe(que10)
