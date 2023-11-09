from googleapiclient.discovery import build
import pymongo
import mysql.connector 
import pandas as pd
from datetime import datetime
import streamlit as st
from streamlit_option_menu import option_menu
import isodate 

api_key="AIzaSyBs9ty24bC4TKkl8rCKTMiDwLGij715BMQ"
#channel_ids=['UCpEo2A5KTWQGvSr2k-othcA',
#'UCOeIvpX4x09EsWS-6FwWonQ',
#'UCE6Qmh-8ovqDoB6S4YjDKUg',
#'UC1RauiosDyz3K16X1wkaeiA',
#'UC7cs8q-gJRlGwj4A8OmCmXg',
#'UCwr-evhuzGZgDFrq_1pLt_A',
#'UCiGuBDNF9Itd8WmvET23_BQ',
#'UCEKcQCTOaJISRIf3P2HK4MA',
#'UCzdOan4AmF65PmLLks8Lmww',
#'UCW0_B9HlOaWQ4jcQP3uZZ4g',
# 'UCnz-ZXXER4jOvuED5trXfEA' - techTFQ,
# 'UCCJ8b9cCjNJxCABLXZw51dg' - learn with malathi,
# 'UC7cs8q-gJRlGwj4A8OmCmXg' - Alex the analys]
youtube=build(
        "youtube","v3",developerKey=api_key)

#function for channel details
def get_channel_details(channel_ids):
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_ids,
        
    )
    response = request.execute()
    for i in range(0,len(response['items'])):
        data = dict(Channel_name = response['items'][i]['snippet']['title'],
                    Channel_id = response['items'][i]['id'],
                    Channel_description = response['items'][i]['snippet']['description'],
                    Total_views = response['items'][i]['statistics']['viewCount'],
                    Total_subscriber = response['items'][i]['statistics']['subscriberCount'],
                    Total_Video = response['items'][i]['statistics']['videoCount'],
                    playlist_id = response['items'][i]['contentDetails']['relatedPlaylists']['uploads']
                   )
    return data
#to get playlist details
def get_playlist_details(channel_ids):
    playlist=[]
    next_page_token=None
    more_pages=True
    while more_pages:
        request = youtube.playlists().list(
                part="snippet,contentDetails",
                channelId=channel_ids,
                maxResults=50,
                pageToken=next_page_token

            )
        response = request.execute()
        for i in range(0,len(response['items'])):
            data=dict(Playlist_id=response['items'][i]['id'],
                      Channel_id=response['items'][i]['snippet']['channelId'],
                      Playlist_name=response['items'][i]['snippet']['title']
            )
            playlist.append(data)
            next_page_token=response.get('nextPageToken')
            if next_page_token is None:
                more_pages=False
    return playlist
#function to get video id
def get_video_ids(channel_ids):
    video_id=[]
    next_page_token = None
    more_page = True 
    while more_page:
        req = youtube.channels().list(
                part="contentDetails",
                id=channel_ids).execute()
        playlist_id = req['items'][0]['contentDetails']['relatedPlaylists']['uploads']
        request = youtube.playlistItems().list(
                                 part="contentDetails",
                                 playlistId=playlist_id,
                                 maxResults=50,
                                 pageToken=next_page_token
        )
        response=request.execute()
        for i in range(len(response['items'])):
            video_id.append(response['items'][i]['contentDetails']['videoId'])
        next_page_token = response.get('nextPageToken')
        if next_page_token is None:
            more_page = False
    return video_id
#to get video details
def get_video_details(video_id):
    video_details=[]
    for ids in video_id:
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=ids
        )
        response = request.execute()
        for i in range(len(response['items'])):
            data=dict(Id=response['items'][i]['id'],
                      channel_id=response['items'][i]['snippet']['channelId'],
                      Channel_name=response['items'][i]['snippet']['channelTitle'],
                      Video_name=response['items'][i]['snippet']['title'],
                      Video_description=response['items'][i]['snippet']['description'],
                      Publish_date= response['items'][i]['snippet']['publishedAt'],
                      view_count= response['items'][i]['statistics']['viewCount'],
                      like_count = response['items'][i]['statistics']['likeCount'],
                      Favourite_count = response['items'][i]['statistics']['favoriteCount'],
                      Comment_count = response['items'][i]['statistics']['commentCount'],
                      Duration = isodate.parse_duration(response['items'][i]['contentDetails']['duration']).total_seconds(),
                      Thumbnail = response['items'][i]['snippet']['thumbnails']['default']['url'],
                      Caption_status= response['items'][i]['contentDetails']['caption'])
            video_details.append(data)
    return video_details
#to get comment details
def get_comment_details(video_id):
    comment_details=[]
    try:
        for ids in video_id:
            request = youtube.commentThreads().list(
                part="snippet",
                videoId=ids,
                maxResults=50
            )
            response = request.execute()
            for i in range(len(response['items'])):
                data=dict(channel_id=response['items'][i]['snippet']['channelId'],
                          Video_id=response['items'][i]['snippet']['videoId'],
                          Comment_id=response['items'][i]['id'],
                          Comment=response['items'][i]['snippet']['topLevelComment']['snippet']['textDisplay'],
                          Comment_author=response['items'][i]['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                          Comment_published_date=response['items'][i]['snippet']['topLevelComment']['snippet']['publishedAt']
                         )
                comment_details.append(data)
    except:
        pass 
    return comment_details
#mongodb connection
client = pymongo.MongoClient("mongodb://localhost:27017")
db = client["youtubedata"]
#main function
def main(channel_id):
    ch_details = get_channel_details(channel_id)
    pl_details = get_playlist_details(channel_id)
    v_ids = get_video_ids(channel_id)
    v_details = get_video_details(v_ids)
    cm_details = get_comment_details(v_ids)

    collection = db["channel_details"]
    collection.insert_one({"channel_details":ch_details,
                           "playlist_details":pl_details,
                           "video_details":v_details,
                           "comment_details":cm_details})
    
    return "successfully uploaded all data"
#sql connection
mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="Nivya@123",
    database="youtubedata",
    autocommit=True
)
mycursor = mydb.cursor()
#creating tables for channel,playlist,video,comment
def channel_table():
    drop_query="drop table if exists channels"
    mycursor.execute(drop_query)

    try:
        create_query='''create table if not exists channels(Channel_name varchar(100),
        Channel_id varchar(100) primary key,
        Channel_description longtext,
        Total_views bigint,
        Total_subscriber bigint,
        Total_Video bigint,
        playlist_id varchar(100)
        )'''
        mycursor.execute(create_query)
    except:
        print("Table already created")


    ch_details=[]
    db = client["youtubedata"]
    collection = db["channel_details"]
    for i in collection.find({},{"_id":0,"channel_details":1}):
        ch_details.append(i["channel_details"])
    df=pd.DataFrame(ch_details)


    for index,row in df.iterrows():
        insert_query='''insert into channels(Channel_name,
                                                Channel_id,
                                                Channel_description,
                                                Total_views,
                                                Total_subscriber,
                                                Total_Video,
                                                playlist_id)

                                                values(%s,%s,%s,%s,%s,%s,%s)'''

        values=(row["Channel_name"],
        row["Channel_id"],
        row["Channel_description"],
        row["Total_views"],
        row["Total_subscriber"],
        row["Total_Video"],
        row["playlist_id"])

        mycursor.execute(insert_query,values)
def playlist_table():
    drop_query="drop table if exists playlist"
    mycursor.execute(drop_query)

    try:
        create_query='''create table if not exists playlist(Playlist_id varchar(100) primary key,
    Channel_id varchar(100),
    Playlist_name varchar(100)
    )'''
        mycursor.execute(create_query)
    except:
        print("Table already created")

    pl_details=[]
    db = client["youtubedata"]
    collection = db["channel_details"]
    for pl in collection.find({},{"_id":0,"playlist_details":1}):
        for i in range(len(pl["playlist_details"])):
            pl_details.append(pl["playlist_details"][i])
    df1=pd.DataFrame(pl_details)

    for index,row in df1.iterrows():
        insert_query1='''insert into playlist(Playlist_id,
                                                    Channel_id,
                                                    Playlist_name)

                                                    values(%s,%s,%s)'''

        values=(row["Playlist_id"],
                    row["Channel_id"],
                    row["Playlist_name"])

        mycursor.execute(insert_query1,values)

def video_table():
    drop_query="drop table if exists video"
    mycursor.execute(drop_query)

    try:
        create_query2='''create table if not exists video(Id varchar(100) primary key,
        channel_id varchar(100),
        Channel_name varchar(100),
        Video_name varchar(100),
        Video_description longtext,
        Publish_date timestamp,
        view_count bigint,
        like_count bigint,
        Favourite_count int,
        Comment_count bigint,
        Duration varchar(100),
        Thumbnail varchar(100),
        Caption_status varchar(10)
        )'''
        mycursor.execute(create_query2)
    except:
        print("Table already created")

            
    vd_details=[]
    db = client["youtubedata"]
    collection = db["channel_details"]
    for vd in collection.find({},{"_id":0,"video_details":1}):
        for i in range(len(vd["video_details"])):
            vd_details.append(vd["video_details"][i])
    df2=pd.DataFrame(vd_details)

    for index,row in df2.iterrows():
        formatted_datetime = datetime.strptime(row["Publish_date"], "%Y-%m-%dT%H:%M:%SZ").strftime("%Y-%m-%d %H:%M:%S")

        insert_query2='''insert into video(Id,
        channel_id,
        Channel_name,
        Video_name,
        Video_description,
        Publish_date,
        view_count,
        like_count,
        Favourite_count,
        Comment_count,
        Duration,
        Thumbnail,
        Caption_status)

        values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''

        values=(row["Id"],
                        row["channel_id"],
                        row["Channel_name"],
                        row["Video_name"],
                        row["Video_description"],
                        formatted_datetime,
                        row["view_count"],
                        row["like_count"],
                        row["Favourite_count"],
                        row["Comment_count"],
                        row["Duration"],
                        row["Thumbnail"],
                        row["Caption_status"])

        mycursor.execute(insert_query2,values)

def comment_table():
    drop_query="drop table if exists comments"
    mycursor.execute(drop_query)

    try:
        create_query3='''create table if not exists comments(channel_id varchar(100),
        Video_id varchar(100),
        Comment_id varchar(100) primary key,
        Comment longtext,
        Comment_author varchar(100),
        Comment_published_date timestamp
        )'''

        mycursor.execute(create_query3)
    except:
        print("Table already creaed")

    cm_details=[]
    db = client["youtubedata"]
    collection = db["channel_details"]
    for cm in collection.find({},{"_id":0,"comment_details":1}):
        for i in range(len(cm["comment_details"])):
            cm_details.append(cm["comment_details"][i])
    df3=pd.DataFrame(cm_details)
    
    for index,row in df3.iterrows():
        formatted_datetime = datetime.strptime(row["Comment_published_date"], "%Y-%m-%dT%H:%M:%SZ").strftime("%Y-%m-%d %H:%M:%S")

        insert_query3='''insert into comments(channel_id,
            Video_id,
            Comment_id,
            Comment,
            Comment_author,
            Comment_published_date
            )

            values(%s,%s,%s,%s,%s,%s)'''

        values=(row["channel_id"],
                            row["Video_id"],
                            row["Comment_id"],
                            row["Comment"],
                            row["Comment_author"],
                            formatted_datetime)
        mycursor.execute(insert_query3,values)    

def all_tables():
    channel_table()
    playlist_table()
    video_table()
    comment_table()
    return "Table created successfully"

#setting streamlit page
with st.sidebar:
    st.title("YOUTUBE DATA HARVESTING AND WAREHOUSING")
    selected= option_menu("Main Menu",["Extracting details & uploading in Mongo DB","Migrate to SQL","Analysis using SQL"],
                          icons =["cloud-upload","database","filetype-sql"] )
    
if selected == "Extracting details & uploading in Mongo DB":
    Ch_id = st.text_input("**Enter Channel Id**")
    if st.button("Extract details"):
        st.write("All details extracted successfully")
    if st.button("upload to mongoDB"):
        db = client["youtubedata"]
        collection = db["channel_details"]
        id=[]
        for i in collection.find({},{"_id":0,"channel_details":1}):
            id.append(i["channel_details"]["Channel_id"])
        if Ch_id in id:
            with st.spinner("uploading..."):
                st.success("This channel is already exists")
        else:
            with st.spinner("uploading..."):
                result = main(channel_id=Ch_id)
                st.success(result)
if selected == "Migrate to SQL":
    if st.button('Migrate'):
        with st.spinner("Migrating all data to sql"):
            tables = all_tables()
            st.success(tables)

if selected == "Analysis using SQL":
    qs = st.selectbox("Select a question",
     ("1. What are the names of all the videos and their corresponding channels?",
      "2. which channel has most number of videos and how many videos do they have?",
      "3. what are the top 10 most viewed videos and their channel?",
      "4. how many comments were made on each video, and what are their video name?",
      "5. which video have highest number of likes and their channel name?",
      "6. what is the total no.of likes in each video and their channel name?",
      "7. what is the total no.of views for each channel and their channel names?",
      "8. what are the names of all channels that have published videos in the year 2022?",
      "9. what is the average duartion of all video in each channel and their channel name?",
      "10. which video have highest comments and what are their channel names?"))

    if qs == "1. What are the names of all the videos and their corresponding channels?":
        qs1= "select Video_name,Channel_name from video;"
        mycursor.execute(qs1)
        result=mycursor.fetchall()
        st.dataframe(pd.DataFrame(result,columns= ["video name","channel name"]))

    if qs == "2. which channel has most number of videos and how many videos do they have?":
        qs2= '''select Channel_name, COUNT(Id) as Total_video
        from video
        group by Channel_name
        having COUNT(Id) = (select MAX(VideoCount) from (select COUNT(Id) as VideoCount from video group by Channel_name) as MaxVideoCount);
        '''
        mycursor.execute(qs2)
        result1=mycursor.fetchall()
        st.dataframe(pd.DataFrame(result1,columns= ["channel name","Total_video"]))

    if qs == "3. what are the top 10 most viewed videos and their channel?":
        qs3='''select view_count,Video_name,Channel_name from video 
        order by view_count desc
        limit 10'''
        mycursor.execute(qs3)
        result2=mycursor.fetchall()
        st.dataframe(pd.DataFrame(result2,columns= ["views","Video_name","channel name"]))

    if qs == "4. how many comments were made on each video, and what are their video name?":
        
        qs4='''select Comment_count,Video_name from video'''
        mycursor.execute(qs4)
        result3=mycursor.fetchall()
        st.dataframe(pd.DataFrame(result3,columns= ["Comment_count","Video_name"]))

    if qs == "5. which video have highest number of likes and their channel name?":
        qs5='''select Channel_name,Video_name,like_count from video
        order by like_count desc
        limit 1'''
        mycursor.execute(qs5)
        result4=mycursor.fetchall()
        st.dataframe(pd.DataFrame(result4,columns= ["channel name","Video_name","Total likes"]))

    if qs == "6. what is the total no.of likes in each video and their channel name?":
        qs6='''select Channel_name,Video_name,like_count from video'''
        mycursor.execute(qs6)
        result5=mycursor.fetchall()
        st.dataframe(pd.DataFrame(result5,columns= ["channel name","Video_name","Total likes"]))

    if qs == "7. what is the total no.of views for each channel and their channel names?":   
        qs7='''select sum(view_count),Channel_name from video
        group by Channel_name'''
        mycursor.execute(qs7)
        result6=mycursor.fetchall()
        st.dataframe(pd.DataFrame(result6,columns= ["Total view","channel name"]))

    if qs == "8. what are the names of all channels that have published videos in the year 2022?":
        qs8='''select Channel_name,Video_name from video
        where year(Publish_date) = 2022'''
        mycursor.execute(qs8)
        result7=mycursor.fetchall()
        st.dataframe(pd.DataFrame(result7,columns= ["channel name","video name"]))

    if qs == "9. what is the average duartion of all video in each channel and their channel name?":
        qs9='''select avg(Duration) as average_duration,Channel_name from video
        group by Channel_name'''
        mycursor.execute(qs9)
        result8=mycursor.fetchall()
        st.dataframe(pd.DataFrame(result8,columns= ["Average duration","channel name"]))

    if qs == "10. which video have highest comments and what are their channel names?":
        qs10 = '''select Video_name,Comment_count,Channel_name from video
        order by Comment_count desc'''
        mycursor.execute(qs10)
        result9=mycursor.fetchall()
        st.dataframe(pd.DataFrame(result9,columns= ["video name","comment count","channel name"]))

