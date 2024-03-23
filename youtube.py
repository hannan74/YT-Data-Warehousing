import googleapiclient.discovery     
from pprint import pprint                
import pymongo                       
import mysql.connector               
import pandas as pd                  
import streamlit as st               
from datetime import timedelta      
import re

api_key = "AIzaSyAjAoyNwPxnNKJSFntFk3wCKi0dVioG2EM"
youtube = googleapiclient.discovery.build("youtube", "v3", developerKey= api_key) 

#to get channel details

def channel_data(channel_id):
  request = youtube.channels().list(
          part="snippet,contentDetails,statistics",
          id=channel_id
      )
  response = request.execute()

  details = dict(channel_name = response['items'][0]['snippet']['title'],
                channel_id = response['items'][0]['id'],
                description = response['items'][0]['snippet']['description'],
                joined_at = response['items'][0]['snippet']['publishedAt'],
                thumbnails = response['items'][0]['snippet']['thumbnails']['medium']['url'],
                subscribers_count = response['items'][0]['statistics']['subscriberCount'],
                video_count = response['items'][0]['statistics']['videoCount'],
                views_count = response['items'][0]['statistics']['viewCount'],
                playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads'])
  return details

#To get Playlist Details

def playl_details(channel_id):

    next_pagetoken = None

    playlist_details =[]


    while True:
        request4 = youtube.playlists().list(
                    part="snippet,contentDetails",
                    channelId=channel_id,
                    maxResults=50,
                    pageToken =next_pagetoken
                )
        response4 = request4.execute()

        for item in response4['items']:
                playlist_data = dict(playlist_id = item['id'],
                                     channel_id = item['snippet']['channelId'],
                                     playlist_title = item['snippet']['title'],
                                     playlist_videocount = item['contentDetails']['itemCount'],
                                     channel_name = item['snippet']['channelTitle'],
                                     playlist_publishdate = item['snippet']['publishedAt']
                                    )
                playlist_details.append(playlist_data)

        next_pagetoken = response4.get('nextPageToken')

        if next_pagetoken is None:
            break
    return playlist_details

#To get video id's

def videoid(channel_id):
    video_id_list=[]

    request = youtube.channels().list(
            part="contentDetails",
            id= channel_id
        )
    response = request.execute()

    playlist_id= response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token = None

    while True:
        request1 = youtube.playlistItems().list(
                part = "snippet",
                playlistId= playlist_id,
                maxResults=50,
                pageToken=next_page_token
            )
        response1 = request1.execute()

        for i in range(len(response1['items'])):
            video_id_list.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token = response1.get('nextPageToken')

        if next_page_token is None:
            break
    return video_id_list 

#to get video details

def vid_details(vid_ids):
    video_details=[]

    for i in vid_ids:
        request2 = youtube.videos().list(
                part="snippet,contentDetails,statistics",
                id=i
        )
        response2 = request2.execute()

        for item in response2['items']:
             video_data = dict(channel_name = item['snippet']['channelTitle'],
                             channel_id = item['snippet']['channelId'],
                             video_id = item['id'],
                             video_name = item['snippet']['title'],
                             tags = item['snippet'].get('tags'),
                             thumbnail = item['snippet']['thumbnails']['default']['url'],
                             description = item['snippet']['description'],
                             published_date = item['snippet']['publishedAt'],
                             duration = item['contentDetails']['duration'],
                             definition = item['contentDetails']['definition'],
                             comment_count = item['statistics'].get('commentCount'),
                             likes_count = item['statistics'].get('likeCount'),
                             views_count = item['statistics']['viewCount'],
                             favourite_count = item['statistics']['favoriteCount'],
                             caption_status = item['contentDetails']['caption']
                             )
                
             video_details.append(video_data)

    return video_details

#To get comment Details 

def com_details(vid_ids):
    comment_details=[]
    try:
        for vi_ids in vid_ids:
            request3 = youtube.commentThreads().list(
                 part="snippet",
                 videoId=vi_ids,
                 maxResults = 10
               )
            response3 = request3.execute()

            for id in response3['items']:
                comment_data = dict(comment_id = id['id'],
                                    video_id = id['snippet']['topLevelComment']['snippet']['videoId'],
                                    comment = id['snippet']['topLevelComment']['snippet']['textDisplay'],
                                    comment_author = id['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                                    comment_publish_date = id['snippet']['topLevelComment']['snippet']['publishedAt']
                                   )

                comment_details.append(comment_data)

    except:
        pass
    return comment_details

#mongodb
client = pymongo.MongoClient("mongodb://localhost:27017")
db = client["youtube_data"]

def channel_details(channel_id):
    ch_data = channel_data(channel_id)
    pl_data = playl_details(channel_id)
    vi_ids = videoid(channel_id)
    vi_data = vid_details(vi_ids)
    com_data = com_details(vi_ids)
    
    collection1 = db["youtube_channel_details"]
    collection1.insert_one({"channel_info":ch_data,
                           "playlist_info":pl_data,
                           "video_info":vi_data,
                           "comment_info":com_data})
    
    return "Upload Completed"

def channel_table(channel_name_s):

    mydb = mysql.connector.connect(host ='localhost',user='root',password='7ApriL@2002',database='youtube')
    mycursor=mydb.cursor()



    query =  '''CREATE TABLE if not exists channels(channel_name varchar(80),
                                      channel_id varchar(50) primary key,
                                      description text,
                                      joined_at varchar(80),
                                      thumbnails text,
                                      subscribers_count bigint,
                                      video_count int,
                                      views_count bigint,
                                      playlist_id varchar(50))'''
    mycursor.execute(query)
    mydb.commit()

   

    #mongo to mysql

    single_channel_detail=[]
    db = client["youtube_data"]
    coll1 = db["youtube_channel_details"]
    for channel_datas in coll1.find({"channel_info.channel_name":channel_name_s},{"_id":0}):
        single_channel_detail.append(channel_datas["channel_info"])

    df_single_channel_detail = pd.DataFrame(single_channel_detail)




    for index,row in df_single_channel_detail.iterrows():
        insert_query = '''insert into channels(channel_name,
                                                channel_id,
                                                description,
                                                joined_at,
                                                thumbnails,
                                                subscribers_count,
                                                video_count,
                                                views_count,
                                                playlist_id)

                                                values(%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        values = (row['channel_name'],
                 row['channel_id'],
                 row['description'],
                 row['joined_at'],
                 row['thumbnails'],
                 row['subscribers_count'],
                 row['video_count'],
                 row['views_count'],
                 row['playlist_id'])
        try:
            mycursor.execute(insert_query,values)
            mydb.commit()
        except:
            news = f"Your Provided Channel Details for {channel_name_s} is already exists"
            return news
            
            
def playlist_table(channel_name_s):

    mydb = mysql.connector.connect(host ='localhost',user='root',password='7ApriL@2002',database='youtube')
    mycursor=mydb.cursor()


    create_query1 =  '''CREATE TABLE if not exists playlists(playlist_id varchar(80) primary key,
                                                            channel_id varchar(50),
                                                            playlist_title varchar(80),
                                                            playlist_videocount int,
                                                            channel_name varchar(80),
                                                            playlist_publishdate varchar(80))'''
    mycursor.execute(create_query1)
    mydb.commit()


    single_playlist_detail=[]
    db = client["youtube_data"]
    coll1 = db["youtube_channel_details"]
    for channel_datas in coll1.find({"channel_info.channel_name":channel_name_s},{"_id":0}):
        single_playlist_detail.append(channel_datas["playlist_info"])

    df_single_playlist_detail = pd.DataFrame(single_playlist_detail[0])

    


    for index,row in df_single_playlist_detail.iterrows():
            insert_query1 = '''insert into playlists(playlist_id,
                                                    channel_id,
                                                    playlist_title,
                                                    playlist_videocount,
                                                    channel_name,
                                                    playlist_publishdate)


                                                    values(%s,%s,%s,%s,%s,%s)'''

            values = (row['playlist_id'],
                     row['channel_id'],
                     row['playlist_title'],
                     row['playlist_videocount'],
                     row['channel_name'],
                     row['playlist_publishdate']
                     )

            try:
                mycursor.execute(insert_query1,values)
                mydb.commit()
            except:
                 print("Failed to insert channel:", channel_name_s)



def video_table(channel_name_s):

    mydb = mysql.connector.connect(host ='localhost',user='root',password='7ApriL@2002',database='youtube')
    mycursor=mydb.cursor()


    create_query2 =  '''CREATE TABLE if not exists videos(channel_name varchar(80),
                                                         channel_id varchar(80),
                                                         video_id varchar(30) primary key,
                                                         video_name varchar(150),
                                                         tags text,
                                                         thumbnail varchar(200),
                                                         description text,
                                                         published_date varchar(50),
                                                         duration TIME,
                                                         definition varchar(50),
                                                         comment_count int,
                                                         likes_count bigint,
                                                         views_count bigint,
                                                         favourite_count int,
                                                         caption_status varchar(50))'''

    mycursor.execute(create_query2)
    mydb.commit()

    single_video_detail=[]
    db = client["youtube_data"]
    coll1 = db["youtube_channel_details"]
    for channel_datas in coll1.find({"channel_info.channel_name":channel_name_s},{"_id":0}):
        single_video_detail.append(channel_datas["video_info"])

    df_single_video_detail = pd.DataFrame(single_video_detail[0])


    for index, row in df_single_video_detail.iterrows():
        tags_str = ', '.join(row['tags']) if isinstance(row['tags'], list) else ''
        duration_str = row['duration']

        # Parse duration string and convert to timedelta
        hours = 0
        minutes = 0
        seconds = 0
        pattern = re.compile(r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?')
        match = pattern.match(duration_str)
        if match:
            hours_str, minutes_str, seconds_str = match.groups()
            if hours_str:
                hours = int(hours_str)
            if minutes_str:
                minutes = int(minutes_str)
            if seconds_str:
                seconds = int(seconds_str)

        duration = timedelta(hours=hours, minutes=minutes, seconds=seconds)

        insert_query2 = '''insert into videos(
                                channel_name,
                                channel_id,
                                video_id,
                                video_name,
                                tags,
                                thumbnail,
                                description,
                                published_date,
                                duration,
                                definition,
                                comment_count,
                                likes_count,
                                views_count,
                                favourite_count,
                                caption_status
                            )
                            values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''

        values = (row['channel_name'],
                  row['channel_id'],
                  row['video_id'],
                  row['video_name'],
                  tags_str,  # Use the joined tags string
                  row['thumbnail'],
                  row['description'],
                  row['published_date'],
                  duration, # Use the converted duration
                  row['definition'],
                  row['comment_count'],
                  row['likes_count'],
                  row['views_count'],
                  row['favourite_count'],
                  row['caption_status']
                  )

        try:
            mycursor.execute(insert_query2, values)
            mydb.commit()
        except mysql.connector.Error as err:
            print("Failed to insert video:", row['video_id'])
            print("MySQL Error:", err.msg)

    update_description_query = '''
        UPDATE videos
        SET description = NULL
        WHERE description = '';
    '''
    mycursor.execute(update_description_query)

    update_tags_query = '''
        UPDATE videos
        SET tags = NULL
        WHERE tags = '';
    '''

    mycursor.execute(update_tags_query)
    mydb.commit()

def comments_table(channel_name_s):

    mydb = mysql.connector.connect(host ='localhost',user='root',password='7ApriL@2002',database='youtube')
    mycursor=mydb.cursor()



    create_query3 =  '''CREATE TABLE if not exists comments(comment_id varchar(80) primary key,
                                                            video_id varchar(50),
                                                            comment text,
                                                            comment_author varchar(150),
                                                            comment_publish_date varchar(50))'''


    mycursor.execute(create_query3)
    mydb.commit()


    single_comment_detail=[]
    db = client["youtube_data"]
    coll1 = db["youtube_channel_details"]
    for channel_datas in coll1.find({"channel_info.channel_name":channel_name_s},{"_id":0}):
        single_comment_detail.append(channel_datas["comment_info"])

    df_single_comment_detail = pd.DataFrame(single_comment_detail[0])


    for index,row in df_single_comment_detail.iterrows():
            insert_query3 = '''insert into comments(comment_id,
                                                    video_id,
                                                    comment,
                                                    comment_author,
                                                    comment_publish_date)


                                                    values(%s,%s,%s,%s,%s)'''

            values = (row['comment_id'],
                     row['video_id'],
                     row['comment'],
                     row['comment_author'],
                     row['comment_publish_date'])

            try:
                mycursor.execute(insert_query3,values)
                mydb.commit()
            except:
                 print("Failed to insert channel:", channel_name_s)

def tables(single_channel):
    news = channel_table(single_channel)
    if news:
        return news
    else:
        playlist_table(single_channel)
        video_table(single_channel)
        comments_table(single_channel)
    
        return "Tables Created Sucessfully"
    
#streamlit table
def view_channel_table():
    channel_listm = []
    db = client["youtube_data"]
    coll1 = db["youtube_channel_details"]
    for channel_datas in coll1.find({},{"_id":0,"channel_info":1}):
        channel_listm.append(channel_datas['channel_info'])

    dataframe = st.dataframe(channel_listm)
    
    return dataframe

def view_playlist_table():
    playlist_listm = []
    db = client["youtube_data"]
    coll1 = db["youtube_channel_details"]
    for playlist_datas in coll1.find({},{"_id":0,"playlist_info":1}):
        for i in range(len(playlist_datas["playlist_info"])):
            playlist_listm.append(playlist_datas["playlist_info"][i])

    dataframe1 = st.dataframe(playlist_listm)
    
    return dataframe1

def view_video_table():
    video_listm = []
    db = client["youtube_data"]
    coll1 = db["youtube_channel_details"]
    for video_datas in coll1.find({},{"_id":0,"video_info":1}):
        for i in range(len(video_datas["video_info"])):
            video_listm.append(video_datas["video_info"][i])

    dataframe2 = st.dataframe(video_listm)
    
    return dataframe2

def view_comment_table():
    comment_listm = []
    db = client["youtube_data"]
    coll1 = db["youtube_channel_details"]
    for comment_datas in coll1.find({},{"_id":0,"comment_info":1}):
        for i in range(len(comment_datas["comment_info"])):
            comment_listm.append(comment_datas["comment_info"][i])

    dataframe3 = st.dataframe(comment_listm)
    
    return dataframe3

#streamlit
with st.sidebar:
    st.title(":green[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
    st.header("Data Mining")
    st.caption("Data Scarping in Python")
    st.caption("Data Compilation")
    st.caption("MongoDB")
    st.caption("Linking with API")
    st.caption("Data Manipulation with SQL & MongoDB")
    
channel_id = st.text_input("Enter the channel ID")

if st.button("Scrape and Store Data"):
    ch_ids = []
    db = client["youtube_data"]
    coll1 = db["youtube_channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_info":1}):
        ch_ids.append(ch_data["channel_info"]["channel_id"])
        
    if channel_id in ch_ids:
        st.success("Channel Details of the given channel ID already exists")
    else:
        insert = channel_details(channel_id)
        st.success(insert)
        
all_channels=[]
db = client["youtube_data"]
coll1 = db["youtube_channel_details"]
for channel_datas in coll1.find({},{"_id":0,"channel_info":1}):
    all_channels.append(channel_datas["channel_info"]["channel_name"])
    
        
unique_channel = st.selectbox("Select the Desired Channel",all_channels)
        

if st.button("Data Transition to SQL"):
    move_tables = tables(unique_channel)
    st.success(move_tables)
    
show_table = st.radio("SELECT THE TYPE OF TABLE YOU WANT TO VIEW",("CHANNELS",'PLAYLIST','VIDEOS','COMMENTS'))

if show_table=="CHANNELS":
    view_channel_table()
    
elif show_table=="PLAYLIST":
    view_playlist_table()
    
elif show_table=="VIDEOS":
    view_video_table()
    
elif show_table=="COMMENTS":
    view_comment_table()
    
#MySQL connection

mydb = mysql.connector.connect(host ='localhost',user='root',password='7ApriL@2002',database='youtube')
mycursor=mydb.cursor()


query = st.selectbox("Select your Queries",("1. Names of all the videos and their corresponding channels?",
                                           "2.  Channels with most number of videos?",
                                           "3.  Top 10 most viewed videos and their respective channels?",
                                           "4.  Number of comments made in each video with their respective video name?",
                                           "5.  Videos with highest likes count with their respective channel name?",
                                           "6.  Total number of likes for each video with their respective video name?",
                                           "7.  Total views count for each channel with their corresponding channel name?",
                                           "8.  Names of all the channels that have published videos in the year 2022?",
                                           "9.  Average duration of all videos in each channel with their respective channel name?",
                                           "10. Videos with highest comment count with their corresponding channel name?"))

if query=="1. Names of all the videos and their corresponding channels?":

    query1 = '''select video_name as videos, channel_name as channels from videos'''

    mycursor.execute(query1)
    table1 = mycursor.fetchall()  
    mydb.commit()

    df = pd.DataFrame(table1, columns=["video name", "channel name"])
    st.write(df)
    
elif query=="2.  Channels with most number of videos?":

    query2 = '''select channel_name as channels, video_count as total_videos from channels
                 order by video_count desc'''

    mycursor.execute(query2)
    table2 = mycursor.fetchall()  
    mydb.commit()

    df2 = pd.DataFrame(table2, columns=["channel name", "total videos"])
    st.write(df2)
    
elif query=="3.  Top 10 most viewed videos and their respective channels?":

    query3 = '''select views_count as views, channel_name as channels,video_name as videos from videos
                 where views_count is not null
                 order by views_count desc limit 10'''

    mycursor.execute(query3)
    table3 = mycursor.fetchall()  
    mydb.commit()

    df3 = pd.DataFrame(table3, columns=["views", "channels","videos"])
    st.write(df3)
    
elif query=="4.  Number of comments made in each video with their respective video name?":

    query4 = '''select comment_count as total_comments, video_name as videos from videos
                 where comment_count is not null
                 '''

    mycursor.execute(query4)
    table4 = mycursor.fetchall()  
    mydb.commit()

    df4 = pd.DataFrame(table4, columns=["total_comments", "videos"])
    st.write(df4)
    

elif query=="5.  Videos with highest likes count with their respective channel name?":

    query5 = '''select video_name as videos, channel_name as channels, likes_count as total_likes from videos
                 where likes_count is not null
                 order by likes_count desc'''

    mycursor.execute(query5)
    table5 = mycursor.fetchall()  
    mydb.commit()

    df5 = pd.DataFrame(table5, columns=["videos","channels","total_likes"])
    st.write(df5)
    

elif query=="6.  Total number of likes for each video with their respective video name?":

    query6 = '''select video_name as videos, likes_count as total_likes from videos

                 '''

    mycursor.execute(query6)
    table6 = mycursor.fetchall()  
    mydb.commit()

    df6 = pd.DataFrame(table6, columns=["videos","total_likes"])
    st.write(df6)
    

elif query=="7.  Total views count for each channel with their corresponding channel name?":

    query7 = '''select channel_name as channels,views_count as total_views from channels

                 '''

    mycursor.execute(query7)
    table7 = mycursor.fetchall()  
    mydb.commit()

    df7 = pd.DataFrame(table7, columns=["channels","total_views"])
    st.write(df7)
    
elif query=="8.  Names of all the channels that have published videos in the year 2022?":

    query8 = '''select channel_name as channels,video_name as videos,published_date as published from videos

                 where extract(year from published_date)= 2022'''

    mycursor.execute(query8)
    table8 = mycursor.fetchall()  
    mydb.commit()

    df8 = pd.DataFrame(table8, columns=["channels","videos","published_date"])
    st.write(df8)

elif query=="9.  Average duration of all videos in each channel with their respective channel name?":

    query9 = '''SELECT channel_name AS channels, SEC_TO_TIME(AVG(TIME_TO_SEC(duration))) AS average_duration FROM videos GROUP BY channel_name'''
    mycursor.execute(query9)
    table9 = mycursor.fetchall()  
    mydb.commit()

    df9 = pd.DataFrame(table9, columns=["channels","average_duration"])
    st.write(df9)


    
elif query=="10. Videos with highest comment count with their corresponding channel name?":

    query10 = '''select channel_name as channels,video_name as videos,comment_count as total_comments from videos
                 where comment_count is not null
                 order by comment_count desc'''

    mycursor.execute(query10)
    table10 = mycursor.fetchall()  
    mydb.commit()

    df10 = pd.DataFrame(table10, columns=["channels","videos","total_comments"])
    st.write(df10)
    
