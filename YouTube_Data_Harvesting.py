import pymongo
import mysql.connector
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine,MetaData,Integer, String, Column, Table,DateTime,BIGINT,VARCHAR,TIMESTAMP,TEXT,Date
from sqlalchemy.dialects.mysql import LONGTEXT
from googleapiclient.discovery import build
import matplotlib.pyplot as plt
import numpy as np



def api_connect():
    Api_ID = ""
    api_service_name = "youtube"
    api_version = "v3"
    youtube = build(api_service_name, api_version, developerKey=Api_ID)

    return youtube


youtube = api_connect()


# getting_channel_information.
def get_youtube_channel_data(channel_id):
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id

    )
    response = request.execute()

    for i in response["items"]:
        data = dict(ChannelName=i["snippet"]["title"], ChannelId=i["id"],
                    playlist_Id=i["contentDetails"]["relatedPlaylists"]["uploads"],
                    Channel_Description=i["snippet"].get('description','Null'))
        data_stat = i['statistics']
        data.update(data_stat)
        data.pop("hiddenSubscriberCount")
    return data


# get_playlist_info
def get_playlist_info(channel_id):
    playlist_data = []
    next_page_token = None
    next_page = True
    while next_page:

        request = youtube.playlists().list(
            part="snippet,contentDetails",
            channelId=channel_id,
            maxResults=50,
            pageToken=next_page_token
        )
        response = request.execute()

        for item in response['items']:
            data = {'PlaylistId': item['id'],
                    'Title': item['snippet']['title'],
                    'ChannelId': item['snippet']['channelId'],
                    'ChannelName': item['snippet']['channelTitle'],
                    'PublishedAt': item['snippet']['publishedAt'],
                    'VideoCount': item['contentDetails']['itemCount']}
            playlist_data.append(data)
        next_page_token = response.get('nextPageToken')
        if next_page_token is None:
            next_page = False
    return playlist_data


# getting vedio ID
def vedio_ids(channel_id):
    Vedio_id_list = []
    response_playlist_id = youtube.channels().list(part="contentDetails",
                                                   id=channel_id).execute()
    playlist_id = response_playlist_id["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]
    next_page_token = None
    while True:
        response_Vedio_ID = youtube.playlistItems().list(part="snippet",
                                                         playlistId=playlist_id,
                                                         maxResults=50,
                                                         pageToken=next_page_token).execute()

        # response_Vedio_ID
        for i in range(len(response_Vedio_ID["items"])):
            Vedio_id_list.append(response_Vedio_ID["items"][i]["snippet"]["resourceId"]["videoId"])
        next_page_token = response_Vedio_ID.get("nextPageToken")
        if next_page_token is None:
            break
    return Vedio_id_list


# getting vedio info
def get_video_info(Vedio_id_list):
    video_info = []

    for video_id in Vedio_id_list:
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=video_id)
        response = request.execute()

        for item in response["items"]:
            data = dict(ChannelName=item['snippet']['channelTitle'],
                        ChannelId=item['snippet']['channelId'],
                        Video_Id=item['id'],
                        Title=item['snippet']['title'],
                        Views=item['statistics']['viewCount'],
                        Likes=item['statistics'].get('likeCount'),
                        Comments=item['statistics'].get('commentCount'),
                        Tags=item['snippet'].get('tags'),
                        Thumbnail=item['snippet']['thumbnails']['default']['url'],
                        Description=item['snippet'].get('description','Null'),
                        Published_Date=item['snippet']['publishedAt'][0:10],
                        Duration=item['contentDetails']['duration'],
                        Favorite_Count=item['statistics']['favoriteCount'],
                        Definition=item['contentDetails']['definition'],
                        Caption_Status=item['contentDetails']['caption']
                        )
            video_info.append(data)
    return video_info


# get comment information
def get_comment_info(Vedio_id_list):
    Comment_Information = []
    try:
        for video_id in Vedio_id_list:

            request = youtube.commentThreads().list(
                part="snippet",
                videoId=video_id, maxResults=20
            )
            response = request.execute()

            for item in response["items"]:
                comment_information = dict(

                    Comment_Id=item["snippet"]["topLevelComment"]["id"],
                    ChannelId=item['snippet']['channelId'],
                    Video_Id=item["snippet"]["videoId"],
                    Comment_Text=item["snippet"]["topLevelComment"]["snippet"]["textOriginal"],
                    Comment_Author=item["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"])

                Comment_Information.append(comment_information)
    except:
        pass

    return Comment_Information


# Loading into mangodb

client = pymongo.MongoClient("mongodb+srv://user:psw@cluster0.kz2lose.mongodb.net/?retryWrites=true&w=majority")

db = client["Youtube_data"]


def channel_details(channel_id):
    ch_details = get_youtube_channel_data(channel_id)
    pl_details = get_playlist_info(channel_id)
    vi_ids = vedio_ids(channel_id)
    vi_details = get_video_info(vi_ids)
    com_details = get_comment_info(vi_ids)

    coll1 = db["channel_details"]
    coll1.insert_one(
        {"channel_information": ch_details, "playlist_information": pl_details, "video_information": vi_details,
         "comment_information": com_details})

    return "upload completed successfully"


# Table creation for channels,playlists, videos, comments
def channels_table():
    db = client["Youtube"]
    coll1 = db["channel_details"]
    pl_list = []
    ch_list = []
    db = client["Youtube_data"]
    coll1 = db["channel_details"]
    for ch_data in coll1.find({}, {"_id": 0, "channel_information": 1}):
        ch_list.append(ch_data["channel_information"])
    df = pd.DataFrame(ch_list)
    print(df)
    engine = create_engine(
        "mysql+pymysql://{user}:{pw}@{host}/{db}".format(host="127.0.0.1", db="youtube", user="root", pw="kevin"))

    Meta = MetaData()
    table_channel = Table(
        'channels', Meta, Column('ChannelName', VARCHAR(100)), Column('ChannelId', VARCHAR(100)),
        Column('playlist_Id', VARCHAR(100)), Column('Channel_Description', LONGTEXT), Column('viewCount', Integer),
        Column('subscriberCount', BIGINT), Column('videoCount', Integer)

    )
    Meta.create_all(engine)
    df.to_sql('channels', engine, if_exists='append', index=False)



def playlists_table():
    db = client["Youtube_data"]
    coll1 = db["channel_details"]
    pl_list = []
    for pl_data in coll1.find({}, {"_id": 0, "playlist_information": 1}):
        for i in range(len(pl_data["playlist_information"])):
            pl_list.append(pl_data["playlist_information"][i])
    df1 = pd.DataFrame(pl_list)
    print(df1)

    engine = create_engine(
        "mysql+pymysql://{user}:{pw}@{host}/{db}".format(host="127.0.0.1", db="youtube", user="root", pw="kevin"))
    Meta = MetaData()
    table_channel = Table(
        'playlist', Meta, Column('PlaylistId', VARCHAR(100), primary_key=True), Column('Title', VARCHAR(80)),
        Column('ChannelId', VARCHAR(100)), Column('ChannelName', VARCHAR(100)), Column('PublishedAt', TIMESTAMP),
        Column('VideoCount', Integer)

    )
    df1.to_sql('playlist', engine, if_exists='append', index=False)




def videos_table():
    client = pymongo.MongoClient("mongodb+srv://user:pws@cluster0.kz2lose.mongodb.net/?retryWrites=true&w=majority")
    vi_list = []
    db = client["Youtube_data"]
    coll1 = db["channel_details"]
    for vi_data in coll1.find({}, {"_id": 0, "video_information": 1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])
    df2 = pd.DataFrame(vi_list)
    df2 = df2.drop(['Tags'], axis=1)
    pd.set_option('display.max_columns', 14)
    print(df2)

    engine = create_engine(
        "mysql+pymysql://{user}:{pw}@{host}/{db}".format(host="127.0.0.1", db="youtube", user="root", pw="kevin"))
    Meta = MetaData()
    table_channel = Table(
        'videos', Meta, Column('ChannelName', VARCHAR(100)), Column('ChannelId', VARCHAR(100)),
        Column('Video_Id', VARCHAR(100), primary_key=True), Column('Title', LONGTEXT),
        Column('Views', BIGINT), Column('Likes', BIGINT), Column('Comments', Integer),
        Column('Thumbnail', TEXT), Column('Description',LONGTEXT), Column('Published_Date', Date),
        Column('Duration', TEXT),
        Column('Favorite_Count', Integer), Column('Definition', TEXT), Column('Caption_Status', VARCHAR(100))
    )

    Meta.create_all(engine)

    df2.to_sql('videos', engine, if_exists='append', index=False)





def comments_table():
    db = client["Youtube_data"]
    com_list = []
    db = client["Youtube_data"]
    coll1 = db["channel_details"]
    for com_data in coll1.find({}, {"_id": 0, "comment_information": 1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])
    df3 = pd.DataFrame(com_list)
    pd.set_option('display.max_columns', 5)
    print(df3)
    engine = create_engine(
        "mysql+pymysql://{user}:{pw}@{host}/{db}".format(host="127.0.0.1", db="youtube", user="root", pw="kevin"))

    Meta = MetaData()

    table_channel = Table(
        'comments', Meta, Column('Comment_Id', VARCHAR(100), primary_key=True), Column('ChannelId', VARCHAR(100)),
        Column('Video_Id', VARCHAR(100)), Column('Comment_Text', TEXT),
        Column('Comment_Author', TEXT))

    Meta.create_all(engine)

    df3.to_sql('comments', engine, if_exists='append', index=False)




def tables():
    channels_table()
    playlists_table()
    videos_table()
    comments_table()
    return "Tables Created successfully"


def show_channels_table():
    ch_list = []
    db = client["Youtube_data"]
    coll1 = db["channel_details"]
    for ch_data in coll1.find({}, {"_id": 0, "channel_information": 1}):
        ch_list.append(ch_data["channel_information"])
    channels_table = st.dataframe(ch_list)
    return channels_table


def show_playlists_table():
    db = client["Youtube_data"]
    coll1 = db["channel_details"]
    pl_list = []
    for pl_data in coll1.find({}, {"_id": 0, "playlist_information": 1}):
        for i in range(len(pl_data["playlist_information"])):
            pl_list.append(pl_data["playlist_information"][i])
    playlists_table = st.dataframe(pl_list)
    return playlists_table


def show_videos_table():
    vi_list = []
    db = client["Youtube_data"]
    coll2 = db["channel_details"]
    for vi_data in coll2.find({}, {"_id": 0, "video_information": 1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])
    videos_table = st.dataframe(vi_list)
    return videos_table


def show_comments_table():
    com_list = []
    db = client["Youtube_data"]
    coll3 = db["channel_details"]
    for com_data in coll3.find({}, {"_id": 0, "comment_information": 1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])
    comments_table = st.dataframe(com_list)
    return comments_table


st.set_page_config(
    page_title="YouTube Data Harvesting and Warehousing",
    page_icon="🏂",
    layout="wide",
    initial_sidebar_state="expanded")

st.title(":red[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
with st.sidebar:
    channel_id = st.text_input("Enter the Channel id")
    channels = channel_id.split(',')
    channels = [ch.strip() for ch in channels if ch]

    if st.button("Collect and Store data"):
        for channel in channels:
            ch_ids = []
            db = client["Youtube_data"]
            coll1 = db["channel_details"]
            for ch_data in coll1.find({}, {"_id": 0, "channel_information": 1}):
                ch_ids.append(ch_data["channel_information"]["ChannelId"])
            if channel in ch_ids:
                st.success("Channel details of the given channel id: " + channel + " already exists")
            else:
                output = channel_details(channel)
                st.success(output)

    if st.button("Migrate to SQL"):
        display = tables()
        st.success(display)


with st.sidebar:
    st.write(":orange[Channel Details]")
    show_channels_table()
    show_playlists_table()
    show_videos_table()
    show_comments_table()


# SQL connection
mydb = mysql.connector.connect(host="127.0.0.1",
                               user="root",
                               password="",
                               database="youtube",
                               port="3306"
                               )
cursor = mydb.cursor(buffered=True)
co1,co2,co3=st.columns(3)

with co1:
        st.write("All the videos and the Channel Name")
        query1 = "select Title as videos, ChannelName as ChannelName from videos;"
        cursor.execute(query1)
        mydb.commit()
        t1 = cursor.fetchall()
        st.write(pd.DataFrame(t1, columns=["Video Title", "ChannelName"]))

        st.write("2. Channels with most number of videos")
        query2 = "select ChannelName as ChannelName,videoCount as NO_Videos from channels order by videoCount desc;"
        cursor.execute(query2)
        mydb.commit()
        t2 = cursor.fetchall()
        st.write(pd.DataFrame(t2, columns=["ChannelName", "No Of Videos"]))
        st.bar_chart(pd.DataFrame(t2))

        st.write('3. 10 most viewed videos')
        query3 = '''select Views as views , ChannelName as ChannelName,Title as VideoTitle from videos 
                where Views is not null order by Views desc limit 10;'''
        cursor.execute(query3)
        mydb.commit()
        t3 = cursor.fetchall()
        st.write(pd.DataFrame(t3, columns=["views", "channel Name", "video title"]))


        st.write( '4. Comments in each video')
        query4 = "select Comments as No_comments ,Title as VideoTitle from videos where Comments is not null;"
        cursor.execute(query4)
        mydb.commit()
        t4 = cursor.fetchall()
        st.write(pd.DataFrame(t4, columns=["No Of Comments", "Video Title"]))


        st.write('5. Videos with highest likes')
        query5 = '''select Title as VideoTitle, ChannelName as ChannelName, Likes as LikesCount from videos 
               where Likes is not null order by Likes desc;'''
        cursor.execute(query5)
        mydb.commit()
        t5 = cursor.fetchall()
        st.write(pd.DataFrame(t5, columns=["video Title", "channel Name", "like count"]))

        st.write('6. likes of all videos')
        query6 = '''select Likes as likeCount,Title as VideoTitle from videos;'''
        cursor.execute(query6)
        mydb.commit()
        t6 = cursor.fetchall()
        st.write(pd.DataFrame(t6, columns=["like count", "video title"]))

        st.write('7. views of each channel')
        query7 = "select ChannelName as ChannelName, viewCount as Channelviews from channels;"
        cursor.execute(query7)
        mydb.commit()
        t7 = cursor.fetchall()
        st.write(pd.DataFrame(t7, columns=["channel name", "total views"]))


        st.write( '8. videos published in the year 2022')
        query8 = '''select Title as Video_Title, Published_Date as VideoRelease, ChannelName as ChannelName from videos 
        where extract(year from Published_Date) = 2022;'''
        cursor.execute(query8)
        mydb.commit()
        t8 = cursor.fetchall()

        st.write(pd.DataFrame(t8, columns=["Name", "Video Publised On", "ChannelName"]))

        st.write('9. average duration of all videos in each channel')
        query9 = "SELECT ChannelName as ChannelName, AVG(Duration) AS Duration FROM videos GROUP BY ChannelName;"
        cursor.execute(query9)
        mydb.commit()
        t9 = cursor.fetchall()

        t9 = pd.DataFrame(t9, columns=['ChannelTitle', 'Average Duration'])
        T9 = []
        for index, row in t9.iterrows():
            channel_title = row['ChannelTitle']
            average_duration = row['Average Duration']
            average_duration_str = str(average_duration)
            T9.append({"Channel Title": channel_title, "Average Duration": average_duration_str})
        st.write(pd.DataFrame(T9))

        st.write('10. videos with highest number of comments')
        query10 = '''select Title as VideoTitle, ChannelName as ChannelName, Comments as Comments from videos 
               where Comments is not null order by Comments desc;'''
        cursor.execute(query10)
        mydb.commit()
        t10 = cursor.fetchall()
        st.write(pd.DataFrame(t10, columns=['Video Title', 'Channel Name', 'NO Of Comments']))
with co2:
    engine = create_engine(
        "mysql+pymysql://{user}:{pw}@{host}/{db}".format(host="", db="", use="", pw=""))

    q2 = '''select ChannelName,videoCount from channels
          '''
    st.write(":red[Channels with most number of videos]")
    df2 = pd.read_sql(q2, engine)
    q2= df2.plot(kind='bar', x='ChannelName', y='videoCount', color=['#BB0000'])
    st.pyplot(plt.gcf())

    st.write(":red[10 Most Viewed Videos]")

    q3 = '''select Views as views , ChannelName as ChannelName,Title as VideoTitle from videos 
                    where Views is not null order by Views desc limit 10;'''
    df3 = pd.read_sql(q3, engine)
    q1=df3.plot(kind='bar', x='VideoTitle', y='views',color = ['#BB0000'])
    st.pyplot(plt.gcf())
with co3:
    st.write(":red[Most liked video]")
    q4 = '''select Likes as likeCount,Title as VideoTitle from videos order by Likes limit 15;'''
    df4 = pd.read_sql(q4, engine)
    q4 = df4.plot(kind='bar', x="VideoTitle", y='likeCount', color=['#BB0000'])
    st.pyplot(plt.gcf())

    st.write(":red[Total views of each channel]")
    q5="select ChannelName as ChannelName, viewCount as Channelviews from channels;"
    df5 = pd.read_sql(q5, engine)
    q4 = df5.plot(kind='bar', x="ChannelName", y='Channelviews')
    st.pyplot(plt.gcf())




