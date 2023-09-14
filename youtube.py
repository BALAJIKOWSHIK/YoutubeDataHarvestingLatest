from googleapiclient.discovery import build
import pandas as pd
import numpy
import seaborn as sn
import streamlit as st 
from PIL import Image
import pymongo
from streamlit_option_menu import option_menu
import mysql.connector as sql


st.set_page_config(page_title= "Youtube Data Harvesting and Warehousing",
                   layout= "wide",
                   initial_sidebar_state= "expanded",
                   menu_items={'About': """#Created by *M K KOWSHIK BALAJI!*"""})


st.title('YouTube Data Harvesting and Warehousing using SQL, MongoDB and Streamlit')

with st.sidebar:
    selected = option_menu(None, ["Overview","Extract","View","Created By"],
                           icons=["house-door-fill","file-earmark-arrow-down-fill","eye-fill","person-circle"],
                           default_index=0,
                           orientation="vertical",
                           styles={"nav-link": {"font-size": "20px", "text-align": "centre", "margin": "0px",
                                                "--hover-color": "#F90072"},
                                   "icon": {"font-size": "20px"},
                                   "container" : {"max-width": "5000px"},
                                   "nav-link-selected": {"background-color": "#F9008C"}})

mydb = sql.connect(host="localhost",
                   user="gurubaran",
                   password='gurubaran@711999',
                   database= "kowshik_test",
                   port = "3306"
                  )
my_cursor = mydb.cursor(buffered=True)

api_key='AIzaSyBt8hOtVZHH2xIsdqGJnjFhEUEIaFP2BwI'
youtube=build('youtube','v3',developerKey=api_key)

client=pymongo.MongoClient('mongodb+srv://kowshik_balaji:iamkowshik@cluster0.abjmvcq.mongodb.net/')
db=client.Youtube_api_data
coll1=db.channel_Data
coll2=db.Video_data
coll3=db.Comment_data


def get_channel_info(youtube,channel_ids):#FUNCTION TO GET CHANNEL ID AND STATS
    all_data=[]
    try:
     response=youtube.channels().list(
     part='snippet,contentDetails,statistics',
     id=channel_ids
              ).execute()
     for i in range(len(response['items'])):
                data = dict(
                Channel_name=response['items'][i]['snippet']['title'],
                Views=response['items'][i]['statistics']['viewCount'],
                Subscribers=response['items'][i]['statistics']['subscriberCount'],
                Videos=response['items'][i]['statistics']['videoCount'],
                Description=response['items'][i]['snippet']['description'],
                Channel_id=response['items'][i]['id'],
                Playlist_id=response['items'][i]['contentDetails']['relatedPlaylists']['uploads'],
                )
     all_data.append(data)
     return all_data
    except KeyError:
           pass


def get_channel_videos(channel_ids):#Function get Uploads playlist id
    video_ids = []
    try:
      res = youtube.channels().list(id=channel_ids,
                                  part='contentDetails').execute()
      playlist_id = res['items'][0]['contentDetails']['relatedPlaylists']['uploads']
      next_page_token = None
      while True:
        res = youtube.playlistItems().list(playlistId=playlist_id, 
                                           part='snippet', 
                                           maxResults=50,
                                           pageToken=next_page_token).execute()
        for i in range(len(res['items'])):
            video_ids.append(res['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token = res.get('nextPageToken')
        if next_page_token is None:
            break
      return video_ids
    except TypeError:
        pass
    except :
        pass

def get_video_details(v_ids):# Function to get video details
    video_stats = []
    try:
      for i in range(0, len(v_ids), 50):
        response = youtube.videos().list(
                    part="snippet,contentDetails,statistics",
                    id=','.join(v_ids[i:i+50])).execute()
        for video in response['items']:
            video_details = dict(Channel_name = video['snippet']['channelTitle'],
                                Channel_id = video['snippet']['channelId'],
                                Video_id = video['id'],
                                Title = video['snippet']['title'],
                                Thumbnail = video['snippet']['thumbnails']['default']['url'],
                                Description = video['snippet']['description'],
                                Published_date = video['snippet']['publishedAt'],
                                Duration = video['contentDetails']['duration'],
                                Views = video['statistics'].get('viewCount'),
                                Likes = video['statistics'].get('likeCount'),
                                Comments = video['statistics'].get('commentCount'),
                                Favorite_count = video['statistics']['favoriteCount'],
                                Definition = video['contentDetails']['definition'],
                                Caption_status = video['contentDetails']['caption']
                               )
            video_stats.append(video_details)
      return video_stats
    except TypeError:
        pass

def get_comments_details(v_id):#Function to comment details
    comment_data = []
    try:
        next_page_token = None
        while True:
            response = youtube.commentThreads().list(part="snippet,replies",
                                                    videoId=v_id,
                                                    maxResults=100,
                                                    pageToken=next_page_token).execute()
            for cmt in response['items']:
                data = dict(Comment_id = cmt['id'],
                            Video_id = cmt['snippet']['videoId'],
                            Comment_text = cmt['snippet']['topLevelComment']['snippet']['textDisplay'],
                            Comment_author = cmt['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            Comment_posted_date = cmt['snippet']['topLevelComment']['snippet']['publishedAt'],
                            Like_count = cmt['snippet']['topLevelComment']['snippet']['likeCount'],
                            Reply_count = cmt['snippet']['totalReplyCount']
                           )
                comment_data.append(data)
            next_page_token = response.get('nextPageToken')
            if next_page_token is None:
                break
    except:
        pass
    return comment_data

def channel_names():
            collections1 =[]

            for i in coll1.find({},{"_id":0,"Channel_name":1}):
                    collections1.append(i['Channel_name'])
                    
            return collections1


if selected=='Overview':
    col1,col2 = st.columns(2,gap= 'small')
    col1.markdown("## :red[Domain] : Social Media")
    col1.markdown("## :red[Technologies used] : Python,MongoDB,Youtube API, MySql, Streamlit")
    col1.markdown("## :red[About] : Overall, this approach involves building a simple UI with Streamlit, retrieving data from the YouTube API, storing it in a MongoDB data lake, migrating it to a SQL data warehouse, querying the data warehouse with SQL, and displaying the data in the Streamlit app.")



if selected=='Extract':
  tab1, tab2,tab3 = st.tabs(["Extract", "Upload to MongoDB","Upload to sql"])

  with tab1:
     ch_info=st.text_input("Enter ChannelID(channel's home page > Right click > View page source > ctr+F > Find channel_id):")
     option = st.selectbox(
          ' Select the options to extract data',
          ('Channel info', 'video details', 'comment info'))
     if option=='Channel info':
        text1=get_channel_info(youtube,ch_info)
        st.table(text1)
     elif option == 'video details':
        text2=get_channel_videos(ch_info)
        text3=get_video_details(text2)
        st.table(text3)
     elif option=='comment info':
        text2=get_channel_videos(ch_info)
        def comments():
         com_d = []
         for i in text2:
            com_d += get_comments_details(i)
         return com_d

        text4=comments()
        st.table(text4)

  with tab2:
    if st.button("Upload"):
        text1=get_channel_info(youtube,ch_info)
        coll1.insert_many(text1)
        text2=get_channel_videos(ch_info)
        text3=get_video_details(text2)
        coll2.insert_many(text3)    
        def comments():
          com_d = []
          for i in text2:
             com_d += get_comments_details(i)
          return com_d
        text4=comments()
        coll3.insert_many(text4)  
        st.success("MongoDB upload is successful")
    else:
        st.markdown("Click upload  to import data into MongoDb")
  with tab3:
      st.markdown("### Select a channel to upload to SQL")
      chan_names = channel_names()
      inp = st.selectbox("Select channel",options=chan_names)
      def channels():#function to insert channel data into sql
        collections = db.channel_Data
        query = """INSERT INTO channel_data VALUES(%s,%s,%s,%s,%s,%s,%s)"""
        for i in collections.find({"Channel_name" : inp},{'_id' : 0}):
          print(i)
          my_cursor.execute(query,tuple(i.values()))
          mydb.commit()
    
      def video():#function to insert video data into sql
          collections1 = db.Video_data
          query1 = """INSERT INTO video_details VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
          st.write(inp)
          for i in collections1.find({"Channel_name" : inp},{'_id' : 0}):
            st.write(i)
            values = [str(val).replace("'", "''").replace('"', '""') if isinstance(val, str) else val for val in i.values()]
            # for val in i.values():
            #  my_cursor.execute(query1, list(val.replace("'", "''").replace('"', '""')))
            my_cursor.execute(query1, list(values))
            mydb.commit()


      def comments():#function to insert cooments data into sql
            collections2= db.Video_data
            collections3 = db.Comment_data
            query2 = """INSERT INTO comments_details VALUES(%s,%s,%s,%s,%s,%s,%s)"""

            for vid in collections2.find({"Channel_name" : inp},{'_id' : 0}):
                for i in collections3.find({'Video_id': vid['Video_id']},{'_id' : 0}):
                    my_cursor.execute(query2,tuple(i.values()))
                    mydb.commit()
      
      if st.button("Submit"):
         channels()
         video()
         comments()
         st.success("Upload to MySQL Successful")
             
                # sql queries to fetch data according to the question
if selected=='View':
    st.write("## :red[Click any question to get answers]")
    ques = st.selectbox('Questions',
    ['1. What are the names of all the videos and their corresponding channels?',
    '2. Which channels have the most number of videos, and how many videos do they have?',
    '3. What are the top 10 most viewed videos and their respective channels?',
    '4. How many comments were made on each video, and what are their corresponding video names?',
    '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
    '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
    '7. What is the total number of views for each channel, and what are their corresponding channel names?',
    '8. What are the names of all the channels that have published videos in the year 2022?',
    '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
    '10. Which videos have the highest number of comments, and what are their corresponding channel names?'])
     
    if ques == '1. What are the names of all the videos and their corresponding channels?':
        my_cursor.execute("""SELECT Title AS Video_name, channel_name AS Channel_Name
                            FROM video_details
                            ORDER BY channel_name""")
        df = pd.DataFrame(my_cursor.fetchall(),columns=my_cursor.column_names)
        st.write(df)
    elif ques == '2. Which channels have the most number of videos, and how many videos do they have?':  
        my_cursor.execute("""SELECT channel_name AS Channel_Name, Videos AS Total_Videos
                            FROM channel_data
                            ORDER BY Videos """)
        df = pd.DataFrame(my_cursor.fetchall(),columns=my_cursor.column_names)
        st.write(df)
    elif ques == '3. What are the top 10 most viewed videos and their respective channels?':
        my_cursor.execute("""SELECT channel_name AS Channel_Name, Title AS Video_Title, views AS Views 
                            FROM video_details
                            ORDER BY views Desc
                            LIMIT 10""")
        df = pd.DataFrame(my_cursor.fetchall(),columns=my_cursor.column_names)
        st.write(df)
    elif ques == '4. How many comments were made on each video, and what are their corresponding video names?':
        # my_cursor.execute("""SELECT a.Video_id AS Video_id, Title AS Video_Title, b.Comments
        #                     FROM video_details AS a
        #                     LEFT JOIN (SELECT Video_id,COUNT(Comment_id) AS Total_Comments
        #                     FROM comment_details GROUP BY Video_id) AS b
        #                     ON a.video_id = b.video_id
        #                     ORDER BY b.Total_Comments DESC""")
        my_cursor.execute("""SELECT a.video_id AS Video_id,a.Title, b.Total_Comments
                            FROM video_details AS a
                            LEFT JOIN (SELECT video_id,COUNT(comment_id) AS Total_Comments
                            FROM comments_details GROUP BY video_id) AS b
                            ON a.video_id = b.video_id
                            ORDER BY b.Total_Comments DESC""")
        df = pd.DataFrame(my_cursor.fetchall(),columns=my_cursor.column_names)
        st.write(df)
    elif ques == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
        my_cursor.execute("""SELECT channel_name AS Channel_Name,Title AS Title,Likes AS Like_Count 
                            FROM video_details
                            ORDER BY Likes DESC
                            LIMIT 10""")
        df = pd.DataFrame(my_cursor.fetchall(),columns=my_cursor.column_names)
        st.write(df)
    elif ques == '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
        my_cursor.execute("""SELECT Title AS Title, Likes AS Like_count
                            FROM video_details
                            ORDER BY Like_count DESC""")
        df = pd.DataFrame(my_cursor.fetchall(),columns=my_cursor.column_names)
        st.write(df)
    elif ques == '7. What is the total number of views for each channel, and what are their corresponding channel names?':
        my_cursor.execute("""SELECT channel_name AS Channel_Name, Views AS Views
                            FROM channel_data
                            ORDER BY Views DESC""")
        df = pd.DataFrame(my_cursor.fetchall(),columns=my_cursor.column_names)
        st.write(df)
    elif ques== '8. What are the names of all the channels that have published videos in the year 2022?':
        my_cursor.execute("""SELECT channel_name AS Channel_Name
                            FROM video_details
                            WHERE published_date LIKE '2022%'
                            GROUP BY channel_name
                            ORDER BY channel_name""")
        df = pd.DataFrame(my_cursor.fetchall(),columns=my_cursor.column_names)
        st.write(df)
    elif ques == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
        my_cursor.execute("""SELECT channel_name AS Channel_Name,
                            AVG(Duration)/60 AS "Average_Video_Duration (mins)"
                            FROM video_details
                            GROUP BY channel_name
                            ORDER BY AVG(duration)/60 DESC""")
        df = pd.DataFrame(my_cursor.fetchall(),columns=my_cursor.column_names)
        st.write(df)
    elif ques == '10. Which videos have the highest number of comments, and what are their corresponding channel names?':
        my_cursor.execute("""SELECT channel_name AS Channel_Name,Title AS Video_title,Comments AS Comments
                            FROM video_details
                            ORDER BY comments DESC
                            LIMIT 10""")
        df = pd.DataFrame(my_cursor.fetchall(),columns=my_cursor.column_names)
        st.write(df)

if selected=='Created By':
    col3,col4=st.columns(2)
    col3.markdown("## :red[NAME] : ***M K KOWSHIK BALAJI***")
    col3.markdown("## :red[Mail ID] : ***balajikowshik@gmail.com***")