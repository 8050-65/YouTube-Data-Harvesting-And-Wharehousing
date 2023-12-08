from googleapiclient.discovery import build
import pymongo
import  psycopg2
import pandas as pd
import streamlit as st


#API KEY CONNECTION

def Api_connect():
    Api_Key="AIzaSyAeYql7-3X-BqsBjss3d_DHc3byhnLQBmY"

    api_service_name="youtube"
    
    api_version="v3"

    youtube=build(api_service_name,api_version,developerKey=Api_Key)

    return youtube

youtube=Api_connect()


#get channel details
def get_channel_info(channel_id):


        request=youtube.channels().list(
                part="snippet,contentDetails,statistics",
                id=channel_id)
        
                                        
        response=request.execute()

        for item in response['items']:
                data=dict(channel_name=item['snippet']['title'],
                          channel_id=item['id'],
                          channel_description=item['snippet']['description'],
                          channel_publishedat=item['snippet']['publishedAt'],
                          channel_playlists=item['contentDetails']['relatedPlaylists'],
                          channel_playlist_id=item['contentDetails']['relatedPlaylists']['uploads'],
                          channel_likes=item['contentDetails']['relatedPlaylists']['likes'],
                          channel_subscribercount=item['statistics']['subscriberCount'],
                          channel_videocount=item['statistics']['videoCount'],
                          channel_viewcount=item['statistics']['viewCount'])
        return data 


# get video ids

def get_videos_ids(channel_id):
    video_ids = []
    response = youtube.channels().list(id=channel_id,
                                       part="contentDetails").execute()
    
    playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    
    next_page_token = None
    while True:
        response1 = youtube.playlistItems().list(
            part='snippet',
            playlistId=playlist_id,
            maxResults=50,
            pageToken=next_page_token
        ).execute()
    
        for i in range(len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
    
        next_page_token = response1.get('nextPageToken')
    
        if not next_page_token:
            break

    return video_ids


    #get video information
def get_video_info(video_ids):
        video_data=[]
        for video_id in video_ids:
            request=youtube.videos().list(
                part="snippet,ContentDetails,statistics",
                id=video_id
            )
            response=request.execute()

            for item in response["items"]:
                data=dict(Channel_Name=item['snippet']['channelTitle'],
                        Channel_Id=item['snippet']['channelId'],
                        Video_Id=item['id'],
                        Title=item['snippet']['title'],
                        Tags=item['snippet'].get('tags', []),
                        Thumbnail=item['snippet']['thumbnails']['default']['url'],
                        Description=item['snippet'].get('description'),
                        Published_Date=item['snippet']['publishedAt'],
                        Duration=item['contentDetails']['duration'],
                        Veiws=item['statistics'].get('viewCount',0),
                        likes=item['statistics'].get('likeCount',0),
                        Comments=item['statistics'].get('commentCount',0),
                        Favorite_count=item['statistics'].get('favoriteCount',0),
                        Definition=item['contentDetails']['definition'],
                        Caption_status=item['contentDetails']['caption']
                        )
                video_data.append(data)
        return video_data


#get comment information
def get_comment_info(video_ids):
    Comment_data=[]                                        #comment_data=[] for all cooment details is to fill in list 1 by 1
    try:                                                   #try for knowing /checking weather channel is ok or not
        for video_id in video_ids:                         #for all video comment details
            request=youtube.commentThreads().list(         #video_Ids to video_ids for UPDATED list by def funtion for json format 
                part="snippet",
                videoId=video_id,
                maxResults=50
            )
            response=request.execute()

            for item in response['items']:
                data=dict(Comment_Id=item['snippet']['topLevelComment']['id'],
                        Video_Id=item['snippet']['topLevelComment']['snippet']['videoId'],
                        Comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                        Comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        Comment_Published_Date=item['snippet']['topLevelComment']['snippet']['publishedAt'])
                
                Comment_data.append(data)       
                        
    except:                                                     #except and pass is to tried code or checked ok to be pass the code
        pass 
    return Comment_data


#get_playlists_details                           #for more than 5 or 50 playlists details

def get_playlists_details(channel_id):                     #def for json format that use in also sql,mngdb



    next_page_token=None

    All_data=[]

    while True:

        request=youtube.playlists().list(
                part="snippet,contentDetails",
                channelId=channel_id,                       #channelId=channel_id is for all playlist videos as 1 by 1
                maxResults=50,
                pageToken=next_page_token
        )

        response=request.execute()

        for item in response['items']:
                data=dict(Playlist_Id=item['id'],
                        Title=item['snippet']['title'],
                        Channel_Id=item['snippet']['channelId'],
                        Channel_Name=item['snippet']['channelTitle'],
                        PublishedAt=item['snippet']['publishedAt'],
                        Video_Count=item['contentDetails']['itemCount']
                        )
                All_data.append(data)
        next_page_token=response.get('nextPageToken')
        if next_page_token is None:
            break
    return All_data 


#upload to mongodb                                    #vsc connecto to mongodb via cloud atlas mngdb

client=pymongo.MongoClient("mongodb+srv://vikramhuggi:vicky@cluster0.3rnkvtx.mongodb.net/?retryWrites=true&w=majority")
db=client['Youtube_data']


def channel_details(channel_id):
    ch_details=get_channel_info(channel_id)
    pl_details=get_playlists_details(channel_id)
    vi_ids=get_videos_ids(channel_id)
    vi_details=get_video_info(vi_ids)
    com_details=get_comment_info(vi_ids)

    coll1=db['channel_details']
    coll1.insert_one({'channel_information':ch_details,'playlists_information':pl_details,
                      'video_information':vi_details,'comment_information':com_details})

    return "Upload Completed Successfully"                  
    
                                                                     #all details at a time in to mongodb

    
#Table creation for channels
def channels_table():
    mydb=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="vikram",
                        database="youtube_data",
                        port="5432")

    cursor=mydb.cursor()

    drop_query='''drop table if exists channels'''         #dropping duplicate channels
    cursor.execute(drop_query)
    mydb.commit()

    try:
        create_query='''create table if not exists channels(channel_name varchar(100) primary key,
                                                            channel_id varchar(80),
                                                            channel_description text,
                                                            channel_publishedat varchar(80),
                                                            channel_playlist_id varchar(80),
                                                            channel_subscribercount bigint,
                                                            channel_videocount int,
                                                            channel_viewcount bigint)'''
        cursor.execute(create_query)
        mydb.commit()
    except:
        print("channels table already created")




    ch_list=[]                                           #for all channel details
    db=client["Youtube_data"]
    coll1=db["channel_details"]

    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    df=pd.DataFrame(ch_list)


    for index,row in df.iterrows():
        insert_query='''insert into channels(channel_name,
                                            channel_id,
                                            channel_description ,
                                            channel_publishedat ,
                                            channel_playlist_id,
                                            channel_subscribercount,
                                            channel_videocount,
                                            channel_viewcount)
                                            
                                            values(%s,%s,%s,%s,%s,%s,%s,%s)'''
        values=(row['channel_name'],
                row['channel_id'],
                row['channel_description'],
                row['channel_publishedat'],
                row['channel_playlist_id'],
                row['channel_subscribercount'],
                row['channel_videocount'],
                row['channel_viewcount'])
        try:
            cursor.execute(insert_query,values)
            mydb.commit()

        except:
            print("channel values are already inserted ")





 

#table creation for videos
def videos_table():
    mydb=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="vikram",
                        database="youtube_data",
                        port="5432")

    cursor=mydb.cursor()

    drop_query='''drop table if exists videos'''         #dropping duplicate channels
    cursor.execute(drop_query)
    mydb.commit()

    create_query='''create table if not exists videos(Channel_Name varchar(100) ,
                                                    Channel_Id varchar(100),
                                                    Video_Id varchar(30) primary Key,
                                                    Title varchar(150),
                                                    Tags text,
                                                    Thumbnail varchar(200),
                                                    Description text,
                                                    Published_Date timestamp,
                                                    Duration interval,
                                                    Veiws bigint,
                                                    likes int,
                                                    Comments int,
                                                    Favorite_count int,
                                                    Definition varchar(10),
                                                    Caption_status varchar(50))'''

                            
    cursor.execute(create_query)
    mydb.commit()

    vi_list=[]                                           #for all channel details
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for vi_data in coll1.find({},{"_id":0,"video_information":1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])
    df2=pd.DataFrame(vi_list)



    for index,row in df2.iterrows():
        insert_query='''insert into videos(Channel_Name,
                                            Channel_Id,
                                            Video_Id,
                                            Title,
                                            Tags,
                                            Thumbnail,
                                            Description,
                                            Published_Date,
                                            Duration,
                                            Veiws,
                                            likes,
                                            Comments,
                                            Favorite_count,
                                            Definition,
                                            Caption_status
                                            )
                                            
                                    values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        
        values=(row['Channel_Name'],
                row['Channel_Id'],
                row['Video_Id'],
                row['Title'],
                row['Tags'],
                row['Thumbnail'],
                row['Description'],
                row['Published_Date'],
                row['Duration'],
                row['Veiws'],
                row['likes'],
                row['Comments'],
                row['Favorite_count'],
                row['Definition'],
                row['Caption_status'])

        cursor.execute(insert_query,values)
        mydb.commit()



#table creation for comments
def comments_table():
    mydb=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="vikram",
                        database="youtube_data",
                        port="5432")

    cursor=mydb.cursor()

    drop_query='''drop table if exists comments'''         #dropping duplicate channels
    cursor.execute(drop_query)
    mydb.commit()

    create_query='''create table if not exists comments(Comment_Id varchar(100) primary Key,
                                                        Video_Id varchar(50),
                                                        Comment_Text text,
                                                        Comment_Author varchar(150),
                                                        Comment_Published_Date timestamp
                                                        )'''

                            
    cursor.execute(create_query)
    mydb.commit()

    com_list=[]                                           #for all channel details
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for com_data in coll1.find({},{"_id":0,"comment_information":1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])
    df3=pd.DataFrame(com_list)

    for index,row in df3.iterrows():
        insert_query='''insert into comments(Comment_Id,
                                                    Video_Id,
                                                    Comment_Text,
                                                    Comment_Author,
                                                    Comment_Published_Date
                                                    )
                                            
                                            values(%s,%s,%s,%s,%s)'''
        
        values=(row['Comment_Id'],
                row['Video_Id'],
                row['Comment_Text'],
                row['Comment_Author'],
                row['Comment_Published_Date'])
        

        cursor.execute(insert_query,values)
        mydb.commit()


#Table creation for playlists                         #for all channel details
def playlist_table():
    mydb=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="vikram",
                        database="youtube_data",
                        port="5432")

    cursor=mydb.cursor()

    drop_query='''drop table if exists playlists'''         #dropping duplicate channels
    cursor.execute(drop_query)
    mydb.commit()

    create_query='''create table if not exists playlists(Playlist_Id varchar(100) primary Key,
                                                        Title varchar(100),
                                                        Channel_Id varchar(100),
                                                        Channel_Name varchar(100),
                                                        PublishedAt timestamp,
                                                        Video_Count int)'''

                            
    cursor.execute(create_query)
    mydb.commit()


pl_list=[]                                           #for all channel details
db=client["Youtube_data"]
coll1=db["channel_details"]

for pl_data in coll1.find({},{"_id":0,"playlists_information":1}):
    for i in range(len(pl_data["playlists_information"])):
        pl_list.append(pl_data["playlists_information"][i])
df1=pd.DataFrame(pl_list)





def tables():
    channels_table()
    videos_table()
    comments_table()

    return "Tables Created Successfully"        


def show_channels_table():
    ch_list=[]       
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    df=st.dataframe(ch_list) 

    return df





def show_videos_table():
    vi_list=[]                                           
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for vi_data in coll1.find({},{"_id":0,"video_information":1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])
    df2=st.dataframe(vi_list)

    return df2


def show_commnets_table():
    com_list=[]                                           
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for com_data in coll1.find({},{"_id":0,"comment_information":1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])
    df3=st.dataframe(com_list)

    return df3


#sreamlit part

with st.sidebar:
    st.title(":green[YOUTUBE DATA HARVESTING AND WHAREHOUSING]")
    st.header("Skill Take Away")
    st.caption("Python Scripting")
    st.caption("Data Collection")
    st.caption("MongoDB")
    st.caption("API Intigration")
    st.caption("Data Management using MongoDB And SQL")
    st.caption("Difficulty Handling  And Thinking")

channel_id=st.text_input("Enter The YouTube channel ID")

if st.button("Collect And Store Data"):
    ch_ids=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_ids.append(ch_data["channel_information"]["channel_id"])

    if channel_id in ch_ids:
        st.success("Channel Details Of The Given Id Is Already Exists")  

    else:
        insert=channel_details(channel_id)
        st.success(insert) 

if st.button("Migrate To SQL"):
    Table=tables()
    st.success(Table)

show_table=st.radio("SELECT THE TABLE FOR VIEW",("CHANNELS","VIDEOS","COMMENTS"))

if show_table=="CHANNELS":
    show_channels_table()




elif show_table=="VIDEOS":
    show_videos_table()

elif show_table=="COMMENTS":
    show_commnets_table()         


#SQL Connection

mydb=psycopg2.connect(host="localhost",
                    user="postgres",
                    password="vikram",
                    database="youtube_data",
                    port="5432")

cursor=mydb.cursor()

question=st.selectbox("select your question",("1. What are the names of all the videos and their corresponding channels?",
                                              "2. Which channels have the most number of videos, and how many videos do they have?",
                                              "3. What are the top 10 most viewed videos and their respective channels?",
                                              "4. How many comments were made on each video, and what are their corresponding video names?",
                                              "5. Which videos have the highest number of likes, and what are their corresponding channel names?",
                                              "6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
                                              "7 .What is the total number of views for each channel, and what are their corresponding channel names?",
                                              "8. What are the names of all the channels that have published videos in the year 2022?",
                                              "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                                              "10. Which videos have the highest number of comments, and what are their corresponding channel names?"))


if question=="1. What are the names of all the videos and their corresponding channels?":
    query1='''select title as videos,channel_name as channelname from videos'''
    cursor.execute(query1)
    mydb.commit()
    t1=cursor.fetchall()
    df1=pd.DataFrame(t1,columns=["video title","channel name"])
    st.write(df1)

elif question=="2. Which channels have the most number of videos, and how many videos do they have?":
    query2='''select channel_name as channelname,channel_videocount as no_videos from channels
                order by channel_videocount desc'''
    cursor.execute(query2)
    mydb.commit()
    t2=cursor.fetchall()
    df2=pd.DataFrame(t2,columns=["channel name","no of videos"])
    st.write(df2)

elif question=="3. What are the top 10 most viewed videos and their respective channels?":
        query3='''select videos.veiws as views,channel_name as channelname,title as videotitle from videos
                    where videos.veiws is not null order by videos.veiws desc limit 10'''
        cursor.execute(query3)
        mydb.commit()
        t3=cursor.fetchall()
        df3=pd.DataFrame(t3,columns=["views","channel name","video title"])
        st.write(df3)
    
elif question=="4. How many comments were made on each video, and what are their corresponding video names?":
    query4='''select comments as no_comments,title as videotitle from videos where comments is not null'''
    cursor.execute(query4)
    mydb.commit()
    t4=cursor.fetchall()
    df4=pd.DataFrame(t4,columns=["no of comments","videotitle"])
    st.write(df4)
    
elif question=="5. Which videos have the highest number of likes, and what are their corresponding channel names?":
    query5='''select title as videotitle,channel_name as channelname,likes as likecount
                from videos where likes is not null order by likes desc'''
    cursor.execute(query5)
    mydb.commit()
    t5=cursor.fetchall()
    df5=pd.DataFrame(t5,columns=["videotitle","channelname","likecount"])
    st.write(df5)

elif question=="6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?":
        query6='''select likes as likecount,title as videotitle from videos'''
        cursor.execute(query6)
        mydb.commit()
        t6=cursor.fetchall()
        df6=pd.DataFrame(t6,columns=["likecount","videotitle"])
        st.write(df6)

elif question=="7 .What is the total number of views for each channel, and what are their corresponding channel names?":
        query7='''select channel_name as channelname,channel_viewcount as totalviews from channels'''
        cursor.execute(query7)
        mydb.commit()
        t7=cursor.fetchall()
        df7=pd.DataFrame(t7,columns=["channel name","totelviews"])
        st.write(df7)
        
elif question=="8. What are the names of all the channels that have published videos in the year 2022?":
        query8='''select title as video_title,published_date as videorelease,channel_name as channelname from videos
                where extract(year from published_date)=2022'''
        cursor.execute(query8)
        mydb.commit()
        t8=cursor.fetchall()
        df8=pd.DataFrame(t8,columns=["videotitle","published_date","channelname"])
        st.write(df8)


elif question=="9. What is the average duration of all videos in each channel, and what are their corresponding channel names?":
        query9='''select channel_name as channelname,AVG(duration) as averageduration from videos group by channel_name'''
        cursor.execute(query9)
        mydb.commit()
        t9=cursor.fetchall()
        df9=pd.DataFrame(t9,columns=["channelname","averageduration"])

        T9=[]                                                               
        for index,row in df9.iterrows():                        
            channel_title=row["channelname"]
            average_duration=row["averageduration"]
            average_duration_str=str(average_duration)
            T9.append(dict(channeltitle=channel_title,avgduration=average_duration_str))
        df1=pd.DataFrame(T9)
        st.write(df1)

 
elif question=="10. Which videos have the highest number of comments, and what are their corresponding channel names?":
        query10='''select title as videotitle,channel_name as channelname,comments as comments from videos where
                comments is not null order by comments desc'''
        cursor.execute(query10)
        mydb.commit()
        t10=cursor.fetchall()
        df10=pd.DataFrame(t10,columns=["video title","channel name","comments"])
        st.write(df10)
                                      

