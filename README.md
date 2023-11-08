# youtube_data_harvesting_and_warehousing
#Overview
Youtube data harvesting project is to allow users to access and analyze data from multiple YouTube channels. The project utilizes SQL, MongoDB, and Streamlit to create a user-friendly application that allows users to retrieve, store, and query YouTube channel and video data.
#Technologies Used
Python
YouTube API
MongoDB compass
MySQL
Streamlit
#Feature of the Application
#Extracting Details
We extract the youtube channel data using Google API.Here youtube channel id has to be provided as a input which extracts all the details of the channels such as channel details,playlist,video and comments of that channel.
#upload to MongoDB
By clicking Upload to MongoDB, all the extracted details from youtube are stored in MongoDB which is NoSQL Database and it stores data in JSON documents.
#Migrate to SQL database
Here by clicking Migrate button, all the channel details which are stored in MongoDB will be migrated to SQL database in a table format such as Channel,Playlist,Video and comments table.
#Analysis using SQL
By using channel details stored in SQL,the results are derived using MySQL queries for the questions about youtube channel.And the results are displayed in streamlit application in the form of tables 
