import os
import pandas as pd
import mysql.connector as mysql
import reddit_configuration as rc

def db_connection():

    db_list =[]
    db_config = rc.reddit_config_load("DBCRED")
    db = mysql.connect(
    host=db_config['HOST'],
    user=db_config['USER'],
    password=db_config['PASSWORD'])

    mycursor = db.cursor()
    # Checking the existency of db
    mycursor.execute("SHOW DATABASES")
    for database in mycursor:
        db_list.append(database[0])
    
    if (db_config['DATABASE'] not in db_list):
        sql = "CREATE DATABASE " + db_config['DATABASE']
        mycursor.execute(sql)
        db.commit()

    mydb = mysql.connect(
    host=db_config['HOST'],
    user=db_config['USER'],
    password=db_config['PASSWORD'],
    database=db_config['DATABASE'])

    return mydb

def output_subreddit_to_db(df, date_time):
    table = "Subreddit_"
    mydb = db_connection()
    mycursor = mydb.cursor()

    table_name = table + date_time
    query = 'CREATE TABLE ' + table_name + ' (Ranks INT AUTO_INCREMENT PRIMARY KEY, Title VARCHAR(255), Score INT)'
    mycursor.execute(query)
    mydb.commit()

    mycursor = mydb.cursor()
    sql = "INSERT INTO "+ table_name +" (Title, Score) VALUES (%s, %s)"
    for ind in df.index:
        gf =(str(df.loc[ind][0]), str(df.loc[ind][1]))
        mycursor.execute(sql, gf)
        mydb.commit()
    
def output_posts_to_db(df, date_time):
    table = "Posts_"
    mydb = db_connection()

    mycursor = mydb.cursor()
    table_name = table + date_time
    query = 'CREATE TABLE ' + table_name + ' (PostId VARCHAR(255), Subreddit VARCHAR(255), Title VARCHAR(255), Postscore VARCHAR(255))'
    mycursor.execute(query)
    mydb.commit()

    mycursor = mydb.cursor()
    sql = "INSERT INTO "+ table_name +" (PostId, Subreddit, Title, Postscore) VALUES (%s, %s, %s, %s)"
    for ind in df.index:
        gf = (str(df.loc[ind][0]), str(df.loc[ind][1]), str(df.loc[ind][2]), str(df.loc[ind][3]))
        mycursor.execute(sql, gf)
        mydb.commit()

def output_comments_to_db(df, date_time):
    table = "Comments_"
    mydb = db_connection()
    mycursor = mydb.cursor()

    table_name = table + date_time
    query = 'CREATE TABLE '+table_name+' (PostId VARCHAR(255), CommentId VARCHAR(255), Comment LONGTEXT, Commentscore VARCHAR(255))'
    mycursor.execute(query)
    mydb.commit()

    mycursor = mydb.cursor()
    sql = "INSERT INTO "+ table_name +" (PostId, CommentId, Comment, Commentscore) VALUES (%s, %s, %s, %s)"
    for ind in df.index:
        gf = (str(df.loc[ind][0]), str(df.loc[ind][1]), str(df.loc[ind][2]), str(df.loc[ind][3]))
        mycursor.execute(sql, gf)
        mydb.commit()


