import mysql.connector as mysql
import reddit_configuration as rc
from luigi.contrib.mysqldb import MySqlTarget

def db_establishment():

    db_config = rc.reddit_config_load("DBCRED")
    db = mysql.connect(
    host=db_config['HOST'],
    user=db_config['USER'],
    password=db_config['PASSWORD'])

    mycursor = db.cursor()
    # Checking the existency of db
    mycursor.execute("SHOW DATABASES")
    db_list = [database[0] for database in mycursor]

    if (db_config['DATABASE'] not in db_list):
        sql = "CREATE DATABASE " + db_config['DATABASE']
        mycursor.execute(sql)
        db.commit()

    mydb = mysql.connect(
    host=db_config['HOST'],
    user=db_config['USER'],
    password=db_config['PASSWORD'],
    database=db_config['DATABASE'])

    mycursor = mydb.cursor()
    mycursor.execute("SHOW TABLES")
    table_list = [table[0] for table in mycursor]

    if (db_config['IDTABLE'] not in table_list):
        sql = "CREATE TABLE " + db_config['IDTABLE'] + ' (IDNO INT AUTO_INCREMENT PRIMARY KEY, DataID VARCHAR(255))'
        mycursor.execute(sql)
        mydb.commit()
    return mydb

DB_CONFIG = rc.reddit_config_load("DBCRED")

class CustomMysql(MySqlTarget):
    def __init__(self, host, database, user, password, table, update_id):
        super().__init__(host, database, user, password, table, update_id)


    def dataid_to_db(self, date_time):
        table = DB_CONFIG['IDTABLE']
        mydb = db_establishment()
        mycursor = mydb.cursor()
        sql = "INSERT INTO "+ table +" (DataID) VALUES ('{}')".format(date_time)
        mycursor.execute(sql)
        mydb.commit()

    def outputsub_to_db(self, df, date_time):
        table = DB_CONFIG['SUBREDDIT_TABLE']
        mydb = db_establishment()
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
    
    def output_posts_to_db(self, df, date_time):
        table = DB_CONFIG['POST_TABLE']
        mydb = db_establishment()

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

    def output_comments_to_db(self, df, date_time):
        table = DB_CONFIG['COMMENT_TABLE']
        mydb = db_establishment()
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
