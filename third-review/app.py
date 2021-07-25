from flask import Flask, render_template, request
import storage as reddit_db
from ranking_reddit_luigi import DataOutput
import reddit_configuration as rc
import luigi
import datetime

app = Flask(__name__)
date_hour = datetime.datetime.now()
job_id = date_hour.strftime("%Y_%m_%d_%H")
DB_CONFIG = rc.reddit_config_load("DBCRED")
DATA_ID = ""

@app.route('/')
def reddit_dashboard():
   '''
   Function read the data id from data base.
   '''
   mydb = reddit_db.db_establishment()
   my_cursor = mydb.cursor()
   sql = "SELECT * FROM "+DB_CONFIG['IDTABLE']

   my_cursor.execute(sql)
   data = my_cursor.fetchall()
   data_id_list = []
   for row in data:
      row_list = [x for x in row]
      data_id_list.append(row_list)
   return render_template('reddit_dashboard.html', result = data_id_list)

@app.route('/subreddit', methods = ['POST', 'GET'])
def subreddit():
   '''
   Function read the subreddit data from data base.
   '''
   if request.method == 'POST':
      result = request.form

   id_date = str(result["dataid"])
   global DATA_ID 
   DATA_ID = id_date
   mydb = reddit_db.db_establishment()
   my_cursor = mydb.cursor()
   sql = "SELECT * FROM "+DB_CONFIG['SUBREDDIT_TABLE']+id_date

   my_cursor.execute(sql)
   data = my_cursor.fetchall()
   subreddit_list = []
   for row in data:
      row_list = [x for x in row]
      subreddit_list.append(row_list)
   return render_template('subreddit_gui.html', result = subreddit_list)

@app.route('/posts', methods = ['POST', 'GET'])
def posts():
   '''
   Function read the posts data from data base.
   '''
   if request.method == 'POST':
      result = request.form

   subreddit = (str(result["subreddit"]),)
   mydb = reddit_db.db_establishment()
   my_cursor = mydb.cursor()
   sql = "SELECT * FROM "+DB_CONFIG['POST_TABLE']+DATA_ID+" WHERE Subreddit = %s"
   my_cursor.execute(sql, subreddit)
   data = my_cursor.fetchall()
   subreddit_list = []

   for row in data:
      row_list = [x for x in row]
      subreddit_list.append(row_list)
   return render_template('post_gui.html', result = subreddit_list)

@app.route('/comments', methods = ['POST', 'GET'])
def comments():
   '''
   Function read the comment data from data base.
   '''
   if request.method == 'POST':
      result = request.form
   postid = (str(result["PostID"]),)
   mydb = reddit_db.db_establishment()
   my_cursor = mydb.cursor()
   sql = "SELECT * FROM "+DB_CONFIG['COMMENT_TABLE']+DATA_ID+" WHERE PostId = %s"
   my_cursor.execute(sql, postid)
   data = my_cursor.fetchall()
   subreddit_list = []
   for row in data:
      row_list = [x for x in row]
      subreddit_list.append(row_list)
   return render_template('comments_gui.html', result = subreddit_list)

if __name__ == '__main__':
   reddit_db.db_establishment()
   luigi.build([DataOutput(process_id=date_hour)], local_scheduler=True)
   app.run(host='0.0.0.0')
