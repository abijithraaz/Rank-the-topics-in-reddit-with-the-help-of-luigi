from flask import Flask, render_template, request
import mysql.connector as mysql
import storage as db
from ranking_reddit_luigi import DataOutput
import luigi
import datetime

app = Flask(__name__)
date_hour = datetime.datetime.now()
job_id = date_hour.strftime("%Y_%m_%d_%H")

@app.route('/')
def subreddit():
   '''
   Function read the subreddit data from data base.
   '''
   # luigi.build([DataOutput(process_id=time)], local_scheduler=True)

   mydb = db.db_connection()
   my_cursor = mydb.cursor()
   sql = "SELECT * FROM subreddit_"+job_id

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
   mydb = db.db_connection()
   my_cursor = mydb.cursor()
   sql = "SELECT * FROM posts_"+job_id+" WHERE Subreddit = %s"
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
   mydb = db.db_connection()
   my_cursor = mydb.cursor()
   sql = "SELECT * FROM comments_"+job_id+" WHERE PostId = %s"
   my_cursor.execute(sql, postid)
   data = my_cursor.fetchall()
   subreddit_list = []
   for row in data:
      row_list = [x for x in row]
      subreddit_list.append(row_list)
   return render_template('comments_gui.html', result = subreddit_list)

if __name__ == '__main__':
   luigi.build([DataOutput(process_id=date_hour)], local_scheduler=True)
   app.run(debug = True)