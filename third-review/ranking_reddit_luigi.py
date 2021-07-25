import pandas as pd
import os
import luigi
import pickle
import subreddit_rank_support as sr
import reddit_configuration as rc
from storage import CustomMysql

SUBREDDIT_LIMIT = int((rc.reddit_config_load('OUTPUT'))['SUBREDDIT_LIMIT'])
OUTPUT_POST_LIMIT = int((rc.reddit_config_load('OUTPUT'))['POST_LIMIT'])
OUTPUT_COMMENTS_LIMIT = int((rc.reddit_config_load('OUTPUT'))['COMMENT_LIMIT'])

OUTPUT_DATA_PATH = (rc.reddit_config_load('REDDITPATH'))['OUTPUT_DATA_PATH']
OUTPUT_LOG_PATH = (rc.reddit_config_load('REDDITPATH'))['OUTPUT_LOG_PATH']

POSTS_COLUMNS = ['PostID', 'Subreddit', "Title", 'NoOfComments']
COMMENTS_COLUMNS = ['PostID', 'CommentID', 'Comment', 'CommentScore']

class DataExtraction(luigi.Task):
    """"
    This class is for connecting and storing the datas from reddit
    """
    process_id = luigi.DateHourParameter()

    def output(self):
        """
        Returns the target output for this task.
        """
        if not os.path.isdir(OUTPUT_DATA_PATH):
            os.mkdir(OUTPUT_DATA_PATH)

        return luigi.LocalTarget(self.process_id.strftime(OUTPUT_DATA_PATH+"/reddit_data%Y_%m_%d_%H.xlsx"))
        
    def run(self):
        """
        This function will run and store the data that we want.
        """
        comment_df_list = []
        post_df_list = []
        subreddit_df_list = []

        reddit = sr.reddit_interface()
        subreddits = reddit.subreddits.popular(limit = SUBREDDIT_LIMIT) # Lists the top 50 subreddits

        for subreddit in subreddits:
            top_posts = reddit.subreddit(str(subreddit)).top(limit = 5)
            for post in top_posts:
                if not post.stickied:
                    post_list = [post.id, str(post.subreddit), post.title, post.num_comments]
                    post.comments.replace_more(limit = 0)
                    for comment in post.comments.list()[:5]:
                        comment_list = [str(comment.parent()), comment.id, comment.body, int(comment.score)]
                        comment_df_list.append(comment_list)
                    post_df_list.append(post_list)
            subreddit_df_list.append([str(subreddit)])

        comment_df_list = pd.DataFrame(comment_df_list, columns = COMMENTS_COLUMNS)
        post_df_list = pd.DataFrame(post_df_list, columns = POSTS_COLUMNS)
        subreddit_df_list = pd.DataFrame(subreddit_df_list, columns =['Subreddit'])
        reddit_df = [subreddit_df_list, post_df_list, comment_df_list]
        sr.save_xlsx(reddit_df, self.output().path)
    
class DataProcessing(luigi.Task):
    """
    This task is for processing the data
    """
    process_id = luigi.DateHourParameter()

    def requires(self):

        return DataExtraction(self.process_id)

    def output(self):
        if not os.path.isdir(OUTPUT_DATA_PATH):
            os.mkdir(OUTPUT_DATA_PATH)

        return [luigi.LocalTarget(self.process_id.strftime(OUTPUT_DATA_PATH+"/subreddit_%Y_%m_%d_%H.pkl")), 
                    luigi.LocalTarget(self.process_id.strftime(OUTPUT_DATA_PATH+"/posts_%Y_%m_%d_%H.pkl")), 
                        luigi.LocalTarget(self.process_id.strftime(OUTPUT_DATA_PATH+"/comments_%Y_%m_%d_%H.pkl"))]
    
    def run(self):  
        total_data_df = []
        sorted_data_df = []
        input_file = pd.ExcelFile(self.input().path) # Reading the excel file
        
        for sheet in (input_file.sheet_names):
            data_df = pd.DataFrame(input_file.parse(sheet)).iloc[: , 1:]
            total_data_df.append(data_df)
        subreddit_df, posts_df, comments_df = total_data_df

        comments_df = comments_df.sort_values(by = ["PostID", "CommentScore"], ascending=False)
        comments_df.reset_index(drop=True,inplace=True)
        posts_df = sr.post_score_calc(posts_df, comments_df)
        subreddit_df = sr.subreddit_score_calc(SUBREDDIT_LIMIT,subreddit_df, posts_df)

        sorted_data_df = [subreddit_df, posts_df, comments_df]
        for outpath, data_fram in zip(self.output(), sorted_data_df):
            outfile = open(outpath.path, "wb")
            pickle.dump(data_fram, outfile, protocol=pickle.HIGHEST_PROTOCOL)
            outfile.close()
    
class DataOutput(luigi.Task):
    """
    This task is for storing the output.
    """
    process_id = luigi.DateHourParameter()

    def requires(self):
        return DataProcessing(self.process_id)

    def output(self):
        db_config = rc.reddit_config_load("DBCRED")

        task_id = self.process_id.strftime("%Y_%m_%d_%H")
        return CustomMysql(host=db_config['HOST'], 
                            database=db_config['DATABASE'], 
                            user=db_config['USER'], 
                            password=db_config['PASSWORD'], 
                            table=db_config['IDTABLE'], 
                            update_id= task_id)
    
    def run(self):
        datafram_input = []

        for entry in (self.input()):
            file = open(entry.path, 'rb')
            df = pickle.load(file)
            datafram_input.append(df)

        subreddit_df, post_df, comment_df = datafram_input
        comment_df_list = pd.DataFrame(columns = COMMENTS_COLUMNS)
        post_df_list = pd.DataFrame(columns = POSTS_COLUMNS)

        for subreddit in subreddit_df["Subreddit"]:
            post_index_list = sr.getIndexes(post_df, subreddit)[:OUTPUT_POST_LIMIT]
            for post_index in (post_index_list):
                postid = post_df["PostID"].iloc[post_index]
                comment_index_list = sr.getIndexes(comment_df, postid)[:OUTPUT_COMMENTS_LIMIT]
                for comment_index in (comment_index_list):
                    comment = comment_df.iloc[[comment_index]]
                    comment_df_list = comment_df_list.append(comment)
                post = post_df.iloc[[post_index]]
                post_df_list = post_df_list.append(post)
        
        task_id = self.process_id.strftime("%Y_%m_%d_%H")
        post_df_list.reset_index(drop=True,inplace=True)
        comment_df_list.reset_index(drop=True,inplace=True)

        self.output().dataid_to_db(task_id)
        self.output().outputsub_to_db(subreddit_df, task_id)
        self.output().output_posts_to_db(post_df_list, task_id)
        self.output().output_comments_to_db(comment_df_list, task_id)
        self.output().touch()
