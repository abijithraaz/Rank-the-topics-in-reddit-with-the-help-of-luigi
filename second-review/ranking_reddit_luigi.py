import praw
import pandas as pd
import os
import time
import luigi
import pickle
import subreddit_rank_support as sr

SUBREDDIT_LIMIT = 50
OUTPUT_POST_LIMIT = 10
OUTPUT_COMMENTS_LIMIT = 5
OUTPUT_DATA_PATH = "data"
OUTPUT_LOG_PATH = "output_log"
POSTS_COLUMNS = ['PostID', 'Subreddit', "Title", 'NoOfComments']
COMMENTS_COLUMNS = ['PostID', 'CommentID', 'Comment', 'CommentScore']

class DataExtraction(luigi.Task):
    """"
    This class is for connecting and storing the datas from reddit
    """
    api_client_id = luigi.Parameter()
    api_client_secret = luigi.Parameter()
    user_agent_name = luigi.Parameter()

    def output(self):
        """
        Returns the target output for this task.
        """
        if not os.path.isdir(OUTPUT_DATA_PATH):
            os.mkdir(OUTPUT_DATA_PATH)

        return luigi.LocalTarget(time.strftime(OUTPUT_DATA_PATH+"/reddit_data%Y_%m_%d.xlsx"))
        
    def run(self):
        """
        This function will run and store the data that we want.
        """
        comment_df_list = []
        post_df_list = []
        subreddit_df_list = []

        reddit = praw.Reddit(client_id = self.api_client_id, client_secret = self.api_client_secret, user_agent = self.user_agent_name)        
        subreddits = reddit.subreddits.popular(limit = SUBREDDIT_LIMIT) # Lists the top 50 subreddits

        for subreddit in subreddits:
            top_posts = reddit.subreddit(str(subreddit)).top()
            for post in top_posts:
                if not post.stickied:
                    post_list = [post.id, str(post.subreddit), post.title, post.num_comments]
                    post.comments.replace_more(limit = 0)
                    for comment in post.comments.list():
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

    api_client_id = luigi.Parameter()
    api_client_secret = luigi.Parameter()
    user_agent_name = luigi.Parameter()

    def requires(self):

        return DataExtraction(self.api_client_id, self.api_client_secret, self.user_agent_name)

    def output(self):
        if not os.path.isdir(OUTPUT_DATA_PATH):
            os.mkdir(OUTPUT_DATA_PATH)

        return [luigi.LocalTarget(time.strftime(OUTPUT_DATA_PATH+"/subreddit_%Y_%m_%d.pkl")), 
                    luigi.LocalTarget(time.strftime(OUTPUT_DATA_PATH+"/posts_%Y_%m_%d.pkl")), 
                        luigi.LocalTarget(time.strftime(OUTPUT_DATA_PATH+"/comments_%Y_%m_%d.pkl"))]
    
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

    api_client_id = luigi.Parameter()
    api_client_secret = luigi.Parameter()
    user_agent_name = luigi.Parameter()

    def requires(self):
        return DataProcessing(self.api_client_id, self.api_client_secret, self.user_agent_name)
    
    def output(self):
        if not os.path.isdir(OUTPUT_LOG_PATH):
            os.mkdir(OUTPUT_LOG_PATH)

        return luigi.LocalTarget(time.strftime(OUTPUT_LOG_PATH+"/subreddit_%Y_%m_%d.xlsx"))

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
        
        post_df_list.reset_index(drop=True,inplace=True)
        comment_df_list.reset_index(drop=True,inplace=True)
        subreddit_df.index += 1
        post_df_list.index += 1
        comment_df_list.index += 1
        output_df = [subreddit_df, post_df_list, comment_df_list]
        sr.save_xlsx(output_df, self.output().path)

if __name__ == "__main__":
    luigi.run()
