from pandas import ExcelWriter
import praw
import reddit_configuration as rc


def reddit_interface():
    """
    function helps to create reddit instance
    """
    reddit_config = rc.reddit_config_load("REDDITCRED")
    reddit = praw.Reddit(
        client_id = reddit_config['API_CLIENT_ID'],
        client_secret = reddit_config['API_CLIENT_SECRET'],
        user_agent = reddit_config['USER_AGENT_NAME'])
    return reddit

def save_xlsx(list_dfs, excel_path):
    """
    Function find the score of the posts.
    """
    with ExcelWriter(excel_path) as writer:
        for n, df in enumerate(list_dfs, 1):
            df.to_excel(writer,'sheet{}'.format(n))
        writer.save()

def getIndexes(dfObj, value):
    """
    Function find the indexe of the value.
    """
    listOfPos = list()
    # Get bool dataframe with True at positions where the given value exists
    result = dfObj.isin([value])
    # Get list of columns that contains the value
    seriesObj = result.any()
    columnNames = list(seriesObj[seriesObj == True].index)
    # Iterate over list of columns and fetch the rows indexes where value exists
    for col in columnNames:
        rows = list(result[col][result[col] == True].index)
        for row in rows:
            listOfPos.append(row)
    # Return a list of tuples indicating the positions of value in the dataframe
    return listOfPos

def post_score_calc(post_df, comment_df):
    """
    Function find the score of the posts.
    """
    post_score = 0.0
    post_score_list = []
    post_rows, post_columns = post_df.shape

    for i in range(post_rows):
        total_score = 0    
        # Get list of index positions i.e. row of all occurrences of ParentID in the dataframe
        listOfPositions = getIndexes(comment_df, post_df["PostID"].iloc[i])
        for j in (listOfPositions):
            # Score of the comment in 4th column
            # Calculate post score with help of comments score
            total_score = (total_score + int(comment_df['CommentScore'].iloc[j]))
        comment_no = post_df["NoOfComments"].iloc[i]
        post_score = (total_score/comment_no)
        post_score_list.append(post_score)
    post_df["Postscore"] = post_score_list
    post_df = post_df.sort_values(by=["Subreddit", "Postscore"], ascending=False)
    post_df.reset_index(drop=True,inplace=True)
    return post_df

def subreddit_score_calc(subreddit_limit, subreddit_df, post_df):
    """
    Function find the score of the subreddits.
    """
    subreddit_score = 0.0
    subreddit_score_list = []

    for i in range(subreddit_limit):
        total_post_score = 0
        post_index_list = getIndexes(post_df, subreddit_df["Subreddit"].iloc[i])
        subreddit_post_len = len(post_index_list)
        for k in (post_index_list):
            # postScore of the post in 5th column
            total_post_score = total_post_score + (post_df["Postscore"].iloc[k])
        subreddit_score = (total_post_score/subreddit_post_len)
        subreddit_score_list.append(subreddit_score)
    subreddit_df["SubredditScore"] = subreddit_score_list
    subreddit_df = subreddit_df.sort_values(by = ["SubredditScore"],ascending=False)
    subreddit_df.reset_index(drop=True,inplace=True)
    return subreddit_df