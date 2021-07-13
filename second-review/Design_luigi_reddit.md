# Design of the luigi pipeline

(1) Writes the output data Task
    (a)requires
        Call the Task for Data processing and Ranking
    (b)output
        A generated output file(excel).
    (c)Run
        > Code logic to write the output data from fame to excel file using openpyxl.

(2) Data Processing and Ranking Task
    (a)requires 
        Call the task for API connection and Storing data into frames.
    (b)output
        Ranked subreddits and its posts and comments.
    (c)run_function
        > Code logic to process the rank of the subreddit.
        > From the given ranking criteria form a code logic to rank the subreddits and its posts and comments.

(3) API connection and Storing data Task
    (a)output
        Stored data frame.
    (b)run_function
        > Connect the reddit API with help of the praw library.
        > Code logic to store the datas into dataframes with help of pandas.
        > Code logic to get top 50 subreddits and its posts and comments.

# Points 
    * Here the task is designed as per the luigi task structure.
    * Scheduling the all these task twice in a day.

