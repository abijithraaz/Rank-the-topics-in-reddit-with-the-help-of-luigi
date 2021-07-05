# Rank-the-topics-in-reddit-with-the-help-of-luigi
Solution that tracks and ranks the trending topics in Reddit over time. With help of Luigi Python module that helps to build pipelines.

To run the code locally, Please edit the line below line in **subreddit_twice_day.py** by replace the api_client_id and api_client secret arguments.
"os.system('python -m luigi --module subreddit_ranking_with_luigi MyTask --api-client-id **4IPibhzEWutTxA** --api-client-secret **ypLf67zPm1VCacoIvQlbcRBs65-Urw** --local-scheduler')"

please give the commands:> **python subreddit_twice_day.py**
