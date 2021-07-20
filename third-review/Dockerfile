FROM subreddit_ranking

WORKDIR /workspace

COPY app.py ./
COPY ranking_reddit_luigi.py ./
COPY reddit_configuration.py ./
COPY storage.py ./
COPY subreddit_rank_support.py ./
COPY templates ./templates
COPY configuration ./configuration


CMD ["python", "app.py"]
