FROM subreddit_ranking

WORKDIR /workspace

COPY subreddit_ranking_with_luigi.py ./
COPY subreddit_twice_day.py ./

CMD ["python", "subreddit_twice_day.py"]
