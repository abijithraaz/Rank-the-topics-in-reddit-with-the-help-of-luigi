# -*- coding: utf-8 -*-

import schedule
import time
import os
#small change
  
def job():
    os.system('python -m luigi --module subreddit_ranking_with_luigi MyTask --api-client-id 4IPibhzEWutTxA --api-client-secret ypLf67zPm1VCacoIvQlbcRBs65-Urw --local-scheduler')
 
schedule.every().day.at("10:15").do(job)
schedule.every().day.at("22:15").do(job)
 
while True:
    schedule.run_pending()
    time.sleep(1)