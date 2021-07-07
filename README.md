# Rank-the-topics-in-reddit-with-the-help-of-luigi
Solution that tracks and ranks the trending topics in Reddit over time. With help of Luigi Python module that helps to build pipelines.

## To run the code locally

Please edit the line below line in **subreddit_twice_day.py** by replace the api_client_id and api_client secret arguments.
"os.system('python -m luigi --module subreddit_ranking_with_luigi MyTask --api-client-id **4IPibhzEWutTxA** --api-client-secret **ypLf67zPm1VCacoIvQlbcRBs65-Urw** --local-scheduler')".

Before run the below command need to create **output_log** folder and give to the below code.

please give the commands:> **python subreddit_twice_day.py**

## **To run the code in docker**

Please edit the line below line in **subreddit_twice_day.py** by replace the api_client_id and api_client secret arguments.
"os.system('python -m luigi --module subreddit_ranking_with_luigi MyTask --api-client-id **4IPibhzEWutTxA** --api-client-secret **ypLf67zPm1VCacoIvQlbcRBs65-Urw** --local-scheduler')".

  Open a terminal from the docker folder in the working directory.
  
  Run the command: docker build –t <base image name>  
                  eg: docker build –t  subreddit_ranking . 
  
  Run the command to verify that image is created is
                  :> docker images
    
  After all this open a terminal from working directory for build.
  
   Run the command: docker build –t <base image name>  
                  eg: docker build –t  subreddit_ranking . 
  
  Before run the below command need to create output_log folder and give to the below code.
  
  Run the command in terminal to run the project.
                :> Docker run –v /host/fullpath:/container/path <base image name> 
                Eg: docker run -v D:/Allianz/output_log:/workspace/output_log subreddit_ranking
## Full Documentation
  The project solution full documentation is in the **README.docx**. Just go through it when you need to know more about the project.
    
