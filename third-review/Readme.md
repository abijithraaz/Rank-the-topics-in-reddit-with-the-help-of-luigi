Rank-the-topics-in-reddit-with-the-help-of-luigi
Solution that tracks and ranks the trending topics in Reddit over time. With help of Luigi Python module that helps to build pipelines.

# To run the project
  Change the configuration and credentials in **config.ini** and follow the below method.

# To run the code locally 
  $ python app.py
  
  once the starting process is completed please give **http://127.0.0.1:8000/** in browser.

# To run the code in docker
Open a terminal from the docker folder in the working directory.

Run the command: docker build –t

**eg: docker build –t subreddit_ranking .**

Run the command to verify that image is created is :> docker images

Then build the project using below command

eg: **docker-compose build** 

To run the project

Give the command '**docker-compose up -d**' in the console.

once the starting process is completed please give '**http://127.0.0.1:8000/**' in browser.
