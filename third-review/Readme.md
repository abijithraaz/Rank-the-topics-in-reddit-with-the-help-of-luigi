Rank-the-topics-in-reddit-with-the-help-of-luigi
Solution that tracks and ranks the trending topics in Reddit over time. With help of Luigi Python module that helps to build pipelines.

# To run the project
  Change the configuration and credentials in **config.ini** and follow the below method.
  
  Also please give the host name as **localhost** when code run in local system.

# To run the code locally 
  $ python app.py
  
  once the starting process is completed please give **http://127.0.0.1:5000/** in browser.

# To run the code in docker
Open a terminal from the docker folder in the working directory.

Run the command: docker build –t <name>

**eg: docker build –t subreddit_ranking .**

Run the command to verify that image is created is :> docker images

Please edit the db host ip from localhost to mysql container ip.
  
Then build the project using below command

eg: **docker-compose build** 

To run the project

Give the command '**docker-compose up -d**' in the console.

once the starting process is completed please give '**http://127.0.0.1:5000/**' in browser.
