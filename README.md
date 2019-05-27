# ocr-python-meetup
Python programs involving in the project ocr-k8s-meetup

Those files contains the python scripts embedded in the docker images used in the project ocr-k8s-meetup (a small app analysing the Meetup activities in France).

PREREQUISITES: 
Those scripts have been tested on Python3.6.
Install the following python libraries:
- meetup-api
- kafka
- cassandra-driver

TESTING:
In order to execute the script for testing you must have to:
- run Cassandra
- run Kafka Broker
- Execute the script init-cassandra.py to initialize the keyspace in Cassandra
- Execute meetup-getcities.py and meetup-getupcomingevents.py scripts in order to get the data from Meetup API.
- Execute meetup-getresults.py script to visualize the results.

DEPLOYMENT:
In order to deploy the scripts, please follow the instructions in the project ocr-k8s-meetup

ROADMAP:
- Change the Meetup API key in order to make it configurable.
- Change the hostname for Kafka to a parameter
- Change the hostname for Cassandra to a parameter
