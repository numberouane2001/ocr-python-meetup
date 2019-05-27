#!/usr/bin/python3

from cassandra.cluster import Cluster


# Initialize the Cassandra Keyspace necessary to run the full Meetup application.
def main():
    cluster = Cluster(['cassandra'],port=9042)
    cluster = Cluster()
    session = cluster.connect()
    session.execute('CREATE KEYSPACE IF NOT EXISTS test_meetup WITH REPLICATION = { \'class\' : \'SimpleStrategy\', \'replication_factor\' : 3 };;')
    session.set_keyspace('test_meetup')
    # or you can do this instead
    session.execute(
        """
        CREATE TABLE events (
        eventid varchar, 
        eventname varchar, 
        status varchar, 
        country varchar,
        cityid int,
        lon float,
        lat float,
        eventdate timestamp,
        PRIMARY KEY(eventid)) ;
        """
        )
    session.execute(
        """
        CREATE TABLE cities (
        cityid int, 
        country varchar, 
        city varchar,
        fullcityname varchar,
        members int,
        state varchar,
        zipcode varchar,
        lon float,
        lat float,
        address varchar,
        PRIMARY KEY(cityid)) ;
        """
        )

# Initialisation du script Python    
if __name__ == "__main__":
    main()
