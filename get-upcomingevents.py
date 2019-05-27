#!/usr/bin/python3

import meetup.api
import requests
import json
from time import sleep
from kafka import KafkaProducer

def parseCities(js):
    elems = []
    for elem in js['results']:
        elems.append(elem)
    return elems

def sendCitiesToKafka(city):
    print(city)
    producer = KafkaProducer(bootstrap_servers=['kafka:9092'])
    producer.send("test-meetup-cities", json.dumps(city).encode(), key=str(city["id"]).encode())

# Send the events received from Meetup API to Kafka broker
def getMeetupPerCities(client, cityname):
    elems = client.GetOpenEvents(city=cityname, country='fr', status='upcoming')
    producer = KafkaProducer(bootstrap_servers=['kafka:9092'])
    try:
        events = elems.results
        for event in events:
            producer.send("test-meetup-upcomingevents", json.dumps(event).encode(), key=str(event["id"]).encode())
            sleep(3)
    except Exception as ex:
        print(ex)
    elems = client.GetOpenEvents(city=cityname, country='fr', status='past')
    try:
        events = elems.results
        for event in events:
            producer.send("test-meetup-upcomingevents", json.dumps(event).encode(), key=str(event["id"]).encode())
            sleep(3)
    except Exception as ex:
        print(ex)

#Connect to the Meetup API.
def main():
    client = meetup.api.Client('7867c313d7836251a4062144c2e616b')
    group_info = client.GetGroup({'urlname': 'Meetup-API-Testing'})
    groupid = group_info.id
    while True:
        df = requests.get("https://api.meetup.com/2/cities?&sign=true&photo-host=public&country=fr")
        cities = parseCities(df.json())
        for city in cities:
            #sendCitiesToKafka(city)        
            getMeetupPerCities(client, city['city'])
            sleep(5)
        sleep(5)                    

# Initialisation du script Python    
if __name__ == "__main__":
    main()
