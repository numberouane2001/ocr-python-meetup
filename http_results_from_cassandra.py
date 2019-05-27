#!/usr/bin/env python
 
from http.server import BaseHTTPRequestHandler, HTTPServer
from cassandra.cluster import Cluster
from datetime import datetime, timedelta
from collections import defaultdict
 
# HTTPRequestHandler class
class testHTTPServer_RequestHandler(BaseHTTPRequestHandler):

    #-- Start - Calculating timestamps and datetime for data filtering
    def fromInitDateFilter(self):
        now = datetime.now()
        now = datetime(now.year, now.month, now.day, 0, 0, 0, 0)
        return int(datetime.timestamp(now))

    def fromDateFilter(self,dateref):
        fromDate = dateref - timedelta(days = 2)
        fromDate = datetime(fromDate.year, fromDate.month, fromDate.day, 0, 0, 0, 0)
        return int(datetime.timestamp(fromDate))

    def toDateFilter(self,dateref):
        toDate = dateref - timedelta(days = 1)
        return int(datetime.timestamp(toDate))

    def getDateRef(self):
        now = datetime.now()
        now = datetime(now.year, now.month, now.day, 0, 0, 0, 0)
        dateRef = now - timedelta(days = 30)
        return dateRef

    def getNewDateRef(self,oldDateRef):
        dateRef = oldDateRef + timedelta(days = 1)
        return dateRef

    def transformDate(self,timestamp_s):
        dDate = datetime.fromtimestamp(timestamp_s)
        dDate = datetime(dDate.year, dDate.month, dDate.day, 0, 0, 0, 0)
        return int(datetime.timestamp(dDate))
    
    #End - Calculating timestamps and datetime for data filtering

    #Get the id and the city name from Cassandra to map the cityid in the events with the city name
    def cities(self, session):
        session.set_keyspace('test_meetup')
        rows = session.execute("select cityid, city from cities")
        cities = {}
        for (cityid, city) in rows:
            cities[cityid] = city

        return cities

    #Generate the XML string containing the results of the top 3 most active cities in Meetup France
    def writeFile(self, mon_dictionnaire):
        xml_data = "<events_per_day>"
        for datekey in mon_dictionnaire:
            str_date = datetime.utcfromtimestamp(datekey / 1000).strftime('%Y-%m-%d')
            xml_data += "<day_activity day=\"" + str_date + "\"><rows>"
            ma_liste = mon_dictionnaire[datekey]
            i = 0
            if len(ma_liste) >= 3:
                while i < 3:
                    xml_data += ma_liste[i]
                    i += 1

            xml_data += "</rows></day_activity>"
        xml_data += "</events_per_day>"
        """
        file='/home/damien/Documents/workspace/ooc/Projet6/python/test.xml' 
        with open(file, 'w') as filetowrite:
            filetowrite.write(xml_data)
            filetowrite.close()
        """
        return xml_data

    #Manage the answer of the HTTP server
    def getResults(self):
        cluster = Cluster(['cassandra'],port=9042)
        session = cluster.connect()
        session.set_keyspace('test_meetup')
        my_cities = self.cities(session)
        mon_dictionnaire = {}
        dateRef = self.getDateRef()
        dateFrom = self.fromDateFilter(dateRef) * 1000
        dateTo = self.toDateFilter(dateRef) * 1000
        
        while dateTo <= self.fromInitDateFilter() * 1000:
            rows = session.execute("select eventid,cityid from events where eventdate > %s and eventdate <= %s ALLOW FILTERING ", (str(dateFrom), str(dateTo)))
            d = defaultdict(int)
            for (eventid,cityid) in rows:
                my_city = cityid
                if cityid in my_cities:
                    my_city = my_cities[cityid]
                d[my_city] += 1
            ma_liste = []
            for w in sorted(d, key=d.get, reverse=True):
                ma_liste.append("<row city=\"" + str(w) + "\" nbevents=\"" + str(d[w]) + "\"></row>")
            mon_dictionnaire[dateFrom] = ma_liste
            dateRef = self.getNewDateRef(dateRef)
            dateFrom = self.fromDateFilter(dateRef) * 1000
            dateTo = self.toDateFilter(dateRef) * 1000
        return self.writeFile(mon_dictionnaire)

  # GET
    def do_GET(self):
        # Send response status code
        self.send_response(200)
 
        # Send headers
        self.send_header('Content-type','text/xml')
        self.end_headers()
 
        # Send message back to client
        message = self.getResults()#"Hello world!"
        # Write content as utf-8 data
        self.wfile.write(bytes(message, "utf8"))
        return
#Start the Python HTTP server process
def run():
    print('starting server...')
    # Server settings
    # Choose port 8080, for port 80, which is normally used for a http server, you need root access
    server_address = ('0.0.0.0', 8081)
    httpd = HTTPServer(server_address, testHTTPServer_RequestHandler)
    print('running server...')
    httpd.serve_forever()
 
 

def main():
    run()
# Initialisation du script Python    
if __name__ == "__main__":
    main()
