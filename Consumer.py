
import threading
import logging
import time
import json
from kafka import KafkaConsumer, KafkaProducer
import json
import os
from google.cloud import bigquery

class Consumer(threading.Thread):
    daemon = True   
    def run(self):
        client=bigquery.Client()
        locationDataList = []

        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "./config/gcloud/application_default_credentials.json"

        consumer = KafkaConsumer(bootstrap_servers='localhost:29092',
                                 auto_offset_reset='earliest',
                                 group_id=None,
                                 value_deserializer=lambda m: json.loads(m.decode('utf-8')))
       
        # consumer.subscribe(['topic22'])   
        # for message in consumer:
        #     location_data = json.loads(message.value)         
        
        #     sql = "INSERT INTO `psychic-linker-387816.assignment.location` values( '" + location_data['AirportCode'] +"','" + location_data['CountryName'] + "','" + location_data['Region'] + "')"

        #     print(sql)
        #     client.query(sql)
        #     # results = query_job.result()

    
        consumer.subscribe(['topic23'])
        for message in consumer:
            location_data2 = json.loads(message.value)      
            segmentData = location_data2["Segment"] 
            segmentstring = ""
            for i in segmentData:
                segmentstring = segmentstring + "('" +i["DepartureAirportCode"] + "','" + i["ArrivalAirportCode"] + "','" + i["SegmentNumber"] +"','" + i["LegNumber"]+"','"+i["NumberOfPassengers"]+"'),"
                segmentvalue=segmentstring[:-1]
                sql2 = "INSERT INTO `psychic-linker-387816.assignment.transactiontable` values('"+location_data2["UniqueId"]+"','"+location_data2["TransactionDateUTC"]+"','"+location_data2["Itinerary"]+"','"+location_data2["DestinationAirportCode"]+"','"+location_data2["DestinationAirportCode"]+"','"+location_data2["OneWayOrReturn"]+"'," +"["+segmentvalue+"])"
                client.query(sql2)

            
            #sql3="select country from `psychic-linker-387816.assignment.location` where AirportCode IN (SELECT data['OriginAirportCode'] FROM `psychic-linker-387816.assignment.transactionJson) "
            #print(sql3)     #     locationDataList.append(sql)
       #     print(sql)
              
            
            # results = query_job.result()
        # for message in consumer:
        #     transacation_data=json.loads(message.value)
        #     print("2ndjson",transacation_data)	
        

