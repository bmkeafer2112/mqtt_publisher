# -*- coding: utf-8 -*-
"""
Created on Fri Mar 10 09:50:37 2023

@author: AMTAdmin
"""

import psycopg2 as pg
import json
import xmltodict
import time
import pandas as pd
from paho.mqtt import client as mqtt_client
import datetime as dt
import configparser
import socket

#Read in Variables from Config File (keep in same directory as executable)
#from denso_mqtt_publisher_config import *

#Read in from Config File
config = configparser.ConfigParser()
config.readfp(open(r'config.txt'))
device_list = config.get('config', 'device_list')
port = int(config.get('config', 'port'))
broker = config.get('config', 'broker')
agg_interval = config.get('config', 'agg_interval') + " " + "minutes"
send_interval = (int(config.get('config', 'agg_interval'))* 60)


#Get Name of Computer
device_name = socket.gethostname()

#Check Connections to servers
while(True):
    try: 
        # Connect to your postgres DB
        conn = pg.connect("dbname=postgres user=postgres password=admin")
        
        # Open a cursor to perform database operations
        cur = conn.cursor()
        
        print("Connected to database")
        
        #MQTT Connection       
        client = mqtt_client.Client(device_name)
        #Set Last Will & Testament Message (offline)
        lwm = device_name + ": offline"
        will_topic = "Device_Status/" + device_name
        client.will_set(will_topic, lwm, qos=1, retain=True)
        
        
        #Start Connection Loop (handles reconnection)
        client.connect_async(broker, port, keepalive=120)
        client.loop_start()
        #time.sleep(5)
        
        #Device is online status to counter the last will message
        online = device_name + ": active"
        client.publish(will_topic, online, qos=1, retain=True)
        print("Connected to MQTT Server")
        break
    except:
        time.sleep(5)
        print("Attempting connections...")
        continue


# open the input xml file and read
# data in form of python dictionary
# using xmltodict module
with open(device_list) as xml_file:
    #Read File and Create Dictionary 
    data_dict = xmltodict.parse(xml_file.read())
    print(data_dict)
    time.sleep(10)
    #Parse Dictionary and Create List of Devices
    #Create Info List
    info_list = ['Group', 'Line', 'Block', 'Name']
    topic_list = []
    topic_dict = {}
    
    #Loop Over Index of Dictionary to build MQTT topic
    for e in range(len(data_dict['Devices']['device'])):
        print(e)
        print(len(data_dict['Devices']['device']))
        print((data_dict['Devices']['device']))
        time.sleep(10)
        output = []
        #Loop over info list
        for i in info_list:
            print(i)
            time.sleep(10)
            output.append(data_dict['Devices']['device'][e][i])
        topic_list.append(f'{output[0]}/{output[1]}/{output[2]}/Robot/{output[3]}')
        topic_dict.update({output[3] : f'{output[0]}/{output[1]}/{output[2]}/Robot/{output[3]}'})
        

######Query for Aggregate Data
while(True):
    #Start Connection Loop (handles reconnection)
    #client.loop_start()
    start = time.time()
    #Update Status of device to online
    client.publish(will_topic, online, qos=1, retain=True,)
    print("Collecting Data...")
    time.sleep(send_interval)

    
    now = dt.datetime.now()
    # Execute a query
    try:
        # Execute a query
        cur.execute('''    
                SELECT 
                "Robot_Name",
                AVG("Torque_1"), MAX("Torque_1"), MIN("Torque_1"), STDDEV("Torque_1"), COUNT("Torque_1"),
                AVG("Amp_1"), MAX("Amp_1"), MIN("Amp_1"), STDDEV("Amp_1"), COUNT("Amp_1"),
                AVG("load_factor_1"), MAX("load_factor_1"), MIN("load_factor_1"), STDDEV("load_factor_1"), COUNT("load_factor_1"),
                AVG("Torque_2"), MAX("Torque_2"), MIN("Torque_2"), STDDEV("Torque_2"), COUNT("Torque_2"),
                AVG("Amp_2"), MAX("Amp_2"), MIN("Amp_2"), STDDEV("Amp_2"), COUNT("Amp_2"),
                AVG("load_factor_2"), MAX("load_factor_2"), MIN("load_factor_2"), STDDEV("load_factor_2"), COUNT("load_factor_2"),
                AVG("Torque_3"), MAX("Torque_3"), MIN("Torque_3"), STDDEV("Torque_3"), COUNT("Torque_3"),
                AVG("Amp_3"), MAX("Amp_3"), MIN("Amp_3"), STDDEV("Amp_3"), COUNT("Amp_3"),
                AVG("load_factor_3"), MAX("load_factor_3"), MIN("load_factor_3"), STDDEV("load_factor_3"), COUNT("load_factor_3"),
                AVG("Torque_4"), MAX("Torque_4"), MIN("Torque_4"), STDDEV("Torque_4"), COUNT("Torque_4"),
                AVG("Amp_4"), MAX("Amp_4"), MIN("Amp_4"), STDDEV("Amp_4"), COUNT("Amp_4"),
                AVG("load_factor_4"), MAX("load_factor_4"), MIN("load_factor_4"), STDDEV("load_factor_4"), COUNT("load_factor_4"),
                AVG("Torque_5"), MAX("Torque_5"), MIN("Torque_5"), STDDEV("Torque_5"), COUNT("Torque_5"),
                AVG("Amp_5"), MAX("Amp_5"), MIN("Amp_5"), STDDEV("Amp_5"), COUNT("Amp_5"),
                AVG("load_factor_5"), MAX("load_factor_5"), MIN("load_factor_5"), STDDEV("load_factor_5"), COUNT("load_factor_5"),
                AVG("Torque_6"), MAX("Torque_6"), MIN("Torque_6"), STDDEV("Torque_6"), COUNT("Torque_6"),
                AVG("Amp_6"), MAX("Amp_6"), MIN("Amp_6"), STDDEV("Amp_6"), COUNT("Amp_6"),
                AVG("load_factor_6"), MAX("load_factor_6"), MIN("load_factor_6"), STDDEV("load_factor_6"), COUNT("load_factor_6"),                
                Now()
                FROM public.everything
                WHERE "Time_Stamp" > now() - interval %s
                GROUP BY "Robot_Name";
        ''', (agg_interval,))
          
        #Get Column names from query
        colnames = [desc[0] for desc in cur.description]
        
        # Retrieve query results
        records = cur.fetchall()
        
        
    #Try/Exception Procedure to re-connect to database
    except:
        try: 
            #Get connection
            conn = pg.connect("dbname=postgres user=postgres password=admin")
            cur = conn.cursor()
            
        #Keep trying to connect by going to top of loop
        except pg.Error:
            print ("Trying Database Connection...")
            continue
    
    #Convert to Pandas Dataframe for Processing
    data = pd.DataFrame(data = records, columns = colnames)
    
    #Build MQTT Message
    for i in range(len(records)):     
            mqtt_message = {
                "Robot_Name":records[i][0],
                "Device_Type":"robot",
                "Time_Stamp":now,
                "Aggregation_Interval(mins)": 1, 
                "Joints":[
                    {
                        "Name":"joint_1",
                        "amps":{
                            "UOM":"Percentage_of_load",                          
                            "avg":records[i][6],
                            "max":records[i][7],
                            "min":records[i][8],
                            "stddev":records[i][9],
                            "count":records[i][10]
                            
                            },
                        "Torque":{
                            "UOM":"percentage_of_load",
                            "avg":records[i][1],
                            "max":records[i][2],
                            "min":records[i][3],
                            "stddev":records[i][4],
                            "count":records[i][5]
                            },
                        
                        "load_factor":{
                            "UOM":"percentage_of_load",
                            "avg":records[i][11],
                            "max":records[i][12],
                            "min":records[i][13],
                            "stddev":records[i][14],
                            "count":records[i][15]
                            },
                        
                        },
                    {
                        "Name":"joint_2",
                        "amps":{
                            "UOM":"Percentage_of_load",                          
                            "avg":records[i][21],
                            "max":records[i][22],
                            "min":records[i][23],
                            "stddev":records[i][24],
                            "count":records[i][25]
                            
                            },
                        "Torque":{
                            "UOM":"percentage_of_load",
                            "avg":records[i][16],
                            "max":records[i][17],
                            "min":records[i][18],
                            "stddev":records[i][19],
                            "count":records[i][20]
                            },
                        
                        "load_factor":{
                            "UOM":"percentage_of_load",
                            "avg":records[i][26],
                            "max":records[i][27],
                            "min":records[i][28],
                            "stddev":records[i][29],
                            "count":records[i][30]
                            },
                                           
                        },
                    {
                        "Name":"joint_3",
                        "amps":{
                            "UOM":"Percentage_of_load",                          
                            "avg":records[i][36],
                            "max":records[i][37],
                            "min":records[i][38],
                            "stddev":records[i][39],
                            "count":records[i][40]
                            
                            },
                        "Torque":{
                            "UOM":"percentage_of_load",
                            "avg":records[i][31],
                            "max":records[i][32],
                            "min":records[i][33],
                            "stddev":records[i][34],
                            "count":records[i][35]
                            },
                        
                        "load_factor":{
                            "UOM":"percentage_of_load",
                            "avg":records[i][41],
                            "max":records[i][42],
                            "min":records[i][43],
                            "stddev":records[i][44],
                            "count":records[i][45]
                            },
                                           
                        },
                    {
                        "Name":"joint_4",
                        "amps":{
                            "UOM":"Percentage_of_load",                          
                            "avg":records[i][51],
                            "max":records[i][52],
                            "min":records[i][53],
                            "stddev":records[i][54],
                            "count":records[i][55]
                            
                            },
                        "Torque":{
                            "UOM":"percentage_of_load",
                            "avg":records[i][46],
                            "max":records[i][47],
                            "min":records[i][48],
                            "stddev":records[i][49],
                            "count":records[i][50]
                            },
                        
                        "load_factor":{
                            "UOM":"percentage_of_load",
                            "avg":records[i][56],
                            "max":records[i][57],
                            "min":records[i][58],
                            "stddev":records[i][59],
                            "count":records[i][60]
                            },
                                           
                        },
                    {
                        "Name":"joint_5",
                        "amps":{
                            "UOM":"Percentage_of_load",                          
                            "avg":records[i][66],
                            "max":records[i][67],
                            "min":records[i][68],
                            "stddev":records[i][69],
                            "count":records[i][70]
                            
                            },
                        "Torque":{
                            "UOM":"percentage_of_load",
                            "avg":records[i][61],
                            "max":records[i][62],
                            "min":records[i][63],
                            "stddev":records[i][64],
                            "count":records[i][65]
                            },
                        
                        "load_factor":{
                            "UOM":"percentage_of_load",
                            "avg":records[i][71],
                            "max":records[i][72],
                            "min":records[i][73],
                            "stddev":records[i][74],
                            "count":records[i][75]
                            },
                                           
                        },
                    {
                        "Name":"joint_6",
                        "amps":{
                            "UOM":"Percentage_of_load",                          
                            "avg":records[i][81],
                            "max":records[i][82],
                            "min":records[i][83],
                            "stddev":records[i][84],
                            "count":records[i][85]
                            
                            },
                        "Torque":{
                            "UOM":"percentage_of_load",
                            "avg":records[i][76],
                            "max":records[i][77],
                            "min":records[i][78],
                            "stddev":records[i][79],
                            "count":records[i][80]
                            },
                        
                        "load_factor":{
                            "UOM":"percentage_of_load",
                            "avg":records[i][86],
                            "max":records[i][87],
                            "min":records[i][88],
                            "stddev":records[i][89],
                            "count":records[i][90]
                            }
                                           
                        }
                    ]
                }                     
        
            #Send MQTT message
            mqtt_output = json.dumps(mqtt_message, default=str)
            #Build Topic Based upon Robot name
            #Call Dictionary by Robot name to get topic for publishing
            key = records[i][0]
            topic = topic_dict[key]
            #topic = f"NewTopic/Robot/{records[i][0]}"
            print(topic)
        
            client.publish(topic, mqtt_output, qos=1, retain=True)
            print("Published")
            #print(mqtt_output)
            #end = time.time()
            #print(end, start)
            #time.sleep(send_interval-(end-start))
