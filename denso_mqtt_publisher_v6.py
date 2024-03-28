# -*- coding: utf-8 -*-
"""
Created on Thu Jan 18 13:35:13 2024

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
import warnings

class denso_mqtt_publisher:
    
    def __init__(self):
        print("Please be patient...Setting parameters")
        
        #Ignore Deprecation Wanring
        warnings.filterwarnings("ignore", category=DeprecationWarning) 
        
        #Read from Config file
        config = configparser.ConfigParser()
        config.readfp(open(r'config.txt'))
        
        #Get parameters
        self.device_list = config.get('config', 'device_list')
        self.port = int(config.get('config', 'port'))
        self.broker = config.get('config', 'broker')
        self.agg_interval = config.get('config', 'agg_interval') + " " + "minutes"
        self.send_interval = (int(config.get('config', 'agg_interval'))* 60)
        self.db_address = config.get('config', 'db_address')
        self.db_username = config.get('config', 'db_username')
        self.db_password = config.get('config', 'db_password')
        self.db_name = config.get('config', 'db_name')
        
        #Build Topics from XML file
        self.topic_dict = self.build_topic()
    
    def mqtt_conn(self, run=True):
        '''
        This method is main 
        It is also used to connect and publish to the MQTT broker.
        It calls other methods to build topic, get data, build payload, etc.
        '''
        while(True):
            try:
                
                #Set initial flag, used to verify MQTT broker connection
                flag_connected = 0
                
                # The callback for when the client receives a CONNACK response from the server.
                def on_connect(client, userdata, flag, rc):
                    nonlocal flag_connected
                    
        # =============================================================================
        #             if rc == 0:
        #                 print("Code: " + str(rc) +" ... Connection successful")
        #             elif rc == 1:
        #                 print("Code: " + str(rc) +" ... Connection refused - incorrect protocol version")
        #             elif rc == 2:
        #                 print("Code: " + str(rc) +" ... Connection refused - invalid client identifier")
        #             elif rc == 3:
        #                 print("Code: " + str(rc) +" ... Connection refused - server unavailable")
        #             elif rc == 4:
        #                 print("Code: " + str(rc) +" ... Connection refused - bad username or password")
        #             elif rc == 5:
        #                 print("Code: " + str(rc) +" ... Connection refused - not authorised")
        #             else:
        #                 print("Code: " + str(rc) +" ... Connection refused - unknown reason")
        # =============================================================================
                        
                    #set connection flag    
                    flag_connected = 1
                    print('Broker Connection Satus: ', flag_connected)
                    
                #Callback to be used when there is a disconnection to the MQTT broker        
                def on_disconnect(client, userdata, rc):
                    nonlocal flag_connected
                    #set connection flag
                    flag_connected = 0
                    print('Broker Connection Satus: ', flag_connected)
                              
                # The callback for when a PUBLISH message is received from the server.
                def on_message(client, userdata, msg):
                    print(msg.topic+" "+str(msg.payload))
                    
                #Get Name of Computer
                device_name = socket.gethostname()
                
                #Set client name and callbacks
                client = mqtt_client.Client(device_name, clean_session=True)
                client.on_connect = on_connect
                client.on_disconnect = on_disconnect
                client.on_message = on_message
                
                #Set Last Will Message
                will_topic = "Device_Status/" + device_name
                client.will_set(will_topic, "offline", qos=1, retain=True)
                
                #Set connection parameters
                print('Setting MQTT Connection Parameters...')
                client.connect_async(str(self.broker), self.port, keepalive=60)
                
                #Trying connection
                client.loop_start()
                
                
            #Exception for initial logic of loading MQTT parameters, this is seperate from the main loop exception
            except Exception as error:
                
                # Print Exception and wait 5 seconds to reconnect
                print("An exception occurred:", type(error).__name__, "–", error)
                time.sleep(5) 
                print('Issue occurred on initial start-up')
              
        #Main Loop
        #Keep trying to make connections to mqtt broker, postgres, and sending data
            try:
                
                #Trying connection
                client.loop_start()
                
                print('Connecting to MQTT broker...')              
                
                #Allow time for connection to set flag
                time.sleep(1)
                
                while(True):
                    
                    print('Broker Connection Satus: ', flag_connected)
                        
                    #Runs in test mode
                    if run == False: 
                        
                        #Run when MQTT Broker is connected
                        if flag_connected == 1:
                        
                            #Manually send LWM value to show device is online
                            client.publish(will_topic, "online", qos=1, retain=True)
                            time.sleep(1)
                            print('MQTT broker connection test complete')
                            client.loop_stop(force=False)
                            
                        elif flag_connected == 0:
                            
                            print('MQTT broker did NOT connect successfully..')
                            
                        
                        #return function breaks all loops if test mode runs well
                        return
                        
                    
                    #Run mode    
                    elif run == True:                           
                        
                        #Wait for denso program to collect data at the determined interval
                        print('Collecting Data...')                    
                        time.sleep(self.send_interval)
                        
                        #Build topics from XML file provided by denso data collection tool
                        topic_dict = self.build_topic()
                        
                        #Manually send LWM value to show device is online
                        client.publish(will_topic, "online", qos=1, retain=True)
                        
                        #Retrieve Payload
                        robot_dict = self.build_payload()
                        
                        
                        #Iterate over each robot from dictionary
                        for key, value in robot_dict.items():
                            
                            #Send MQTT message
                            mqtt_output = json.dumps(value, default=str)
                            
                            #Build Topic Based upon Robot name
                            #Get topic from robot name
                            topic = topic_dict[key]  
                            
                            #Publish to server
                            client.publish(topic, mqtt_output, qos=1, retain=True)
                            print("Published to " + topic)  
                            
                        #stop network connection loop
                        client.loop_stop(force=False)
                        time.sleep(1)
                        
                        break

                    
            except Exception as error:
                
                # Print Exception and wait 5 seconds to reconnect
                print("An exception occurred:", type(error).__name__, "–", error)
                time.sleep(5)
                print("Reattempting connections...")
        
    def database_conn(self, run=True):
        ''' 
        This method handles connecting to local AMT postgres database credentials specified
        from config file and disconnecting once query is performed
        ''' 
        #Keep trying database connection until a god connection is made
        while(True):
            try:
                print("Connecting to postgres database...")
                #Attempt Initial Connection to database
                conn = pg.connect(
                    host=str(self.db_address),
                    database=self.db_name,
                    user=self.db_username,
                    password=self.db_password)
                
                #Runs in test mode
                if run == False:                  
                    print("Postgres database connection test complete")              
                    #Close Connection to database
                    conn.close()
                
                #Run mode    
                if run == True:
                    print('Retrieving Data...')
                    #Open a cursor to perform database operations
                    cur = conn.cursor()
                    
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
                            WHERE "Time_Stamp" > (now() - interval %(time)s)
                            GROUP BY "Robot_Name";
                    ''', {'time':self.agg_interval})
                    
                    #Get Column names from query
                    colnames = [desc[0] for desc in cur.description]
                    
                    # Retrieve query results
                    records = cur.fetchall()
                    
                    #Convert to Pandas Dataframe for Processing
                    query_results = pd.DataFrame(data = records, columns = colnames)
                    return records, query_results
                
                    
                    
                
                #If everything goes well break this loop
                break
            
            except Exception as error:
                
                # Print Exception and wait 5 seconds to reconnect
                print("An exception occurred:", type(error).__name__, "–", error)
                time.sleep(5)
                print("Reattempting postgres database connection...")               
            
        
    def build_payload(self):
        ''' 
        This method builds the payload to be sent to MQTT broker.
        This iterates over the query results to build the payload.
        '''
        print('Building payload...')
        
        #Get Data from Postgres database
        records, result = self.database_conn(run=True)
        
        #Check if there is data
        if len(records) == 0:
            print('No data available')
            
        else:
        
            #Build Dictionary to store message for each robot
            robot_dict = {}
            
            #Set current time to be used in message
            now = dt.datetime.now()
            
            #Build MQTT Message
            for i in range(len(records)):     
                    mqtt_message = {
                        "Robot_Name":records[i][0],
                        "Device_Type":"robot",
                        "Time_Stamp":now,
                        "Aggregation_Interval(mins)": 1, 
                        "Joints":[
                            {
                                "Name":"Not Used",
                                },
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
                    robot_dict[records[i][0]] = mqtt_message
            return robot_dict
                
    
    def build_topic(self):
        '''
        This method handles building the MQTT topic that the payload will be sent to
        '''
        print('Mapping to robots...')
        # open the input xml file and read data in form of python dictionary
        while(True):
            try:
                with open(self.device_list) as xml_file:
                    
                    #Read File and Create Dictionary (forces device into a list to get information from later)
                    data_dict = xmltodict.parse(xml_file.read(), force_list=set({'device'}))              
                    
                    #Parse Dictionary and Create List of Devices
                    #Create Info List
                    info_list = ['Group', 'Line', 'Block', 'Name', 'DeviceType']
                    topic_list = []
                    topic_dict = {}
                    
                    #Loop Over Index of Dictionary to build MQTT topic
                    for e in range(len(data_dict['Devices']['device'])):
                        output = []
                        
                        #Loop over info list
                        for i in info_list:
                            output.append(data_dict['Devices']['device'][e][i])
                            
                        #Change device type string to match capitalization requirements in message
                        device_type = ((str(output[4]).capitalize()).replace("r", "R")).replace("c", "C")
                        
                        #Build topic
                        topic_list.append(f'{output[0]}/{output[1]}/{output[2]}/{device_type}/{output[3]}')
                        topic_dict.update({output[3] : f'{output[0]}/{output[1]}/{output[2]}/{device_type}/{output[3]}'})
                return topic_dict
            
            #Capture Exceptions seperately since this method runs in __init__
            except Exception as error:
                
                # Print Exception and wait 5 seconds to reconnect
                print("An exception occurred:", type(error).__name__, "–", error)
                time.sleep(5)
                print("Issue finding robots") 
        

#Run Program if Main Program
if __name__ == '__main__':
    #Call Class
    denso_pub = denso_mqtt_publisher()
    
    #Test Connections
    denso_pub.mqtt_conn(run=False)
    denso_pub.database_conn(run=False)
    
    #Use real data
    denso_pub.mqtt_conn(run=True)
    
        