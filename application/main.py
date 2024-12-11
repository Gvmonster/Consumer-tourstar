import boto3
import pandas as pd
from io import StringIO
import stomp
import json
import time

s3 = boto3.client('s3')

def fileUpload(bucket_name, file_key, s3):
    response = s3.get_object(Bucket=bucket_name, Key=file_key)
    csv_content = response['Body'].read().decode('utf-8')

    csv_buffer = StringIO(csv_content)

    data = pd.read_csv(csv_buffer)

    return data

class Producer_Data_Geo():
    def __init__(self, host, port, data, queue_name):
        self.connect_ActiveMQ = stomp.Connection([(host, port)])
        self.connect_ActiveMQ.connect(wait=True)
        self.data = data  
        self.queue_name = queue_name

    def send_data(self):
        for index, row in self.data.iterrows():
            time.sleep(1) 

            formatted_data = {
                "Indice": index,
                "Name": row['Name'],
                "Type": row['Type'],
                "Latitude": row['Latitude'],
                "Longitude": row['Longitude'],
                "Location": row['Location'],
                "WikipediaLink": row['Wikipedia link'],
                "PictureLink": row['Picture link'],
                "BuildInYear": row['Build in year'],

            }
            self.connect_ActiveMQ.send(body=json.dumps(formatted_data), destination=f"/queue/{self.queue_name}")
            print("Dado enviado!")

    def close_connection(self):
        self.connect_ActiveMQ.disconnect()

class Producer_Data_Tips():
    def __init__(self, host, port, data, queue_name):
        self.connect_ActiveMQ = stomp.Connection([(host, port)])
        self.connect_ActiveMQ.connect(wait=True)
        self.data = data  
        self.queue_name = queue_name

    def send_data(self):
        for index, row in self.data.iterrows():
            time.sleep(1) 

            formatted_data = {
                "Indice": index,
                "MURALHA DA CHINA": row['MURALHA DA CHINA'],
                "PETRA": row['PETRA'],
                "CRISTO REDENTOR": row['CRISTO REDENTOR'],
                "MACHU PICCHU": row['MACHU PICCHU'],
                "CHICHEN ITZA": row['CHICHEN ITZA'],
                "COLISEU": row['COLISEU'],
                "TAJ MAHAL": row['TAJ MAHAL'],
            }
            self.connect_ActiveMQ.send(body=json.dumps(formatted_data), destination=f"/queue/{self.queue_name}")
            print("Dado enviado!")

    def close_connection(self):
        self.connect_ActiveMQ.disconnect()



data_geo = fileUpload('wonders-of-world-data-2024', 'wonders_of_world.csv' , s3)
data_tips = fileUpload('wonders-of-world-data-2024', 'tips_wonders_of_world.csv', s3)

producer_data_geo = Producer_Data_Geo('localhost', 61613, data_geo, 'data_geolocated')  
producer_data_geo.send_data()
producer_data_geo.close_connection()

producer_data_tips = Producer_Data_Tips('localhost', 61613, data_tips, 'tips')  
producer_data_tips.send_data()
producer_data_tips.close_connection()