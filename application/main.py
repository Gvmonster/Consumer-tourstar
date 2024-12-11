import stomp
import json

class Queues_Data_Geolocated(stomp.ConnectionListener):

    def on_message(self, data):
        print('\n\nNovo dado recebido:\n\n', data.body)
        self.process_data(data.body)

    def process_data(self, data):
        data = json.loads(data)
        formatted_data = {
        "Indice": data['Indice'],
        "Name": data['Name'],
        "Type": data['Type'],
        "Latitude": data['Latitude'],
        "Longitude": data['Longitude'],
        "Location": data['Location'],
        "WikipediaLink": data['WikipediaLink'],
        "PictureLink": data['PictureLink'],
        "BuildInYear": data['BuildInYear'],
        }

    def close_connection(self):
        self.connect_ActiveMQ.disconnect()

connection_ActiveMQ = stomp.Connection([('localhost', 61613)])  
connection_ActiveMQ.set_listener('', Queues_Data_Geolocated())
connection_ActiveMQ.connect(wait=True)
connection_ActiveMQ.subscribe(destination='/queue/data_geolocated', id=1, ack='auto')
connection_ActiveMQ.close_connection()