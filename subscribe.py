# python3.6

import random
from paho.mqtt import client as mqtt_client
from opentsdb import TSDBClient
import logging

#MQTT config
broker = 'localhost'
port = 10000
topic = "test/demo"
client_id = f'python-mqtt-{random.randint(0, 100)}'
#Opentsdb config
logging.basicConfig(level=logging.DEBUG)
opentsdb_server = '127.0.0.1'


def send_to_opentsdb(tsdb_client, data, metric, tag):
    """
    Parameters
    ----------
    tsdb_client : TSDBClinet
        TSDB client initialize
    data : float
        metric data to send
    metric : str
        metric group to send data
    tag : str
        tag of data
    Send metric to opentsdb, for instance if we want to send that the metrics of plant_1 are 12°C,
    we can send data=12, metric="metric.plant_1", tag=Temperature
    """
    tsdb_client.send(metric, data, tag1=tag)
    print(tsdb_client.statuses )


def get_details(msg):
    """
    Parameters
    ----------
    msg : msg
        mesage recive from the mqtt subscribe part
    Extract details of message and prepare them to be send to opentsdb
    """
    msg_str = msg.payload.decode()
    msg_list = msg_str.split(" ")
    metric = "metric." + msg_list[0]
    tag = msg_list[1]
    data = float(msg_list[2])
    return data, metric, tag


def connect_mqtt() -> mqtt_client:
    """
    Create conection to mqtt server, the mode is not define yet
    Returns
    -------
    mqtt_client.Client
        initialize client
    """
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker!")
        else:
            print("Failed to connect, return code %d\n", rc)

    client = mqtt_client.Client(client_id)
    client.on_connect = on_connect
    client.connect(broker, port)
    return client


def subscribe(client: mqtt_client, tsdb_client: TSDBClient):
    def on_message(client, userdata, msg):
        data, metric, tag = get_details(msg)
        send_to_opentsdb(tsdb_client, data, metric, tag)
        print(f"Received `{msg.payload.decode()}` from `{msg.topic}` topic")

    client.subscribe(topic)
    client.on_message = on_message


def run():
    client = connect_mqtt()
    tsdb = TSDBClient(opentsdb_server)
    subscribe(client, tsdb)
    client.loop_forever()
    tsdb.close()
    tsdb.wait()

if __name__ == '__main__':
    run()
