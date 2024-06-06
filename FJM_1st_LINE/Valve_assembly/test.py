import json
import paho.mqtt.client as mqtt
import time


# MQTT Broker Info
MQTT_BROKER_ADDRESS = "broker.hivemq.com"
MQTT_BROKER_PORT = 1883
MQTT_TOPIC = "PUBLISH_TOPIC"
GL_SEND_DATA = True


# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, reason_code, properties):
    print(f"Connected with result code {reason_code}")
    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    client.subscribe("$SYS/#")


# The callback for when a PUBLISHING message is received from the server.
def on_message(client, userdata, msg):
    print(msg.topic + " " + str(msg.payload))



def try_connect_mqtt1():
    client_mqtt = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client_mqtt.on_connect = on_connect
    client_mqtt.on_message = on_message
    for i in range(5):
        try:
            client_mqtt.connect(MQTT_BROKER_ADDRESS, MQTT_BROKER_PORT, clean_start=mqtt.MQTT_CLEAN_START_FIRST_ONLY, keepalive=60)
            if client_mqtt.is_connected():
                break
        except Exception as e:
            print(f"[-] Unable to connect to mqtt broker {e}")
    try:
        client_mqtt.loop_start()
    except Exception as e:
        print(f"[-] Error while starting loop {e}")
    return client_mqtt


def publish_values1(payload):
    global ob_client_mqtt1
    payload_str = json.dumps(payload)
    print(f"{payload_str}")

    if GL_SEND_DATA:
        result = [None, None]  # set the result to None
        try:
            result = ob_client_mqtt1.publish(MQTT_TOPIC, payload_str, qos=1)  # try to publish the data
        except:  # if publish gives exception
            try:
                ob_client_mqtt1.disconnect()  # try to disconnect the client
                print(f"[+] Disconnected from Broker")
                time.sleep(2)
            except:
                pass
            if not ob_client_mqtt1.is_connected():  # if client is not connected
                print(f"[+] Retrying....")
                for _ in range(5):
                    ob_client_mqtt1 = try_connect_mqtt1()  # retry to connect to the broker
                    time.sleep(1)
                    if ob_client_mqtt1.is_connected():  # if connected: break
                        break


if __name__ == "__main__":
    ob_client_mqtt1 = try_connect_mqtt1()
    while True:
        data = {"serialNumber":"I2320A001","stage":"STG002"}
        publish_values1(data)
        time.sleep(5)