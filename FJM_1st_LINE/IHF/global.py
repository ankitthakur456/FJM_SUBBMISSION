#from Global_Melsec import *
import datetime
from pyModbusTCP.client import ModbusClient
import json
from conversions import get_hour, get_shift, word_list_to_long, f_list
import paho.mqtt.client as mqtt
from database import DBHelper
import datetime
from datetime import date
import os
# import requests
import random
import logging
import logging.handlers
from logging.handlers import TimedRotatingFileHandler
import time

# Setting up Rotating file logging
dirname = os.path.dirname(os.path.abspath(__file__))

log_level = logging.INFO

FORMAT = ('%(asctime)-15s %(levelname)-8s %(module)-15s:%(lineno)-8s %(message)s')

logFormatter = logging.Formatter(FORMAT)
log = logging.getLogger()

# checking and creating logs directory here
if not os.path.isdir("./logs"):
    log.info("[-] logs directory doesn't exists")
    try:
        os.mkdir("./logs")
        log.info("[+] Created logs dir successfully")
    except Exception as e:
        log.info(f"[-] Can't create dir logs Error: {e}")

fileHandler = TimedRotatingFileHandler(os.path.join(dirname, f'logs/app_log'),
                                       when='midnight', interval=1)
fileHandler.setFormatter(logFormatter)
fileHandler.suffix = "%Y-%m-%d.log"
log.addHandler(fileHandler)

consoleHandler = logging.StreamHandler()
consoleHandler.setFormatter(logFormatter)
log.addHandler(consoleHandler)
log.setLevel(log_level)

machine_id = "HPDC_670_T"
line_id = "N8_HPS_1"
# broker = "ec2-13-232-172-215.ap-south-1.compute.amazonaws.com"
SEND_DATA = True
HEADERS = {'content-type': 'application/json'}

SAMPLE_RATE = 60
broker = 'ec2-13-232-172-215.ap-south-1.compute.amazonaws.com'
port = 1883
topic = f"STG003"
# generate client ID with pub prefix randomly
client_id = f'HIS-MQTT-{random.randint(0, 1000)}'
USERNAME = 'mmClient'
PASSWORD = 'ind4.0#2023'
c = DBHelper()
log.info(f"[+] Initialised DB")


def on_message(client, userdata, message):
    log.info("received message: ", str(message.payload.decode("utf-8")))


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        log.info("Connected to MQTT Broker!")
    else:
        log.error("Failed to connect, return code %d\n", rc)


#######################################
#
# Here we try to connect to the MQTT Broker with the client ID
# then we pass all the necessary functions such as on_connect
# Then we try to connect to the broker
# if we were unable to connect to the broker we log the error,
# and then we check if we are connected to the broker
# if we are connected:
#       we start the loop
# else:
#       We Log the error
# and after that we return the client object
# We are returning it like this because we need to check on error if we are connected or not
#
# ----------------------------------------

def try_connect_mqtt():
    client_mqtt = mqtt.Client(client_id)
    client_mqtt.on_connect = on_connect
    client_mqtt.username_pw_set(USERNAME, PASSWORD)
    try:
        client_mqtt.connect(broker, port, clean_start=mqtt.MQTT_CLEAN_START_FIRST_ONLY, keepalive=60)
    except Exception as e:
        log.error(f"[-] Unable to connect to mqtt broker {e}")

    try:
        client_mqtt.loop_start()
    except Exception as e:
        log.error(f"[-] Error while starting loop {e}")
    return client_mqtt


def initiate_client(ip, unit_id):
    """returns the modbus client instance"""
    log.info(f'Modbus Client IP: {ip}')
    return ModbusClient(host=ip, port=502, unit_id=unit_id, auto_open=True, auto_close=True, timeout=2)


def read_values(mb_client, parameters):
    """ Reads the parameters from the machine and returns them as a dictionary"""
    try:
        payload = dict()
        # TCP auto connect on modbus request, close after it
        log.info("[+] Reading values from holding registers")
        data0 = mb_client.read_holding_registers(22, 6)  # here we are reading temperature 1 and 2 values and energy
        data1 = mb_client.read_holding_registers(32, 6)  # here we are reading cycle time
        log.info(f"got temp1 and 2 and energy data as {data0}")
        log.info(f"got time data as {data1}")
        data0 = f_list(data0, False)  # here we are reading temperature 1 and 2 values and energy
        data1 = f_list(data1, False)  # here we are reading cycle time
        log.info(f"got temp1 and 2 and energy data as {data0}")
        log.info(f"got time data as {data1}")
        values = data0
        if values is None:
            log.info(f"[*] Setting values 0")
            for index, keys in enumerate(parameters):
                payload[keys] = 0
        else:
            # we are appending it here because if values were none then it will create an exception
            values.append(sum(data1))
            log.info(f"[+] Got values {values}")
            for index, keys in enumerate(parameters):
                payload[keys] = values[index]
        return payload

    except Exception as e:
        log.error(f"[!] Error reading parameters from machine: {e}")
        return None


if __name__ == '__main__':
    client = try_connect_mqtt()
    try:
        while True:
            prev_date, prev_shift, prev_hour = c.get_misc_data()
            today = (datetime.datetime.today() - datetime.timedelta(hours=7, minutes=0)).strftime("%F")
            if prev_date != today:
                c.update_curr_date(today)

            shift = get_shift()
            if prev_shift != shift:
                c.update_curr_shift(get_shift())

            hour_ = datetime.datetime.now().hour
            if prev_hour != hour_:
                c.update_curr_hour(hour_)

            prev_date, prev_shift, prev_hour = c.get_misc_data()

            mb_client = initiate_client('192.168.0.1', 1)
            data = read_values(mb_client,
                               ['pyrometerTemperature', 'pyrometerTemperature2', 'energyConsumption', 'heatingTime'])
            payload = {
                "stage": 'IHF-1',
                "timestamp": time.time(),
                "serialNumber": 'GFSGHF23123',
                "data": [{
                    "pyrometerTemperature": data['heatingTime'],
                    "energyConsumption": data['energyConsumption'],
                    "heatingTime": data.get('heatingTime')
                }]
            }

            payload_str = json.dumps(payload)
            log.info(f"{payload_str}")

            if SEND_DATA:
                result = [None, None]  # set the result to None
                try:
                    result = client.publish(topic, payload_str)  # try to publish the data if publish gives exception
                except:
                    try:
                        client.disconnect()  # try to disconnect the client
                        log.info(f"[+] Disconnected from Broker")
                        time.sleep(2)
                    except:
                        pass
                    if not client.is_connected():  # if client is not connected
                        log.info(f"[+] Retrying....")
                        for _ in range(5):
                            client = try_connect_mqtt()  # retry to connect to the broker
                            time.sleep(1)
                            if client.is_connected():  # if connected: break
                                break
                # result: [0, 1]
                status = result[0]
                if status == 0:  # if status is 0 (ok)
                    log.info(f"[+] Send `{result}` to topic `{topic}`")
                    sync_data = c.get_sync_data()  # get all the data from the sync payload db
                    if sync_data:  # if sync_data present
                        for i in sync_data:  # for every payload
                            if i:  # if payload is not empty
                                ts = i.get("ts")  # save timestamp
                                sync_payload = json.dumps(i.get("payload"))
                                sync_result = client.publish(topic, sync_payload)  # send payload
                                if sync_result[0] == 0:  # if payload sent successful remove that payload from db
                                    c.clear_sync_data(ts)
                                else:  # else break from the loop
                                    log.error("[-] Can't send sync_payload")
                                    break
                else:
                    log.error(f"[-] Failed to send message to topic {topic}")
                    c.add_sync_data(payload)  # if status is not 0 (ok) then add the payload to the database

            time.sleep(SAMPLE_RATE)

    except Exception as e:
        log.error(e)
        # client.unsubscribe("JBMGroup/MachineData")
        client.disconnect()
        time.sleep(5)
        client.loop_stop()
