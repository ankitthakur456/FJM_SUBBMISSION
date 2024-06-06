import os
import time
import json
import random
import paho.mqtt.client as mqtt
import schedule
import logging
import datetime
import minimalmodbus
import serial
import serial.tools.list_ports
import logging.handlers
from logging.handlers import TimedRotatingFileHandler
from conversions import word_list_to_long, f_list, decode_ieee

# region Rotating Logs
dirname = os.path.dirname(os.path.abspath(__file__))

log_level = logging.INFO

FORMAT = ('%(asctime)-15s %(levelname)-8s %(module)-15s:%(lineno)-8s %(message)s')

logFormatter = logging.Formatter(FORMAT)
log = logging.getLogger("LOGS")

# checking and creating logs directory here
if not os.path.isdir("./logs"):
    log.info("[-] logs directory doesn't exists")
    try:
        os.mkdir("./logs")
        log.info("[+] Created logs dir successfully")
    except Exception as e:
        log.error(f"[-] Can't create dir logs Error: {e}")

fileHandler = TimedRotatingFileHandler(os.path.join(dirname, f'logs/app_log'),
                                       when='midnight', interval=1)

# fileHandler = TimedRotatingFileHandler(f"D:/HIS_LOGS", when='midnight', interval=1)
fileHandler.setFormatter(logFormatter)
fileHandler.suffix = "%Y-%m-%d.log"
log.addHandler(fileHandler)

consoleHandler = logging.StreamHandler()
consoleHandler.setFormatter(logFormatter)
log.addHandler(consoleHandler)
log.setLevel(log_level)
# endregion


machine_info = {
    'ihf_1_em': {
        'type_': '6400NG+',
        'unitId': 1,
        'pName': [
            'energy',
        ],
    },
    'ihf_2_em': {
        'type_': '6400NG+',
        'unitId': 2,
        'pName': [
            'energy',
        ],
    }
}

# region MQTT params
MQTT_BROKER = 'ec2-13-232-172-215.ap-south-1.compute.amazonaws.com'
MQTT_PORT = 1883
USERNAME = 'mmClient'
PASSWORD = 'ind4.0#2023'
GL_CLIENT_ID = f'HIS-MQTT-{random.randint(0, 1000)}'

PUBLISH_TOPIC = ''               # These variables will be initialized by init_conf
TRIGGER_TOPIC = ''             # These variables will be initialized by init_conf
ENERGY_TOPIC = ''         # These variables will be initialized by init_conf
# endregion

# region Global Variables
GL_SEND_DATA = True
SAMPLE_RATE = 5

# endregion


# region Modbus functions
def initiate(slaveId):
    com_port = None
    for i in range(5):
        try:
            ports = serial.tools.list_ports.comports()
            usb_ports = [p.device for p in ports if "USB" in p.description]
            log.info(usb_ports)
            com_port = usb_ports[0]
            break
        except Exception as e:
            log.info(f"[-] Error Can't Open Port {e}")
            com_port = None
            time.sleep(1)

    i = int(slaveId)
    instrument = minimalmodbus.Instrument(com_port, i)
    instrument.serial.baudrate = 19200
    instrument.serial.bytesize = 8
    instrument.serial.parity = serial.PARITY_EVEN
    instrument.serial.stopbits = 1
    instrument.serial.timeout = 3
    instrument.serial.close_after_each_call = True
    log.info(f'Modbus ID Initialized: {i}')
    return instrument


def get_em_values(unitId, type_):
    mb_client = initiate(unitId)
    if type_ == '6400NG+':  # DONE
        for i in range(2):
            try:
                register_data = f_list(mb_client.read_registers(2701, 2, 3), True)
            except Exception as e:
                log.error(f"ERROR:{e}")
                register_data = []
                time.sleep(i / 10)
            if register_data:
                log.info(register_data)
                return register_data

    return None
# endregion


# region MQTT Functions
def on_message(client_, userdata, message):
    pass


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        log.info("Connected to MQTT Broker!")
        for machine_topic, m_info in machine_info.items():
            client.subscribe(machine_topic)
    else:
        log.error(f"Failed to connect, return code {rc}\n")


def try_connect_mqtt():
    client_mqtt = mqtt.Client(GL_CLIENT_ID)
    client_mqtt.on_connect = on_connect
    client_mqtt.on_message = on_message
    client_mqtt.username_pw_set(USERNAME, PASSWORD)
    for i in range(5):
        try:
            client_mqtt.connect(MQTT_BROKER, MQTT_PORT, clean_start=mqtt.MQTT_CLEAN_START_FIRST_ONLY, keepalive=60)
            if client_mqtt.is_connected():
                break
        except Exception as e:
            log.error(f"[-] Unable to connect to mqtt broker {e}")
    try:
        client_mqtt.loop_start()
    except Exception as e:
        log.error(f"[-] Error while starting loop {e}")
    return client_mqtt


def publish_values(payload, topic):
    global ob_client_mqtt
    payload_str = json.dumps(payload)
    log.info(f"{payload_str}")

    if GL_SEND_DATA:
        result = [None, None]  # set the result to None
        try:
            result = ob_client_mqtt.publish(topic, payload_str)  # try to publish the data
        except:       # if publish gives exception
            try:
                ob_client_mqtt.disconnect()  # try to disconnect the client
                log.info(f"[+] Disconnected from Broker")
                time.sleep(2)
            except:
                pass
            if not ob_client_mqtt.is_connected():  # if client is not connected
                log.info(f"[+] Retrying....")
                for _ in range(5):
                    ob_client_mqtt = try_connect_mqtt()  # retry to connect to the broker
                    time.sleep(1)
                    if ob_client_mqtt.is_connected():  # if connected: break
                        break
        # result: [0, 1]
        status = result[0]
        if status == 0:  # if status is 0 (ok)
            log.info(f"[+] Send `{result}` to topic `{topic}`")
        else:
            log.error(f"[+] Error while sending `{result}` to topic `{topic}`")
        #     sync_data = ob_db.get_sync_data()  # get all the data from the sync payload db
        #     if sync_data:  # if sync_data present
        #         for i in sync_data:  # for every payload
        #             if i:  # if payload is not empty
        #                 ts = i.get("ts")  # save timestamp
        #                 sync_payload = json.dumps(i.get("payload"))
        #                 sync_result = ob_client_mqtt.publish(PUBLISH_TOPIC, sync_payload)        # send payload
        #                 if sync_result[0] == 0:         # if payload sent successful remove that payload from db
        #                     ob_db.clear_sync_data(ts)
        #                 else:  # else break from the loop
        #                     log.error("[-] Can't send sync_payload")
        #                     break
        # else:
        #     log.error(f"[-] Failed to send message to topic {PUBLISH_TOPIC}")
        #     ob_db.add_sync_data(payload)  # if status is not 0 (ok) then add the payload to the database


# endregion


if __name__ == '__main__':
    try:
        ob_client_mqtt = try_connect_mqtt()
        while True:
            data = None
            try:
                for machine_topic, m_info in machine_info.items():
                    data = get_em_values(m_info['unitId'], m_info['type_'])
                    try:
                        if data is None:
                            pass
                        else:
                            payload = dict()
                            for i, pName in enumerate(m_info['pName']):
                                payload[pName] = data[i]
                            log.info(payload)
                            if payload:
                                publish_values(payload, machine_topic)
                    except Exception as e:
                        log.error(f"[-] Error while sending payload {e}")
            except Exception as e:
                log.error(f"[-] Error while getting data from payload {e}")
            time.sleep(SAMPLE_RATE)
    except Exception as e:
        log.error(f"[-] Error while running program: {e}")

