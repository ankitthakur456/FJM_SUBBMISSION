import os
import time
import json
import random
import paho.mqtt.client as mqtt
import schedule
from pyModbusTCP.client import ModbusClient
import logging
import datetime
import serial
import serial.tools.list_ports
from database import DBHelper
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

# region Machine params
GL_MACHINE_INFO = {
    'Valve_Assembly1': {
        'pub_topic': 'STG018',
        'sub_topic': 'TRIGGER_STG018',
        'energy_topic': 'ihf_4_em',
        'param_list': ['torque', 'angle', 'NG', 'OK'],
        'ip': '192.168.3.250',
        'machine_id': '01',
        'stage': 'Valve_Assambly',
        'line': 'A',
    },}


GL_MACHINE_NAME = ''  # These variables will be initialized by init_conf
STAGE = ''
LINE = ''
MACHINE_ID = ''
GL_IP = ''
GL_U_ID = 1
GL_PARAM_LIST = []  # These variables will be initialized by init_conf
# endregion

# region MQTT params
MQTT_BROKER1 = 'ec2-13-232-172-215.ap-south-1.compute.amazonaws.com'
MQTT_BROKER = '192.168.33.150'
MQTT_PORT = 1883
USERNAME = 'mmClient'
PASSWORD = 'ind4.0#2023'
GL_CLIENT_ID = f'HIS-MQTT-{random.randint(0, 1000)}'

PUBLISH_TOPIC = ''  # These variables will be initialized by init_conf
TRIGGER_TOPIC = ''  # These variables will be initialized by init_conf
ENERGY_TOPIC = ''  # These variables will be initialized by init_conf
# endregion

ob_db = DBHelper()  # Object for DBHelper database class

# region Program Global Variables
GL_SEND_DATA = True
# endregion
GL_TORQUE = 0
GL_ANGLE = 0
GL_NG = 0
GL_OK = 0
# region Barcode Params
PARITY = serial.PARITY_NONE
STOP_BITS = serial.STOPBITS_ONE
BYTE_SIZE = serial.EIGHTBITS
BAUD_RATE = 9600
# endregion

# region program global variables

GL_PREV_KWH = 0
GL_CURRENT_KWH = 0
GL_MAX_HEATING_TIME = 0
GL_MAX_TEMP = 0
FL_STATUS = False
FL_PREV_STATUS = False
FL_FIRST_CYCLE_RUN = True
GL_SERIAL_NUMBER = ''

# endregion


# region Initialising Configuration here
def init_conf():
    global GL_MACHINE_NAME, GL_PARAM_LIST, PUBLISH_TOPIC, TRIGGER_TOPIC, ENERGY_TOPIC, GL_IP
    global MACHINE_ID, LINE, STAGE
    if not os.path.isdir("./conf"):
        log.info("[-] conf directory doesn't exists")
        try:
            os.mkdir("./conf")
            log.info("[+] Created conf dir successfully")
        except Exception as e:
            pass
            log.error(f"[-] Can't create conf dir Error: {e}")

    try:
        with open('./conf/machine_config.conf', 'r') as f:
            data = f.readline().replace("\n", "")
            data = {data.split('=')[0]: data.split('=')[1]}
            print(data)
            print(type(data))

            GL_MACHINE_NAME = data['m_name']
            GL_PARAM_LIST = GL_MACHINE_INFO[GL_MACHINE_NAME]['param_list']  # para
            PUBLISH_TOPIC = GL_MACHINE_INFO[GL_MACHINE_NAME]['pub_topic']
            TRIGGER_TOPIC = GL_MACHINE_INFO[GL_MACHINE_NAME]['sub_topic']
            ENERGY_TOPIC = GL_MACHINE_INFO[GL_MACHINE_NAME]['energy_topic']
            MACHINE_ID = GL_MACHINE_INFO[GL_MACHINE_NAME]["machine_id"]
            STAGE = GL_MACHINE_INFO[GL_MACHINE_NAME]["stage"]
            LINE = GL_MACHINE_INFO[GL_MACHINE_NAME]["line"]
            GL_IP = GL_MACHINE_INFO[GL_MACHINE_NAME]['ip']
            print(f"[+] Machine_name is {GL_MACHINE_NAME}")
    except FileNotFoundError as e:
        log.error(f'[-] machine_config.conf not found {e}')
        with open('./conf/machine_config.conf', 'w') as f:
            data = "m_name=NO_MACHINE"
            f.write(data)


log.info(f"[+] Initialising configuration")
init_conf()
log.info(f"[+] Machine is {GL_MACHINE_NAME}")
log.info(f"[+] Machine IP is {GL_IP}")
log.info(f"[+] Publish topic is {PUBLISH_TOPIC}")
log.info(f"[+] Trigger topic is {TRIGGER_TOPIC}")


# endregion


# region Modbus Functions

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
        data0 = mb_client.read_holding_registers(1220, 1)  # reading the value of pressure
        data1 = mb_client.read_holding_registers(1224, 1)  # here we are reading cycle time
        data2 = mb_client.read_holding_registers(1210, 1)
        data3 = mb_client.read_holding_registers(1210, 1)
        log.info(f"got torque data as {data0}")
        log.info(f"got before angle data as {data1}")
        log.info(f"got before OK data as {data3}")
        if data1 is None:
            data1 = [0]
        log.info(f"got angle data as {data1}")
        if data2 is None:
            data2 = 0
        log.info(f"got NG data as {data2}")
        if data3 is None:
            data3 = [0]
        log.info(f"got OK data as {data3}")
        v = data0
        values = []
        if v is None:
            log.info(f"[*] Setting values 0")
            for index, keys in enumerate(parameters):
                payload[keys] = 0
        else:
            # we are appending it here because if values were none then it will create an exception
            if data0 is not None:
                v.append(data0 + data1 + data2 + data3)

                values = v[1]
            else:
                values.append(0)
            log.info(f"[+] Got values {values}")
            for index, keys in enumerate(parameters):
                payload[keys] = values[index]
        return payload

    except Exception as e:
        log.error(f"[!] Error reading parameters from machine: {e}")
        return None



# endregion


# region MQTT Functions
def on_message(client_, userdata, message):
    global GL_CURRENT_KWH, GL_SERIAL_NUMBER

    log.info(f"[+] Received message {str(message.payload.decode('utf-8'))}")
    log.info(f"[+] From topic {message.topic}")

    # if got message from the trigger topic run the main function and send the message
    data = json.loads(message.payload)
    log.info(f"[+] Data is {data}")

    if message.topic == TRIGGER_TOPIC:  # if message is from trigger topic for serial number
        if data is not None:
            GL_SERIAL_NUMBER = data.get('serialNumber')
            print(f"serial_number is {GL_SERIAL_NUMBER}")

    elif message.topic == ENERGY_TOPIC:  # if message is from energy meter then update the energy value
        if data is not None:
            GL_CURRENT_KWH = data.get('energy')
            print(f"current kwh is {GL_CURRENT_KWH}")


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        log.info("Connected to MQTT Broker!")
        client.subscribe(PUBLISH_TOPIC)
        client.subscribe(TRIGGER_TOPIC)
        client.subscribe(ENERGY_TOPIC)


    else:
        log.error("Failed to connect, return code %d\n", rc)


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


def publish_values(payload):
    global ob_client_mqtt
    payload_str = json.dumps(payload)
    log.info(f"{payload_str}")

    if GL_SEND_DATA:
        result = [None, None]  # set the result to None
        try:
            result = ob_client_mqtt.publish(PUBLISH_TOPIC, payload_str)  # try to publish the data
        except:  # if publish gives exception
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
            log.info(f"[+] Send `{result}` to topic `{PUBLISH_TOPIC}`")
            sync_data = ob_db.get_sync_data()  # get all the data from the sync payload db
            if sync_data:  # if sync_data present
                for i in sync_data:  # for every payload
                    if i:  # if payload is not empty
                        ts = i.get("ts")  # save timestamp
                        sync_payload = json.dumps(i.get("payload"))
                        sync_result = ob_client_mqtt.publish(PUBLISH_TOPIC, sync_payload)  # send payload
                        if sync_result[0] == 0:  # if payload sent successful remove that payload from db
                            ob_db.clear_sync_data(ts)
                        else:  # else break from the loop
                            log.error("[-] Can't send sync_payload")
                            break
        else:
            log.error(f"[-] Failed to send message to topic {PUBLISH_TOPIC}")
            ob_db.add_sync_data(payload)  # if status is not 0 (ok) then add the payload to the database


# def try_connect_mqtt1():
#     client_mqtt = mqtt.Client(GL_CLIENT_ID)
#     client_mqtt.on_connect = on_connect
#     client_mqtt.on_message = on_message
#     client_mqtt.username_pw_set(USERNAME, PASSWORD)
#     for i in range(5):
#         try:
#             client_mqtt.connect(MQTT_BROKER1, MQTT_PORT, clean_start=mqtt.MQTT_CLEAN_START_FIRST_ONLY, keepalive=60)
#             if client_mqtt.is_connected():
#                 break
#         except Exception as e:
#             log.error(f"[-] Unable to connect to mqtt broker {e}")
#     try:
#         client_mqtt.loop_start()
#     except Exception as e:
#         log.error(f"[-] Error while starting loop {e}")
#     return client_mqtt


# def publish_values1(payload):
#     global ob_client_mqtt1
#     payload_str = json.dumps(payload)
#     log.info(f"{payload_str}")
#
#     if GL_SEND_DATA:
#         result = [None, None]  # set the result to None
#         try:
#             result = ob_client_mqtt1.publish(PUBLISH_TOPIC, payload_str)  # try to publish the data
#         except:  # if publish gives exception
#             try:
#                 ob_client_mqtt1.disconnect()  # try to disconnect the client
#                 log.info(f"[+] Disconnected from Broker")
#                 time.sleep(2)
#             except:
#                 pass
#             if not ob_client_mqtt1.is_connected():  # if client is not connected
#                 log.info(f"[+] Retrying....")
#                 for _ in range(5):
#                     ob_client_mqtt1 = try_connect_mqtt1()  # retry to connect to the broker
#                     time.sleep(1)
#                     if ob_client_mqtt1.is_connected():  # if connected: break
#                         break
#         # result: [0, 1]
#         status = result[0]
#         if status == 0:  # if status is 0 (ok)
#             log.info(f"[+] Send `{result}` to topic `{PUBLISH_TOPIC}`")
#             sync_data = ob_db.get_sync_data()  # get all the data from the sync payload db
#             if sync_data:  # if sync_data present
#                 for i in sync_data:  # for every payload
#                     if i:  # if payload is not empty
#                         ts = i.get("ts")  # save timestamp
#                         sync_payload = json.dumps(i.get("payload"))
#                         sync_result = ob_client_mqtt1.publish(PUBLISH_TOPIC, sync_payload)  # send payload
#                         if sync_result[0] == 0:  # if payload sent successful remove that payload from db
#                             ob_db.clear_sync_data(ts)
#                         else:  # else break from the loop
#                             log.error("[-] Can't send sync_payload")
#                             break
#         else:
#             log.error(f"[-] Failed to send message to topic {PUBLISH_TOPIC}")
#             ob_db.add_sync_data(payload)  # if status is not 0 (ok) then add the payload to the database
# endregion

def get_unknown_serial(line, stage, machine_id):
    stage = "{0:0>4}".format(stage)
    date_ = datetime.datetime.now()
    jd = date_.timetuple().tm_yday
    yr = date_.strftime("%y")
    dt = date_.strftime("%H%M%S")
    return f"U{line}{stage}{machine_id}{yr}{jd}{dt}"


if __name__ == "__main__":
    ob_client_mqtt = try_connect_mqtt()
    #ob_client_mqtt1 = try_connect_mqtt1()
    while True:
        try:
            if GL_SERIAL_NUMBER:
                ob_db.enqueue_serial_number(GL_SERIAL_NUMBER)
                GL_SERIAL_NUMBER = ''
            mb_client = initiate_client(GL_IP, GL_U_ID)
            data = read_values(mb_client, GL_PARAM_LIST)
            if data:
                # log.info(f"[+] Data is {data}")
                GL_TORQUE = data.get('torque')
                log.info(f"torque is {GL_TORQUE}")
                GL_ANGLE = data.get('angle')
                log.info(f"angle is {GL_ANGLE}")
                GL_NG = data.get('NG')
                log.info(f"NG = {GL_NG}")
                GL_OK = data.get('OK')
                log.info(f"OK = {GL_OK}")
                if FL_FIRST_CYCLE_RUN:  # handling reboots and starts of program if this flag is set
                    FL_FIRST_CYCLE_RUN = False  # then initialize the previous values such as prev_kwh and max_temp
                if GL_NG or GL_OK:
                    log.info(f"Fl_status is running")
                    FL_STATUS = True
                else:
                    FL_STATUS = False
                if FL_PREV_STATUS != FL_STATUS:
                    serial_number = ob_db.get_first_serial_number()
                    if serial_number is None:
                        serial_number = 'null'
                        log.info(f"[+] Adding Unknown serial number to queue {serial_number}")
                        ob_db.enqueue_serial_number(serial_number)
                    payload = {"stage": GL_MACHINE_NAME, "timestamp": time.time(), "serialNumber": serial_number}
                    if FL_STATUS:
                        print(payload)
                    if not FL_STATUS:
                        power_consumption = GL_CURRENT_KWH - GL_PREV_KWH
                        if power_consumption < 0:
                            GL_PREV_KWH = GL_CURRENT_KWH
                            power_consumption = 0
                        ob_db.save_running_data(GL_TORQUE, GL_ANGLE, serial_number)
                        payload['data'] = {
                            "Torque": GL_TORQUE,
                            "Angle": GL_ANGLE,
                            "NG": GL_NG,
                            "OK": GL_OK
                        }
                        log.info(payload)
                        publish_values(payload)
                        #publish_values1(payload)
                        ob_db.delete_serial_number(serial_number)
                        GL_TORQUE = 0
                        GL_ANGLE = 0
                        GL_NG = 0
                        GL_OK = 0
                FL_PREV_STATUS = FL_STATUS
            else:
                log.error(f"[-] Machine Disconnected got {data}")
            time.sleep(1)
        except Exception as e:
            time.sleep(5)
            log.error(f"[-] Error in cycle calculation {e}")