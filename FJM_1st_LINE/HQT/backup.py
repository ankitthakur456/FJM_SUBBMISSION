import os
import time
import json
import snap7
import random
import paho.mqtt.client as mqtt
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
    'HQT': {
        'pub_topic': 'STG011',
        'sub_topic': 'TRIGGER_STG011',
        'param_list': ["hardeningTemperatureZ1", "hardeningTemperatureZ2", "quenchingTemperature",
                       "temperingTemperatureZ1", "temperingTemperatureZ2", "temperingTemperatureZ3", "cycleTime"],
        'ip': '192.168.0.1',
        'machine_id': '01',
        'stage': 'HQT',
        'line': 'A',
    }}

GL_MACHINE_NAME = ''  # These variables will be initialized by init_conf
STAGE = ''
LINE = ''
MACHINE_ID = ''
GL_IP = ''
GL_U_ID = 1
GL_PARAM_LIST = []  # These variables will be initialized by init_conf
# endregion

# region MQTT params
#MQTT_BROKER = 'ec2-13-232-172-215.ap-south-1.compute.amazonaws.com'
MQTT_BROKER1 = '192.168.33.150'
MQTT_PORT = 1883
USERNAME = 'mmClient'
PASSWORD = 'ind4.0#2023'
GL_CLIENT_ID = f'HIS-MQTT-{random.randint(0, 1000)}'

plc_ip = " "
rack_no = " "
slot_no = " "
tcp_port = " "

PUBLISH_TOPIC = ''  # These variables will be initialized by init_conf
TRIGGER_TOPIC = ''  # These variables will be initialized by init_conf
GL_SERIAL_TOPIC = 'Acknowledgements'
# endregion

ob_db = DBHelper()  # Object for DBHelper database class

# region Program Global Variables
GL_SEND_DATA = True
# endregion

# region Barcode Params
PARITY = serial.PARITY_NONE
STOP_BITS = serial.STOPBITS_ONE
BYTE_SIZE = serial.EIGHTBITS
BAUD_RATE = 9600
# endregion

# region program global variables
number_of_cylinders = 0
GL_MAX_HARDENING_TEMPRATURE_Z1 = 0
GL_MAX_HARDENING_TEMPRATURE_Z2 = 0
GL_MAX_QUENCHING_TEMPRATURE = 0
GL_MAX_TEMPERING_TEMPRATURE_Z1 = 0
GL_MAX_TEMPERING_TEMPRATURE_Z2 = 0
GL_MAX_TEMPERING_TEMPRATURE_Z3 = 0
GL_PREV_DOOR_STATUS = False
GL_MAX_HEATING_TIME = 0
GL_MAX_TEMP = 0
FL_STATUS = False
FL_PREV_STATUS = False
FL_FIRST_CYCLE_RUN = True
GL_SERIAL_NUMBER = ''
GL_CYCLE_START_TIME = time.time()
GL_CYCLE_STOP_TIME = time.time()
GL_PREV_CYCLE_TIME = 0
GL_QUENCHING_TIME = 0

FL_machine_status = False

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

def initiate(client):
    try:
        client.connect("192.168.0.1", 0, 0)  # 2 for s7-300 # first 0 for port number(0 arrg  automatically assign an available port), second 0 for timeout value.
        if client.get_connected():
            print("Client Connected")
            # log.info("Client Connected!")

            return client
        else:
            # log.info("No Communication from the client.")
            print("No Communication from the client.")
    except Exception as e:
        # log.error(f"ERROR initiating {e}")
        print(f"ERROR initiating {e}")
    return None


def get_machine_data():
    while True:
        client = snap7.client.Client()
        client = initiate(client)
        if client is not None:
            break
        time.sleep(1)
    buffer1 = int.from_bytes(client.read_area(snap7.types.Areas.DB, 3, 110, 2), 'big')
    buffer2 = int.from_bytes(client.read_area(snap7.types.Areas.DB, 3, 96, 2), 'big')
    buffer3 = int.from_bytes(client.read_area(snap7.types.Areas.DB, 3, 82, 2), 'big')
    buffer4 = int.from_bytes(client.read_area(snap7.types.Areas.DB, 3, 68, 2), 'big')
    buffer5 = int.from_bytes(client.read_area(snap7.types.Areas.DB, 3, 54, 2), 'big')
    buffer6 = int.from_bytes(client.read_area(snap7.types.Areas.DB, 7, 2, 2), 'big')  # single row quenching time
    buffer7 = int.from_bytes(client.read_area(snap7.types.Areas.DB, 7, 4, 2), 'big')  # Cycle Time
    buffer8 = int.from_bytes(client.read_area(snap7.types.Areas.DB, 7, 0, 2), 'big')  # double quenching time
    buffer9 = int.from_bytes(client.read_area(snap7.types.Areas.DB, 8, 0, 2), 'big')  # hardening charge

    payload = {
        "hardeningTemperatureZ1": buffer5,
        "hardeningTemperatureZ2": buffer4,
        "quenchingTemperature": buffer7,
        "temperingTemperatureZ1": buffer3,
        "temperingTemperatureZ2": buffer2,
        "temperingTemperatureZ3": buffer1,
        "quenchingTime": buffer6,
        "cycleTime": buffer9,
        "doubleQuenchingTime": buffer8,

    }

    log.info(f"Get machine data Payload: {payload}")

    return payload


def read_values(mb_client, parameters):
    """ Reads the parameters from the machine and returns them as a dictionary"""
    try:
        payload = dict()
        # TCP auto connect on modbus request, close after it
        log.info("[+] Reading values from holding registers")
        data0 = f_list(mb_client.read_holding_registers(22, 6),
                       False)  # here we are reading temperature 1 and 2 values and energy
        data1 = f_list(mb_client.read_holding_registers(32, 6), False)  # here we are reading cycle time
        log.info(f"got temp1 and 2 and energy data as {data0}")
        log.info(f"got time data as {data1}")
        values = data0
        if values is None:
            log.info(f"[*] Setting values 0")
            for index, keys in enumerate(parameters):
                payload[keys] = 0
        else:
            # we are appending it here because if values were none then it will create an exception
            if data1 is not None:
                values.append(sum(data1))
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


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        log.info("Connected to MQTT Broker!")
        client.subscribe(PUBLISH_TOPIC)
        client.subscribe(TRIGGER_TOPIC)

    else:
        log.error("Failed to connect, return code %d\n", rc)


# def try_connect_mqtt():
#     client_mqtt = mqtt.Client(GL_CLIENT_ID)
#     client_mqtt.on_connect = on_connect
#     client_mqtt.on_message = on_message
#     client_mqtt.username_pw_set(USERNAME, PASSWORD)
#     for i in range(5):
#         try:
#             client_mqtt.connect(MQTT_BROKER, MQTT_PORT, clean_start=mqtt.MQTT_CLEAN_START_FIRST_ONLY, keepalive=60)
#             if client_mqtt.is_connected():
#                 break
#         except Exception as e:
#             log.error(f"[-] Unable to connect to mqtt broker {e}")
#     try:
#         client_mqtt.loop_start()
#     except Exception as e:
#         log.error(f"[-] Error while starting loop {e}")
#     return client_mqtt


# def publish_values(payload):
#     global ob_client_mqtt
#     payload_str = json.dumps(payload)
#     log.info(f"Payload String: {payload_str}")
#
#     if GL_SEND_DATA:
#         result = [None, None]  # set the result to None
#         try:
#             result = ob_client_mqtt.publish(PUBLISH_TOPIC, payload_str)  # try to publish the data
#         except:  # if publish gives exception
#             try:
#                 ob_client_mqtt.disconnect()  # try to disconnect the client
#                 log.info(f"[+] Disconnected from Broker")
#                 time.sleep(2)
#             except:
#                 pass
#             if not ob_client_mqtt.is_connected():  # if client is not connected
#                 log.info(f"[+] Retrying....")
#                 for _ in range(5):
#                     ob_client_mqtt = try_connect_mqtt()  # retry to connect to the broker
#                     time.sleep(1)
#                     if ob_client_mqtt.is_connected():  # if connected: break
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
#                         sync_result = ob_client_mqtt.publish(PUBLISH_TOPIC, sync_payload)  # send payload
#                         if sync_result[0] == 0:  # if payload sent successful remove that payload from db
#                             ob_db.clear_sync_data(ts)
#                         else:  # else break from the loop
#                             log.error("[-] Can't send sync_payload")
#                             break
#         else:
#             log.error(f"[-] Failed to send message to topic {PUBLISH_TOPIC}")
#             ob_db.add_sync_data(payload)  # if status is not 0 (ok) then add the payload to the database


# endregion
def try_connect_mqtt1():
    client_mqtt = mqtt.Client(GL_CLIENT_ID)
    client_mqtt.on_connect = on_connect
    client_mqtt.on_message = on_message
    client_mqtt.username_pw_set(USERNAME, PASSWORD)
    for i in range(5):
        try:
            client_mqtt.connect(MQTT_BROKER1, MQTT_PORT, clean_start=mqtt.MQTT_CLEAN_START_FIRST_ONLY, keepalive=60)
            if client_mqtt.is_connected():
                break
        except Exception as e:
            log.error(f"[-] Unable to connect to mqtt broker {e}")
    try:
        client_mqtt.loop_start()
    except Exception as e:
        log.error(f"[-] Error while starting loop {e}")
    return client_mqtt


def publish_values1(payload):
    global ob_client_mqtt1
    payload_str = json.dumps(payload)
    log.info(f"{payload_str}")

    if GL_SEND_DATA:
        result = [None, None]  # set the result to None
        try:
            result = ob_client_mqtt1.publish(PUBLISH_TOPIC, payload_str)  # try to publish the data
        except:  # if publish gives exception
            try:
                ob_client_mqtt1.disconnect()  # try to disconnect the client
                log.info(f"[+] Disconnected from Broker")
                time.sleep(2)
            except:
                pass
            if not ob_client_mqtt1.is_connected():  # if client is not connected
                log.info(f"[+] Retrying....")
                for _ in range(5):
                    ob_client_mqtt1 = try_connect_mqtt1()  # retry to connect to the broker
                    time.sleep(1)
                    if ob_client_mqtt1.is_connected():  # if connected: break
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
                        sync_result = ob_client_mqtt1.publish(PUBLISH_TOPIC, sync_payload)  # send payload
                        if sync_result[0] == 0:  # if payload sent successful remove that payload from db
                            ob_db.clear_sync_data(ts)
                        else:  # else break from the loop
                            log.error("[-] Can't send sync_payload")
                            break
        else:
            log.error(f"[-] Failed to send message to topic {PUBLISH_TOPIC}")
            ob_db.add_sync_data(payload)  # if status is not 0 (ok) then add the payload to the database

def publish_values2(payload):
    global ob_client_mqtt1
    payload_str = json.dumps(payload)
    log.info(f"{payload_str}")

    if GL_SEND_DATA:
        result = [None, None]  # set the result to None
        try:
            result = ob_client_mqtt1.publish(
                GL_SERIAL_TOPIC , payload_str
            )  # try to publish the data
        except:  # if publish gives exception
            try:
                ob_client_mqtt1.disconnect()  # try to disconnect the client
                log.info(f"[+] Disconnected from Broker")
                time.sleep(2)
            except:
                pass
            if not ob_client_mqtt1.is_connected():  # if client is not connected
                log.info(f"[+] Retrying....")
                for _ in range(5):
                    ob_client_mqtt1 = (
                        try_connect_mqtt1()
                    )  # retry to connect to the broker
                    time.sleep(1)
                    if ob_client_mqtt1.is_connected():  # if connected: break
                        break
        # result: [0, 1]
        status = result[0]
        if status == 0:  # if status is 0 (ok)
            log.info(f"[+] Send `{result}` to topic `{GL_SERIAL_TOPIC}`")
            sync_data = (
                ob_db.get_sync_data()
            )  # get all the data from the sync payload db
            if sync_data:  # if sync_data present
                for i in sync_data:  # for every payload
                    if i:  # if payload is not empty
                        ts = i.get("ts")  # save timestamp
                        sync_payload = json.dumps(i.get("values"))
                        sync_result = ob_client_mqtt1.publish(
                            GL_SERIAL_TOPIC, sync_payload
                        )  # send payload
                        if (
                                sync_result[0] == 0
                        ):  # if payload sent successful remove that payload from db
                            ob_db.clear_sync_data(ts)
                        else:  # else break from the loop
                            log.error("[-] Can't send sync_payload")
                            break
        else:
            log.error(f"[-] Failed to send message to topic {GL_SERIAL_TOPIC}")
            # ob_db.add_sync_data(
            #     payload
            # )  # if status is not 0 (ok) then add the payload to the database
# endregion
def get_unknown_serial(line, stage, machine_id):
    stage = "{0:0>4}".format(stage)
    date_ = datetime.datetime.now()
    jd = date_.timetuple().tm_yday
    yr = date_.strftime("%y")
    dt = date_.strftime("%H%M%S")
    # return f"U{line}{stage}{machine_id}{yr}{jd}{dt}"
    return "null1"
def get_unknown_serial2(line, stage, machine_id):
    stage = "{0:0>4}".format(stage)
    date_ = datetime.datetime.now()
    jd = date_.timetuple().tm_yday
    yr = date_.strftime("%y")
    dt = date_.strftime("%H%M%S")
    # return f"U{line}{stage}{machine_id}{yr}{jd}{dt}"
    return "null2"



# Machine Data functions

# input and output sensors functions
def get_input_cylinder_count():
    return 2


import time

prev_time = time.time()
status = True
tb_len = 0
sqness = 0
quench_mode = 2


def get_status():
    global prev_time, status, tb_len, sqness
    if (time.time() - prev_time) > 10:
        prev_time = time.time()
        status = not status
        tb_len = 0
        sqness = 0
    tb_len = tb_len + 1
    sqness = sqness + 1
    data = {'status': status}
    print(data)
    return data


def get_machine_status(prev_status):
    global prev_time, status, tb_len, sqness
    if not prev_status:
        prev_time = time.time()
    return True

if __name__ == "__main__":
    # ob_client_mqtt = try_connect_mqtt()
    ob_client_mqtt1 = try_connect_mqtt1()
    serial_number = ''
    door_open_status = ''
    while True:
        try:
            if GL_SERIAL_NUMBER:
                ob_db.enqueue_serial_number(GL_SERIAL_NUMBER)
                log.info(f'serial number enqueued to db {GL_SERIAL_NUMBER}')
                values = {
                    "topic": "TRIGGER_STG011",
                    "message": {
                        "currentStage": "STG011",
                        "serialNumber": GL_SERIAL_NUMBER,
                        "line": "A",
                        "model": "Y9T"
                    }, }
                publish_values2(values)
            GL_SERIAL_NUMBER = ""

            data = get_machine_data()
            log.info(f"Got data from machine {data}")
            if data:
                quenching_time = data.get("quenchingTime")
                log.info(f'quenching time is {quenching_time}')
                double_quenching_time = data.get("doubleQuenchingTime")
                log.info(f'double quenching time is {double_quenching_time}')
                if 115< quenching_time < 119:
                    quench_mode = 1
                    FL_machine_status = get_machine_status(FL_machine_status)

                if 115<double_quenching_time < 119:
                    quench_mode = 2
                    FL_machine_status = get_machine_status(FL_machine_status)
                # else:
                #     quench_mode = None
                log.info(f"FL_machine_status is {FL_machine_status}")
                log.info(f"difference between time is {time.time() - prev_time}")

                if FL_machine_status and time.time() - prev_time >= 20:
                    FL_STATUS = True
                    prev_time = time.time()
                else:

                    FL_STATUS = False

                log.info(f"fl_status is {FL_STATUS}")
                if FL_STATUS != FL_PREV_STATUS:
                    number_of_cylinders = quench_mode
                    if number_of_cylinders is None:
                        number_of_cylinders = 0

                    serial_numbers = ob_db.get_serial_numbers(number_of_cylinders)
                    log.info(f'serial numbers {serial_numbers}')
                    log.info(f'number of cylinder is {number_of_cylinders}')
                    remaining_num_of_serials = number_of_cylinders - len(serial_numbers)
                    for i in range(remaining_num_of_serials):
                        ob_db.enqueue_serial_number(get_unknown_serial(LINE, STAGE, MACHINE_ID))
                        ob_db.enqueue_serial_number(get_unknown_serial2(LINE, STAGE, MACHINE_ID))
                    if remaining_num_of_serials:
                        serial_numbers = ob_db.get_serial_numbers(number_of_cylinders)

                    serial_list = list()
                    for i in serial_numbers:
                        serial_list.append(i[0])

                    payload = {"stage": GL_MACHINE_NAME, "timestamp": time.time()}
                    log.info(f"payload is {payload}")
                    log.info(f'serial list is {serial_list}')
                    if FL_STATUS:
                        log.info("CYCLE STARTED ")
                        for i in serial_list:
                            payload["serialNumber"] = i
                            log.info(f"published payload is {payload}")
                    if not FL_STATUS:
                        log.info("CYCLE ENDED")
                        for i in serial_list:
                            payload["serialNumber"] = i
                            payload['data'] = {

                                "hardeningTemperatureZ1": data.get("hardeningTemperatureZ1"),
                                "hardeningTemperatureZ2": data.get("hardeningTemperatureZ2"),
                                "quenchingTemperature": data.get("quenchingTemperature"),
                                "temperingTemperatureZ1": data.get("temperingTemperatureZ1"),
                                "temperingTemperatureZ2": data.get("temperingTemperatureZ2"),
                                "temperingTemperatureZ3": data.get("temperingTemperatureZ3"),
                                "cycleTime": data.get("quenchingTime"),
                            }

                            #publish_values(payload)
                            publish_values1(payload)
                            log.info(f"published payload is {payload}")
                            ob_db.delete_serial_number(i)
                            FL_machine_status = False
                FL_PREV_STATUS = FL_STATUS
            else:
                log.error(f"[-] Machine Disconnected got {data}")
            time.sleep(1)
        except Exception as e:
            time.sleep(5)
            log.error(f"[-] Error in cycle calculation {e}")
