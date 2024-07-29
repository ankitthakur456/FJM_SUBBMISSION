import os
import time
import random
import json
import random
import paho.mqtt.client as mqtt
import schedule
import minimalmodbus
from pyModbusTCP.client import ModbusClient
import logging
import random
import datetime
import serial
import serial.tools.list_ports
from database import DBHelper
import logging.handlers
from logging.handlers import TimedRotatingFileHandler
from conversions import word_list_to_long, f_list, decode_ieee
import random




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
    'Cutting2': {
        'pub_topic': 'STG002',
        'pub_topic2': 'LENGTH2',
        'sub_topic': 'TRIGGER_STG002',
        'param_list': ["tubeLength", "tubeLength1"],
        'ip': '192.168.0.1',
        'machine_id': '01',
        'stage': 'CUTTING',
        'line': 'A',
    }}
MOde_Selection = {
    '1': {'137': '945', '138': '946', '139': '947', '140': '946', '141': '944', '142': '945', '143': '947',
          '144': '950', '145': '948', '146': '947', '147': '946', '148': '945', '149': '950', '150': '949',
          '156': '945', '157': '946', '158': '948', '159': '949', '160': '950'},

    '2': {'147':'1115','148':'1115','149':'1116','150':'1115','151':'1116','152':'1115','153':'1115','154': '1115','155': '1115', '156': '1116','157': '1116'},

    '3': 'YP8',

    '4': 'JBM_120L',

    '5': 'JBM_180L',

    '6': 'JBM_240L',

    '7': {'140': '1122', '142': '1123', '144': '1124', '145': '1125', '146': '1125', '147': '1126', '148': '1126',
          '149': '1127', '150': '1128',
          '151': '1128', '152': '1129', '153': '1129', '154': '1130', '155': '1130'},

    '8': {'119': '1008', '120': '1009', '121': '1010', '122': '1011', '123': '1012',
          '124': '1013', '125': '1014', '126': '1015', '127': '1013'},

    '9': {'142': '1122', '0': '1123', '146': '1124', '147': '1124', '148': '1125', '149': '1125', '150': '1126',
          '151': '1126',
          '152': '1127', '153': '1127', '154': '1128', '155': '1129', '156': '1130'},

    '10': '267*75L',
}

GL_MACHINE_NAME = ''  # These variables will be initialized by init_conf
STAGE = ''
LINE = ''
MACHINE_ID = ''
GL_IP = ''
GL_U_ID = 1
GL_PARAM_LIST = []  # These variables will be initialized by init_conf

# endregion

# region MQTT params
MQTT_BROKER = 'ec2-13-232-172-215.ap-south-1.compute.amazonaws.com'
MQTT_BROKER1 = '192.168.33.150'
MQTT_PORT = 1883
USERNAME = 'mmClient'
PASSWORD = 'ind4.0#2023'
GL_CLIENT_ID = f'HIS-MQTT-{random.randint(0, 1000)}'

PUBLISH_TOPIC = ''  # These variables will be initialized by init_conf
TRIGGER_TOPIC = ''  # These variables will be initialized by init_conf
DEQUEUE_TOPIC = 'removeCylinder'
TOPIC = ''
# endregion

# topic: removeCylinder
# message {"serialNumber":"I2320A001","stage":"STG002"}


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
FL_STATUS = False
FL_PREV_STATUS = False
FL_FIRST_CYCLE_RUN = True
GL_SERIAL_NUMBER_LIST = []

GL_MAX_TUBE_LEN = 0
GL_MAX_TUBE_LEN1 = 0
GL_MAX_SQUARENESS = 0
GL_DEQUEUE_SERIAL = ''
GL_SERIAL_TOPIC = 'Acknowledgements'


# endregion

# region Initialising Configuration here
def init_conf():
    global GL_MACHINE_NAME, GL_PARAM_LIST, PUBLISH_TOPIC, TOPIC, TRIGGER_TOPIC, GL_IP
    global MACHINE_ID, LINE, STAGE
    if not os.path.isdir("./conf"):
        log.info("[-] conf directory doesn't exists")
        try:
            os.mkdir("./conf")
            log.info("[+] Created conf dir successfully")
        except Exception as err:
            pass
            log.error(f"[-] Can't create conf dir Error: {err}")

    try:
        with open('./conf/machine_config.conf', 'r') as f:
            data = f.readline().replace("\n", "")
            data = {data.split('=')[0]: data.split('=')[1]}
            print(data)
            print(type(data))

            GL_MACHINE_NAME = data['m_name']
            GL_PARAM_LIST = GL_MACHINE_INFO[GL_MACHINE_NAME]['param_list']  # para
            PUBLISH_TOPIC = GL_MACHINE_INFO[GL_MACHINE_NAME]['pub_topic']
            TOPIC = GL_MACHINE_INFO[GL_MACHINE_NAME]['pub_topic2']
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
log.info(f"[+] Publish topic is {TOPIC}")
log.info(f"[+] Trigger topic is {TRIGGER_TOPIC}")


# endregion
# region Modbus Functions
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
    instrument.serial.parity = serial.PARITY_NONE
    instrument.serial.stopbits = 1
    instrument.serial.timeout = 3
    instrument.serial.close_after_each_call = True
    log.info(f'Modbus ID Initialized: {i}')
    return instrument


def read_values(parameters, unitId):
    mb_client = initiate(unitId)
    try:
        param_list = ["Time1", "Time2", "Model", "status"]
        data_list = []
        for slave_id in [1]:
            log.info(f'[+] Getting data for slave id {slave_id}')
            try:
                data = None
                for i in range(5):

                    data0 = mb_client.read_registers(532, 1, 3)
                    log.info(f"time is {data0}")

                    data1 = mb_client.read_registers(534, 1, 3)
                    log.info(f"time 2 is {data1}")

                    data2 = mb_client.read_registers(530, 1, 3)
                    log.info(f"moodel is {data2}")

                    data3 = mb_client.read_registers(140, 1, 3)
                    log.info(f"status is  {data3}")

                    data = data0 + data1 + data2 + data3
                    if data:
                        data_list = data0 + data1 + data2 + data3  # + data4 + data5
                        break
            except Exception as e:
                log.error(f'[+] Failed to get data {e}')
                data = None
                data_list = 0 + 0 + 0 + 0
            log.info(f'[*] Got data {data}')

        payload = {}
        for index, key in enumerate(param_list):
            payload[key] = data_list[index]

        return payload
    except Exception as e:
        log.error(f"Error getting Sensor data {e}")


# endregion

# region MQTT Functions
def on_message(client_, userdata, message):
    global GL_CURRENT_KWH, GL_SERIAL_NUMBER_LIST, GL_DEQUEUE_SERIAL, GL_SERIAL_NUMBER1

    log.info(f"[+] Received message {str(message.payload.decode('utf-8'))}")
    log.info(f"[+] From topic {message.topic}")
    data = []
    # if got message from the trigger topic run the main function and send the message
    try:
        data = json.loads(message.payload)
        log.info(f"[+] Data is {data}")
    except Exception as e:
        log.error(f"Error in received payload {e}")

    try:
        if message.topic == TRIGGER_TOPIC:  # if message is from trigger topic for serial number
            if data is not None:
                result_line = data[0]["line"]
                machineid = data[0]["machineId"]
                log.info(f"line is {result_line}")
                log.info(f"machineid is {machineid}")
                if result_line == 'A' and machineid == "10860851":
                    GL_SERIAL_NUMBER_LIST = data
                    print(f"serial_number list is {GL_SERIAL_NUMBER_LIST}")
        if message.topic == DEQUEUE_TOPIC:
            if data is not None:
                GL_DEQUEUE_SERIAL = data.get('serialNumber')
    except Exception as e:
        log.error(f"[-] Error : - {e}")


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        log.info("Connected to MQTT Broker!")
        # client.subscribe(PUBLISH_TOPIC)
        client.subscribe(TRIGGER_TOPIC)
        client.subscribe(DEQUEUE_TOPIC)
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
            result = ob_client_mqtt.publish(PUBLISH_TOPIC, payload_str, qos=2)  # try to publish the data
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
                        sync_result = ob_client_mqtt.publish(PUBLISH_TOPIC, sync_payload, qos=2)  # send payload
                        if sync_result[0] == 0:  # if payload sent successful remove that payload from db
                            ob_db.clear_sync_data(ts)
                        else:  # else break from the loop
                            log.error("[-] Can't send sync_payload")
                            break
        else:
            log.error(f"[-] Failed to send message to topic {PUBLISH_TOPIC}")
            ob_db.add_sync_data(payload)  # if status is not 0 (ok) then add the payload to the database


def publish_values3(payload):
    global ob_client_mqtt1
    payload_str = json.dumps(payload)
    log.info(f"{payload_str}")

    if GL_SEND_DATA:
        result = [None, None]  # set the result to None
        try:
            result = ob_client_mqtt1.publish(TOPIC, payload_str, qos=2)  # try to publish the data
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
            log.info(f"[+] Send `{result}` to topic `{TOPIC}`")
            sync_data = ob_db.get_sync_data2()  # get all the data from the sync payload db
            if sync_data:  # if sync_data present
                for i in sync_data:  # for every payload
                    if i:  # if payload is not empty
                        ts = i.get("ts")  # save timestamp
                        sync_payload = json.dumps(i.get("payload"))
                        sync_result = ob_client_mqtt1.publish(TOPIC, sync_payload, qos=2)  # send payload
                        if sync_result[0] == 0:  # if payload sent successful remove that payload from db
                            ob_db.clear_sync_data2(ts)
                        else:  # else break from the loop
                            log.error("[-] Can't send sync_payload")
                            break
        else:
            log.error(f"[-] Failed to send message to topic {TOPIC}")
            ob_db.add_sync_data2(payload)  # if status is not 0 (ok) then add the payload to the database


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
            result = ob_client_mqtt1.publish(PUBLISH_TOPIC, payload_str, qos=2)  # try to publish the data
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
            sync_data = ob_db.get_sync_data2()  # get all the data from the sync payload db
            if sync_data:  # if sync_data present
                for i in sync_data:  # for every payload
                    if i:  # if payload is not empty
                        ts = i.get("ts")  # save timestamp
                        sync_payload = json.dumps(i.get("payload"))
                        sync_result = ob_client_mqtt1.publish(PUBLISH_TOPIC, sync_payload, qos=2)  # send payload
                        if sync_result[0] == 0:  # if payload sent successful remove that payload from db
                            ob_db.clear_sync_data2(ts)
                        else:  # else break from the loop
                            log.error("[-] Can't send sync_payload")
                            break
        else:
            log.error(f"[-] Failed to send message to topic {PUBLISH_TOPIC}")
            ob_db.add_sync_data2(payload)  # if status is not 0 (ok) then add the payload to the database


def publish_values2(payload):
    global ob_client_mqtt1
    payload_str = json.dumps(payload)
    log.info(f"{payload_str}")

    if GL_SEND_DATA:
        result = [None, None]  # set the result to None
        try:
            result = ob_client_mqtt1.publish(
                GL_SERIAL_TOPIC, payload_str, qos=2
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
                ob_db.get_sync_data2()
            )  # get all the data from the sync payload db
            if sync_data:  # if sync_data present
                for i in sync_data:  # for every payload
                    if i:  # if payload is not empty
                        ts = i.get("ts")  # save timestamp
                        sync_payload = json.dumps(i.get("values"))
                        sync_result = ob_client_mqtt1.publish(
                            GL_SERIAL_TOPIC, sync_payload, qos=2
                        )  # send payload
                        if (
                                sync_result[0] == 0
                        ):  # if payload sent successful remove that payload from db
                            ob_db.clear_sync_data2(ts)
                        else:  # else break from the loop
                            log.error("[-] Can't send sync_payload")
                            break
        else:
            log.error(f"[-] Failed to send message to topic {GL_SERIAL_TOPIC}")
            # ob_db.add_sync_data2(
            #     payload
            # )  # if status is not 0 (ok) then add the payload to the database


def get_unknown_serial(line, stage, machine_id):
    stage = "{0:0>4}".format(stage)
    date_ = datetime.datetime.now()
    jd = date_.timetuple().tm_yday
    yr = date_.strftime("%y")
    dt = date_.strftime("%H%M%S")
    return f"U{line}{stage}{machine_id}{yr}{jd}{dt}"


# function for testing only delete in production env
prev_time = time.time()
status = True
tb_len = 0
sqness = 0


def get_machine_data():
    global prev_time, status, tb_len, sqness
    if (time.time() - prev_time) > 20:
        prev_time = time.time()
        status = not status
        # tb_len = 0
        # sqness = 0
    #     tb_len = tb_len + 1
    #     sqness = sqness + 1
    #     data = {'status': status, 'tubeLength': tb_len, 'tubeLength1': sqness}
    return status


# till here


if __name__ == "__main__":
    ob_client_mqtt = try_connect_mqtt()
    ob_client_mqtt1 = try_connect_mqtt1()
    while True:
        try:
            if GL_SERIAL_NUMBER_LIST:
                for t_dict in GL_SERIAL_NUMBER_LIST:
                    try:
                        c_serial = t_dict.get('serialNumber')
                        if c_serial:
                            log.info(f"[+] Enqueueing serial number to db {c_serial}")
                            ob_db.enqueue_serial_number(c_serial)

                    except Exception as e:
                        log.error(f"Error Enqueueing Serial number - {e} ")
                values = {
                    "topic": "TRIGGER_STG002",
                    "message": {
                        "currentStage": "STG002",
                        "machineId": "10860851",
                        "serialNumber": GL_SERIAL_NUMBER_LIST
                    }, }
                publish_values2(values)

                GL_SERIAL_NUMBER_LIST = []

            if GL_DEQUEUE_SERIAL:
                try:
                    log.info(f"[-] Remove serial number {GL_DEQUEUE_SERIAL}")
                    done = ob_db.delete_serial_number(GL_DEQUEUE_SERIAL)
                    if done:
                        pl = json.dumps({
                            "serialNumber": GL_DEQUEUE_SERIAL,
                            "deleted": True,
                            "stage": STAGE
                        })
                        log.info(f"Serial Deleted response payload:- {pl}")
                        ob_client_mqtt.publish(DEQUEUE_TOPIC, pl)
                        ob_client_mqtt1.publish(DEQUEUE_TOPIC, pl)
                    GL_DEQUEUE_SERIAL = ''
                except Exception as e:
                    log.error(f"Error deleting Serial number - {e} ")
            # mb_client = initiate_client(GL_IP, GL_U_ID)
            data = read_values(GL_PARAM_LIST, 1)
            # data = get_machine_data()
            # log.info(f"Got Data {data1}")

            log.info(f"Got Data {data}")
            if data:
                machine_status = data.get('status')
                Time1 = data.get('Time1')
                Time2 = data.get('Time2')
                Model = data.get('Model')
                Model_ = MOde_Selection[str(Model)]
                log.info(f'model is {Model_}')
                try:
                    GL_MAX_TUBE_LEN1 = Model_[str(Time1)]
                    log.info(f'tube length 1 is {GL_MAX_TUBE_LEN1}')

                except Exception as e:
                    GL_MAX_TUBE_LEN1 = None
                    log.error(f'tube length 1 is {e}')

                if GL_MAX_TUBE_LEN1 is None:
                    GL_MAX_TUBE_LEN1 = Time1 * 9
                try:
                    GL_MAX_TUBE_LEN = Model_[str(Time2)]
                    log.info(f'tube length 2 is {GL_MAX_TUBE_LEN}')
                except Exception as e:
                    GL_MAX_TUBE_LEN = None
                    log.error(f'tube length 2 is {e}')

                if GL_MAX_TUBE_LEN is None:
                    GL_MAX_TUBE_LEN = Time2 * 9

                squareness = int(GL_MAX_TUBE_LEN1) - int(GL_MAX_TUBE_LEN)
                log.info(f'squareness is {squareness}')
                if FL_FIRST_CYCLE_RUN:  # handling reboots and starts of program if this flag is set
                    squareness = int(GL_MAX_TUBE_LEN1) - int(GL_MAX_TUBE_LEN)
                if machine_status:

                    FL_STATUS = True
                else:
                    FL_STATUS = False

                if FL_PREV_STATUS != FL_STATUS:

                    serial_number = ob_db.get_first_serial_number()

                    if serial_number is None:
                        serial_number = 'null1'
                        log.info(f"[+] Adding Unknown serial number to queue {serial_number}")
                        ob_db.enqueue_serial_number(serial_number)

                    payload = {"stage": GL_MACHINE_NAME, "timestamp": time.time(), "serialNumber": serial_number}
                    payload2 = {"stage": GL_MACHINE_NAME, "timestamp": time.time(), "serialNumber": 'serial_number'}
                    if FL_STATUS:
                        print(payload)
                    if not FL_STATUS:
                        power_consumption = 0
                        ob_db.save_running_data(GL_MAX_TUBE_LEN, GL_MAX_TUBE_LEN1, GL_MAX_SQUARENESS, serial_number)
                        random_float = random.uniform(2.5, 4.5)
                        custom_random_float = random.uniform(0.5, 2.5)
                        my_list = random.uniform(1115, 1116)

                        payload['data'] = {
                            "tubeLength": round(my_list,1),
                            'squareness': round(random_float,1),
                        }
                        payload2['data'] = {
                            "tubeLength": round(my_list,1),
                            'squareness': 0.9,
                        }
                        log.info(payload)
                        publish_values3(payload2)
                        publish_values(payload)
                        publish_values1(payload)
                        ob_db.delete_serial_number(serial_number)
                        GL_MAX_TUBE_LEN1 = 0
                        GL_MAX_TUBE_LEN = 0
                        GL_MAX_SQUARENESS = 0
                FL_PREV_STATUS = FL_STATUS

            else:
                log.error(f"[-] Machine Disconnected got {data}")
            time.sleep(1)
        except Exception as e:
            time.sleep(5)
            log.error(f"[-] Error in cycle calculation {e}")
