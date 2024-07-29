import os
import time
import json
import random
import paho.mqtt.client as mqtt
from pyModbusTCP.client import ModbusClient
import logging
import minimalmodbus
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
    'IHF-3': {
        'pub_topic': 'STG003',
        'sub_topic': 'IHFQueue',
        'energy_topic': 'ihf_1_em',
        'param_list': ['pyrometerTemperature', 'pyrometerTemperature2', 'energyConsumption', 'heatingTime'],
        'ip': '192.168.0.1',
        'machine_id': '03',
        'stage': 'IHF3',
        'line': 'B',
    },
    'IHF-4': {
        'pub_topic': 'STG006',
        'sub_topic': 'IHFQueue',
        'energy_topic': 'ihf_4_em',
        'param_list': ['heatingTime'],
        'ip': '192.168.33.46',
        'SERIAL_TOPIC': 'Acknowledgements',
        'LWT_TOPIC': 'DEVICE_STATUS',
        'machine_id': '04',
        'stage': 'IHF4',
        'line': 'B',
    }
}

GL_MACHINE_NAME = ''  # These variables will be initialized by init_conf
STAGE = ''
LINE = ''
MACHINE_ID = ''
GL_IP = ''
GL_U_ID = 1
GL_PARAM_LIST = []  # These variables will be initialized by init_conf
# endregion
heating_time = 0
# region MQTT params
MQTT_BROKER = '192.168.33.150'
MQTT_PORT = 1883
USERNAME = 'mmClient'
PASSWORD = 'ind4.0#2023'
GL_CLIENT_ID = f'HIS-MQTT-{random.randint(0, 1000)}'

PUBLISH_TOPIC = ''  # These variables will be initialized by init_conf
TRIGGER_TOPIC = ''  # These variables will be initialized by init_conf
ENERGY_TOPIC = ''  # These variables will be initialized by init_conf
GL_SERIAL_TOPIC = 'Acknowledgements'
GL_PREV_CYCL_START = time.time()
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

GL_PREV_KWH = 0
GL_CURRENT_KWH = 0
GL_MAX_HEATING_TIME = 0
GL_MAX_TEMP = 0
FL_STATUS = False
FL_PREV_STATUS = False
FL_FIRST_CYCLE_RUN = True
GL_SERIAL_NUMBER = ''
GL_SERIAL_NUMBER1 = ''
GL_PREV_CYCL_START = time.time()


# endregion


# region Initialising Configuration here
def init_conf():
    global GL_MACHINE_NAME, GL_PARAM_LIST, PUBLISH_TOPIC, TRIGGER_TOPIC, ENERGY_TOPIC, GL_IP
    global MACHINE_ID, LINE, STAGE, LWT_TOPIC, GL_SERIAL_TOPIC
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
            GL_SERIAL_TOPIC = GL_MACHINE_INFO[GL_MACHINE_NAME]["SERIAL_TOPIC"]
            LWT_TOPIC = GL_MACHINE_INFO[GL_MACHINE_NAME]["LWT_TOPIC"]
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
log.info(f"[+] Energy topic is {ENERGY_TOPIC}")
log.info(f"[+] SERIAL_TOPIC is {GL_SERIAL_TOPIC}")
log.info(f"[+] LWT_TOPIC is {LWT_TOPIC}")
lwt_message = {'machine_id': MACHINE_ID, 'line_id': LINE, 'stage': STAGE, 'status': 'offline'}
lwt_payload = f"{lwt_message}"


# endregion


# region Modbus Functions

def get_serial_port():
    try:
        ports = serial.tools.list_ports.comports()
        usb_ports = [p.device for p in ports if "USB" in p.description]
        log.info(usb_ports)
        if len(usb_ports) < 1:
            raise Exception("Could not find USB ports")
        return usb_ports[0]
    except Exception as e:
        log.error(f"[-] Error Can't Open Port {e}")
        return None


def initiate_modbus(slaveId):
    com_port = None
    for i in range(5):
        com_port = get_serial_port()
        if com_port:
            break
    i = int(slaveId)
    instrument = minimalmodbus.Instrument(com_port, i)
    instrument.serial.baudrate = 19200
    instrument.serial.bytesize = 8
    instrument.serial.parity = serial.PARITY_NONE
    instrument.serial.stopbits = 1
    instrument.serial.timeout = 3
    instrument.serial.close_after_each_call = True
    log.info("Modbus ID Initialized: " + str(i))
    return instrument


def get_machine_data():
    try:
        data_list = []
        param_list = ['pyrometerTemperature']

        for slave_id in [1]:
            log.info(f"[+] Getting data for slave id {slave_id}")
            reg_len = 1

            try:
                data = None
                for i in range(5):
                    mb_client = initiate_modbus(slave_id)
                    data = mb_client.read_registers(0, reg_len, 4)
                    if data:
                        break
                print(f"Got data {data}")
                if data is None:
                    for i in range(reg_len):
                        data_list.append(0)

                else:
                    data_list += data

            except Exception as e:
                log.error(f"[+] Failed to get data {e} slave id {slave_id}")
                for i in range(reg_len):
                    data_list.append(0)

        log.info(f"[*] Got data {data_list}")

        payload = {}
        for index, key in enumerate(param_list):
            payload[key] = data_list[index]

        payload["inductionTemperature"] = 0
        return payload
        # data = {
        #     "inductionTemperature": 70,
        #     "O2PressureHeating": 200,
        #     "O2PressureCutting": 200,
        #     "PNGPressure": 45,
        #     "propanePressure": 23,
        #     "DAAcetylenePressure": 23,
        #     "formingTemperature": get_forming_temperature(),
        #     "energyConsumption": 20,
        #     "hydraulicPowerPack": 20,
        #     'status': True,
        # }

    except Exception as e:
        log.error(e)


# endregion
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
        data0 = mb_client.read_holding_registers(1, 1)
        log.info(f"Got Time from machine : {data0}")
        values = data0
        if values is None:
            values = [0]
        return {'heatingTime': values[0] / 100}

    except Exception as e:
        log.error(f"[!] Error reading parameters from machine: {e}")
        return None


# endregion

# region MQTT Functions
def on_message(client_, userdata, message):
    global GL_CURRENT_KWH, GL_SERIAL_NUMBER, GL_SERIAL_NUMBER1

    log.info(f"[+] Received message {str(message.payload.decode('utf-8'))}")
    log.info(f"[+] From topic {message.topic}")

    # if got message from the trigger topic run the main function and send the message
    data = json.loads(message.payload)
    log.info(f"[+] Data is {data}")

    if message.topic == TRIGGER_TOPIC:  # if message is from trigger topic for serial number
        if data is not None:
            if data.get("line") == 'B' and data.get("stage") == 'STG006' and data.get("operation") == "push":
                GL_SERIAL_NUMBER = data.get("serialNumber")
                log.info(f"serial_number list is {GL_SERIAL_NUMBER}")
            if data.get("line") == 'B' and data.get("stage") == 'STG006' and data.get("operation") == "pop":
                GL_SERIAL_NUMBER1 = data.get("serialNumber")



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
        lwt_message['status'] = 'online'
        client.publish(LWT_TOPIC, f"{lwt_message}", qos=2, retain=False)


    else:
        log.error("Failed to connect, return code %d\n", rc)


def try_connect_mqtt():
    client_mqtt = mqtt.Client(GL_CLIENT_ID)
    client_mqtt.will_set(LWT_TOPIC, payload=lwt_payload, qos=2, retain=False)
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


# endregion
def publish_values2(payload):
    global ob_client_mqtt
    payload_str = json.dumps(payload)
    log.info(f"{payload_str}")

    if GL_SEND_DATA:
        result = [None, None]  # set the result to None
        try:
            result = ob_client_mqtt.publish(GL_SERIAL_TOPIC, payload_str, qos=2)  # try to publish the data
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
                    ob_client_mqtt = (
                        try_connect_mqtt()
                    )  # retry to connect to the broker
                    time.sleep(1)
                    if ob_client_mqtt.is_connected():  # if connected: break
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
                        sync_result = ob_client_mqtt.publish(GL_SERIAL_TOPIC, sync_payload, qos=2)  # send payload
                        if (sync_result[0] == 0):  # if payload sent successful remove that payload from db
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


if __name__ == "__main__":
    ob_client_mqtt = try_connect_mqtt()
    while True:
        try:
            if GL_SERIAL_NUMBER:
                ob_db.enqueue_serial_number(GL_SERIAL_NUMBER)
                log.info(f'[+] serial number enqueued successfully')
                values = {
                    "topic": "IHFQueue",
                    "message": {
                        "currentStage": "STG006",
                        "serialNumber": GL_SERIAL_NUMBER,
                        "line": "B",
                        "model": "Y9T"
                    }, }
                publish_values2(values)
            elif GL_SERIAL_NUMBER1:
                ob_db.delete_serial_number(GL_SERIAL_NUMBER1)
                log.info(f'[-] serial number deleted successfully')
            GL_SERIAL_NUMBER = ''
            GL_SERIAL_NUMBER1 = ""

            mb_client = initiate_client(GL_IP, GL_U_ID)
            data2 = read_values(mb_client, GL_PARAM_LIST)
            log.info(f'data is {data2}')
            data = get_machine_data()
            log.info(f'data is {data}')

            if data:
                # log.info(f"[+] Data is {data}")
                heating_time = data2.get('heatingTime')
                log.info(f"heating time is {heating_time}")
                new_temp = data.get('pyrometerTemperature')

                if FL_FIRST_CYCLE_RUN:  # handling reboots and starts of program if this flag is set
                    FL_FIRST_CYCLE_RUN = False  # then initialize the previous values such as prev_kwh and max_temp
                    GL_PREV_KWH = GL_CURRENT_KWH
                    GL_MAX_TEMP = new_temp

                if heating_time > GL_MAX_HEATING_TIME:
                    log.info(f"{heating_time}  > {GL_MAX_HEATING_TIME}")
                    GL_MAX_HEATING_TIME = heating_time

                    if new_temp > GL_MAX_TEMP:
                        log.info(f"{new_temp} > {GL_MAX_TEMP}")
                        GL_MAX_TEMP = new_temp
                        if not FL_STATUS:
                            GL_PREV_CYCL_START = time.time()
                        FL_STATUS = True
                    log.info(f'[++++++]adding 6 second delay in code {(time.time() - GL_PREV_CYCL_START)} > 90')
                # elif heating_time < GL_MAX_HEATING_TIME and heating_time == 0 and (time.time() - GL_PREV_CYCL_START) > 88:
                elif heating_time == 0 and (time.time() - GL_PREV_CYCL_START) > 90:
                    # log.info(f'[++++++]adding 6 second delay in code {(time.time() - GL_PREV_CYCL_START)}')
                    FL_STATUS = False

                if FL_PREV_STATUS != FL_STATUS:

                    serial_number = ob_db.get_first_serial_number()

                    if serial_number is None:
                        serial_number = 'null1'
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
                        ob_db.save_running_data(GL_MAX_TEMP, GL_MAX_HEATING_TIME, power_consumption, serial_number)

                        payload['data'] = {
                            "pyrometerTemperature": GL_MAX_TEMP,
                            "energyConsumption": power_consumption,
                            "heatingTime": GL_MAX_HEATING_TIME,
                        }
                        log.info(payload)
                        publish_values(payload)
                        ob_db.delete_serial_number(serial_number)
                        GL_PREV_KWH = GL_CURRENT_KWH
                        GL_MAX_HEATING_TIME = 0
                        GL_MAX_TEMP = 0
                FL_PREV_STATUS = FL_STATUS
            else:
                log.error(f"[-] Machine Disconnected got {data}")
            time.sleep(1)
        except Exception as e:
            time.sleep(5)
            log.error(f"[-] Error in cycle calculation {e}")
