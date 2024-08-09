import os
import time
import json
import random
import paho.mqtt.client as mqtt
import schedule
import minimalmodbus
import logging
import datetime
import serial
import serial.tools.list_ports
from database import DBHelper
import logging.handlers
from logging.handlers import TimedRotatingFileHandler
from conversions import word_list_to_long, f_list, decode_ieee
from statistics import mean
from pyModbusTCP.client import ModbusClient

GL_AV_PRESSURE_HEAT = []
GL_AV_PRESSURE_HEAT1 = []
GL_AV_PRESSURE_HEAT2 = []
GL_AV_PRESSURE_CUTTING = []
GL_AV_PNG_PRESSURE = []
GL_AV_PROPANE_PRESSURE = []
GL_AV_PROPANE_PRESSURE1 = []
GL_AV_PROPANE_PRESSURE2 = []
GL_AV_DAACETYLENE_PRESSURE = []
GL_AV_FORMING_TEMP = []
GL_AV_HYDROLIC_POWER_PACK = []

# IF VALUE BELOW THRESHOLD
GL_MAX_PRESSURE_HEAT = 0

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

GL_MACHINE_INFO = {
    "Bottom Spinning2": {
        "pub_topic": "STG004",
        "sub_topic": "TRIGGER_STG004",
        "lwt_topic": "DEVICE_STATUS",
        "energy_topic": "bottom_spinning_em",
        "param_list": [
            "inductionTemperature",
            "O2PressureHeating",
            "O2PressureCutting",
            "PNGPressure",
            "propanePressure",
            "DAAcetylenePressure",
            "energyConsumption",
            "formingTemperature",
            "hydraulicPowerPack"
        ],
        'ip': '192.168.000.002',
        'machine_id': '01',
        'stage': 'BSPN',
        'line': 'B',
    },
    "Neck Spinning 2": {
        "pub_topic": "STG007",
        "sub_topic": "TRIGGER_STG007",
        "lwt_topic": "DEVICE_STATUS",
        "energy_topic": "neck_spinning_em",
        "param_list": [
            "inductionTemperature",
            "O2PressureHeating",
            "O2PressureCutting",
            "PNGPressure",
            "propanePressure",
            "DAAcetylenePressure",
            "energyConsumption",
            "formingTemperature",
            "hydraulicPowerPack"
        ],
        'ip': '192.168.0.1',
        'machine_id': '01',
        'stage': 'NSPN',
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
LWT_TOPIC = ""  # These variables will be initialized by init_conf

# endregion

ob_db = DBHelper()  # Object for DBHelper database class

# region Program Global Variables
FL_FIRST_CYCLE_RUN = True
FL_PREV_STATUS = False
GL_WAIT_CYCLE = False
FL_STATUS = False
GL_SEND_DATA = True
GL_SERIAL_NUMBER = ""
GL_SERIAL_TOPIC = 'Acknowledgements'
GL_INDUCTION_TEMP = 0
GL_O2_PRESSURE_HEAT = 0
GL_PRESSURE_CUTTING = []
GL_PNG_PRESSURE = 0
GL_PROPANE_PRESSURE = []
GL_PRESSURE_HEAT = []
GL_DAACETYLENE_PRESSURE = []
GL_HYDROLIC_POWER_PACK = []
GL_MAX_FORMING_TEMP = []
GL_CURRENT_KWH = 0
GL_PREV_KWH = 0
power_consumption = 0
GL_MAX_POWER_CONSUMPTION = 0
GL_MAX_PRESSURE_HEATING = 0
GL_MAX_PRESSURE_CUTTING = 0
GL_MAX_PROPANE_PRESSURE = 0
GL_MAX_DAACETYLENE_PRESSURE = 0
GL_MAX_HYDROLIC_POWER_PACK = 0
GL_MAX_FORMING_TEMP = 0

# average global parameters
GL_PRESSURE_HEAT_AVG = 0
GL_PRESSURE_HEAT_AVG1 = 0
GL_PRESSURE_HEAT_AVG2 = 0
GL_PRESSURE_CUTTING_AVG = 0
GL_PROPANE_PRESSURE_AVG = 0
GL_PROPANE_PRESSURE_AVG1 = 0
GL_PROPANE_PRESSURE_AVG2 = 0
GL_DAACETYLENE_PRESSURE_AVG = 0
GL_HYDROLIC_POWER_PACK_AVG = 0
GL_FORMING_TEMP_AVG = 0

GL_SERIAL_NUMBER_LIST = ""
GL_PREV_CYCL_START = time.time()


# endregion
prev_time = time.time()


# region Initialising Configuration here
def init_conf():
    global GL_MACHINE_NAME, GL_PARAM_LIST, PUBLISH_TOPIC, TRIGGER_TOPIC, ENERGY_TOPIC, GL_IP
    global MACHINE_ID, LINE, STAGE, LWT_TOPIC
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
            LWT_TOPIC = GL_MACHINE_INFO[GL_MACHINE_NAME]["lwt_topic"]
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
log.info(f"[+] Energy topic is {ENERGY_TOPIC}")


# endregion

# region Machine Specific functions


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


def initiate_client(ip, unit_id):
    """returns the modbus client instance"""
    log.info(f'Modbus Client IP: {ip}')
    return ModbusClient(host=ip, port=502, unit_id=unit_id, auto_open=True, auto_close=True, timeout=2)


def get_machine_status(mb_client):
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
        return values

    except Exception as e:
        log.error(f"[!] Error reading parameters from machine: {e}")
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
    log.info('Modbus ID Initialized: ' + str(i))
    return instrument


def get_machine_data():

    try:
        data_list = []
        param_list = [

            "O2PressureHeating",
            "propanePressure",
            "O2PressureHeating///",
            "propanePressure//",

            "O2PressureCutting",
            "DAAcetylenePressure",
            "propanePressure///",
            "O2PressureHeating//",
            "hydrolicpower",
            "hydraulicPowerPack",
            "formingTemperature"
        ]

        for slave_id in [2, 3, 4, 5]:
            log.info(f"[+] Getting data for slave id {slave_id}")
            reg_len = 4
            if slave_id == 4:
                reg_len = 2
            if slave_id == 5:
                reg_len = 1
            try:
                data = None
                for i in range(5):
                    mb_client = initiate_modbus(slave_id)
                    data = mb_client.read_registers(0, reg_len, 4)
                    if data:
                        break
                log.info(f"Got data {data}")
                if data is None:
                    for i in range(reg_len):
                        data_list.append(0)

                else:
                    data_list += data

            except Exception as e:
                log.error(f'[+] Failed to get data {e} slave id {slave_id}')
                for i in range(reg_len):
                    data_list.append(0)

        log.info(f'[*] Got data {data_list}')

        payload = {}
        for index, key in enumerate(param_list):
            payload[key] = data_list[index]
        payload['inductionTemperature'] = 0
        return payload

    except Exception as e:
        log.error(e)


def on_message(client_, userdata, message):
    global GL_CURRENT_KWH, GL_SERIAL_NUMBER_LIST

    log.info(f"[+] Received message {str(message.payload.decode('utf-8'))}")
    log.info(f"[+] From topic {message.topic}")

    # if got message from the trigger topic run the main function and send the message
    data = json.loads(message.payload)
    log.info(f"[+] Data is {data}")

    if message.topic == TRIGGER_TOPIC:  # if message is from trigger topic for serial number
        if data is not None:
            if data.get("line") == "B":
                GL_SERIAL_NUMBER_LIST = data.get("serialNumber")
                log.info(f"serial_number list is {GL_SERIAL_NUMBER_LIST}")

    elif message.topic == ENERGY_TOPIC:  # if message is from energy meter then update the energy value
        if data is not None:
            GL_CURRENT_KWH = data.get("energy")
            log.info(f"current kwh is {GL_CURRENT_KWH}")


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        log.info("Connected to MQTT Broker!")
        client.subscribe(PUBLISH_TOPIC)
        client.subscribe(TRIGGER_TOPIC)
        client.subscribe(ENERGY_TOPIC)
        online_message = {'machine_id': MACHINE_ID, 'line_id': LINE, 'stage': STAGE, 'status': 'online'}
        client.publish(LWT_TOPIC, f"{online_message}", qos=2, retain=False)

    else:
        log.error("Failed to connect, return code %d\n", rc)


def try_connect_mqtt():
    client_mqtt = mqtt.Client(GL_CLIENT_ID)
    client_mqtt.on_connect = on_connect
    client_mqtt.on_message = on_message
    client_mqtt.username_pw_set(USERNAME, PASSWORD)
    for i in range(5):
        try:
            offline_message = {'machine_id': MACHINE_ID, 'line_id': LINE, 'stage': STAGE, 'status': 'offline'}
            client_mqtt.will_set(LWT_TOPIC, payload=f"{offline_message}", qos=2, retain=False)

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
            log.info(f"{result}")
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
        log.info(f'{status}')
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



def publish_values2(payload):
    global ob_client_mqtt
    payload_str = json.dumps(payload)
    log.info(f"{payload_str}")

    if GL_SEND_DATA:
        result = [None, None]  # set the result to None
        try:
            result = ob_client_mqtt.publish(
                GL_SERIAL_TOPIC , payload_str
            )  # try to publish the data
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
            # sync_data = (
            #     ob_db.get_sync_data2()
            # )  # get all the data from the sync payload db
            # if sync_data:  # if sync_data present
            #     for i in sync_data:  # for every payload
            #         if i:  # if payload is not empty
            #             ts = i.get("ts")  # save timestamp
            #             sync_payload = json.dumps(i.get("values"))
            #             sync_result = ob_client_mqtt.publish(
            #                 GL_SERIAL_TOPIC, sync_payload
            #             )  # send payload
            #             if (
            #                     sync_result[0] == 0
            #             ):  # if payload sent successful remove that payload from db
            #                 ob_db.clear_sync_data2(ts)
            #             else:  # else break from the loop
            #                 log.error("[-] Can't send sync_payload")
            #                 break
        else:
            log.error(f"[-] Failed to send message to topic {GL_SERIAL_TOPIC}")
            # ob_db.add_sync_data2(
            #     payload
            # )  # if status is not 0 (ok) then add the payload to the database


# endregion

def get_unknown_serial(line, stage, machine_id):
    stage = "{0:0>4}".format(stage)
    date_ = datetime.datetime.now()
    jd = date_.timetuple().tm_yday
    yr = date_.strftime("%y")
    dt = date_.strftime("%H%M%S")
    return f"U{line}{stage}{machine_id}{yr}{jd}{dt}"


# region for testing only

status = True
tb_len = 0
sqness = 0

if __name__ == "__main__":
    ob_client_mqtt = try_connect_mqtt()
    #ob_client_mqtt1 = try_connect_mqtt1()
    Threshold_forming_temp = 850
    MAX_VALUE = 1500
    End_thresh = 700
    while True:
        try:
            try:
                if GL_SERIAL_NUMBER_LIST:
                    ob_db.enqueue_serial_number(GL_SERIAL_NUMBER_LIST)
                    log.info(f"[+] Enqueueing serial number to db {GL_SERIAL_NUMBER_LIST}")
                    values = {
                        "topic": "TRIGGER_STG004",
                        "message": {
                            "currentStage": "STG004",
                            "serialNumber": GL_SERIAL_NUMBER_LIST,
                            "line": "B",
                            "model": "Y9T"
                        }, }
                    publish_values2(values)
                    GL_SERIAL_NUMBER_LIST = ""

            except Exception as e:
                time.sleep(5)
                log.error(f"[-] Error in running program{e}")

            data = get_machine_data()
            mb_client = initiate_client(GL_IP, GL_U_ID)
            data2 = get_machine_status(mb_client)
            log.info(f'machine status is {data2}')
            log.info(f'data from massibus is {data}')
            if data:
                if FL_FIRST_CYCLE_RUN:
                    FL_FIRST_CYCLE_RUN = False
                if 180 > data2[0] > 2:
                    FL_STATUS = True
                    log.info('cycle is running')
                    try:
                        if 900 <= data.get("formingTemperature") <= 1200:
                            GL_AV_FORMING_TEMP.append(data.get("formingTemperature"))
                    except Exception as e:
                        log.error(f"[-] Error in GL_MAX_FORMING_TEMP {e}")

                    try:
                        if 1 <= data.get("O2PressureHeating") / 100 <= 6:
                            GL_AV_PRESSURE_HEAT.append(data.get("O2PressureHeating") / 100)
                        else:
                            GL_AV_PRESSURE_HEAT.append(0)
                    except Exception as e:
                        log.error(f"[-] Error in O2PressureHeating {e}")


                    try:
                        if 1 <= data.get("O2PressureHeating//") / 100 <= 6:
                            GL_AV_PRESSURE_HEAT1.append(data.get("O2PressureHeating//") / 100)
                        else:
                            GL_AV_PRESSURE_HEAT1.append(0)
                    except Exception as e:
                        log.error(f"[-] Error in O2PressureHeating {e}")

                    try:
                        if 1 <= data.get("O2PressureHeating///") / 100 <= 6:
                            GL_AV_PRESSURE_HEAT2.append(data.get("O2PressureHeating///") / 100)
                        else:
                            GL_AV_PRESSURE_HEAT2.append(0)
                    except Exception as e:
                        log.error(f"[-] Error in O2PressureHeating {e}")

                    try:
                        if 1 <= data.get("O2PressureCutting") / 100 <= 7:
                            GL_AV_PRESSURE_CUTTING.append(data.get("O2PressureCutting") / 100)
                    except Exception as e:
                        log.error(f"[-] Error in O2PressureCutting {e}")

                    try:
                        if data.get("propanePressure") == 65535:
                            propanePressure = 0
                        else:
                            propanePressure = data.get("propanePressure") / 100
                        if 0.1 <= propanePressure <= 1.9:
                            GL_AV_PROPANE_PRESSURE.append(propanePressure)
                    except Exception as e:
                        log.error(f"[-] Error in propanePressure {e}")


                    try:
                        if data.get("propanePressure//") == 65535:
                            propanePressure1 = 0
                        else:
                            propanePressure1 = data.get("propanePressure//") / 100
                        if 0.1 <= propanePressure1 <= 1.9:
                            GL_AV_PROPANE_PRESSURE1.append(propanePressure1)
                    except Exception as e:
                        log.error(f"[-] Error in propanePressure {e}")


                    try:
                        if data.get("propanePressure///") == 65535:
                            propanePressure2 = 0
                        else:
                            propanePressure2 = data.get("propanePressure///") / 100
                        if 0.1 <= propanePressure2 <= 1.9:
                            GL_AV_PROPANE_PRESSURE2.append(propanePressure2)
                    except Exception as e:
                        log.error(f"[-] Error in propanePressure {e}")


                    try:
                        if data.get("DAAcetylenePressure") > 6000:
                            log.info(f'[+]------------------value is more then 6000 <{data.get("DAAcetylenePressure")}')
                            DAAcetylenePressure = 0
                        else:
                            DAAcetylenePressure = data.get("DAAcetylenePressure") / 100

                        if 0.1 <= DAAcetylenePressure <= 2:
                            GL_AV_DAACETYLENE_PRESSURE.append(DAAcetylenePressure)
                    except Exception as e:
                        log.error(f"[-] Error in DAAcetylenePressure {e}")

                    try:
                        if data.get("hydraulicPowerPack")/10 > 160:
                            log.info(f'[+]-value is more then 160 < {data.get("hydraulicPowerPack")/10}')
                            hydraulicPowerPack = 157
                            GL_AV_HYDROLIC_POWER_PACK.append(hydraulicPowerPack)
                        else:
                            hydraulicPowerPack = data.get("hydraulicPowerPack")/10
                            if 60 <= data.get("hydraulicPowerPack") <= 150:
                                GL_AV_HYDROLIC_POWER_PACK.append(data.get("hydraulicPowerPack")/10)
                    except Exception as e:
                        log.error(f"[-] Error in hydraulicPowerPack {e}")
                else:
                    log.info(f"[+] Cycle Stopped")
                    FL_STATUS = False
                if FL_PREV_STATUS != FL_STATUS:
                    serial_number = ob_db.get_first_serial_number()
                    log.info(serial_number)
                    # if serial_number is None:
                    #     serial_number = "null1"
                    #     log.info(f"[+] Adding Unknown serial number to queue {serial_number}")
                    #     ob_db.enqueue_serial_number(serial_number)
                    payload = {
                        "stage": GL_MACHINE_NAME,
                        "timestamp": time.time(),
                        "serialNumber": serial_number,
                    }
                    log.info(f"{payload}")
                    if FL_STATUS:  # if cycle started if started then publish serial number only
                        log.info("[+] Cycle Running")
                        # print(payload)
                    if not FL_STATUS:  # if cycle ended then publish serial number with data
                        log.info(f"[+] Cycle Stopped")
                        power = GL_CURRENT_KWH - GL_PREV_KWH
                        if power > power_consumption:
                            power_consumption = power
                        GL_FORMING_TEMP_AVG = 0
                        GL_MAX_PNG_PRESSURE = 0
                        ob_db.save_running_data(
                            GL_INDUCTION_TEMP,
                            GL_PRESSURE_HEAT_AVG,
                            GL_PRESSURE_CUTTING_AVG,
                            GL_MAX_PNG_PRESSURE,
                            GL_PROPANE_PRESSURE_AVG,
                            GL_DAACETYLENE_PRESSURE_AVG,
                            GL_HYDROLIC_POWER_PACK_AVG,
                            GL_FORMING_TEMP_AVG,
                            serial_number,
                        )
                        try:
                            log.info(f"GL_AV_FORMING_TEMP {GL_AV_FORMING_TEMP}")
                            log.info(f"GL_AV_PRESSURE_CUTTING {GL_AV_PRESSURE_CUTTING}")
                            log.info(f"GL_AV_PNG_PRESSURE {GL_AV_PNG_PRESSURE}")
                            log.info(f"GL_AV_DAACETYLENE_PRESSURE {GL_AV_DAACETYLENE_PRESSURE}")
                            if GL_AV_FORMING_TEMP:
                                GL_FORMING_TEMP_AVG = max(GL_AV_FORMING_TEMP)
                                log.info(f'Average forming temp: {GL_FORMING_TEMP_AVG}')
                            else:
                                GL_FORMING_TEMP_AVG = data.get("formingTemperature")

                            if GL_AV_PRESSURE_HEAT:
                                GL_PRESSURE_HEAT_AVG = max(GL_AV_PRESSURE_HEAT)
                                log.info(f'Average pressure heat: {GL_PRESSURE_HEAT_AVG}')

                            if GL_AV_PRESSURE_HEAT1:
                                GL_PRESSURE_HEAT_AVG1 = max(GL_AV_PRESSURE_HEAT1)
                                log.info(f'Average pressure heat: {GL_PRESSURE_HEAT_AVG1}')

                            if GL_AV_PRESSURE_HEAT2:
                                GL_PRESSURE_HEAT_AVG2 = max(GL_AV_PRESSURE_HEAT2)
                                log.info(f'Average pressure heat: {GL_PRESSURE_HEAT_AVG2}')

                            if GL_AV_PRESSURE_CUTTING:
                                GL_PRESSURE_CUTTING_AVG = max(GL_AV_PRESSURE_CUTTING)
                                log.info(f'Average pressure cutting: {GL_PRESSURE_CUTTING_AVG}')

                            if GL_AV_HYDROLIC_POWER_PACK:
                                GL_HYDROLIC_POWER_PACK_AVG = max(GL_AV_HYDROLIC_POWER_PACK)
                                log.info(f'Average hydrolic power pack: {GL_HYDROLIC_POWER_PACK_AVG}')
                            else:
                                log.info('Hydrolic power pack is empty')

                            if GL_AV_PROPANE_PRESSURE:
                                GL_PROPANE_PRESSURE_AVG = max(GL_AV_PROPANE_PRESSURE)

                                log.info(f'Average propane pressure: {GL_PROPANE_PRESSURE_AVG}')
                            else:
                                log.info('Propane pressure is empty')

                            if GL_AV_PROPANE_PRESSURE1:
                                GL_PROPANE_PRESSURE_AVG1 = max(GL_AV_PROPANE_PRESSURE1)

                                log.info(f'Average propane pressure1: {GL_PROPANE_PRESSURE_AVG1}')
                            else:
                                log.info('Propane pressure1 is empty')

                            if GL_AV_PROPANE_PRESSURE2:
                                GL_PROPANE_PRESSURE_AVG2 = max(GL_AV_PROPANE_PRESSURE2)

                                log.info(f'Average propane pressure2: {GL_PROPANE_PRESSURE_AVG2}')
                            else:
                                log.info('Propane pressure2 is empty')


                            if GL_AV_DAACETYLENE_PRESSURE:

                                GL_DAACETYLENE_PRESSURE_AVG = max(GL_AV_DAACETYLENE_PRESSURE)
                                log.info(f'Average Daacetylene pressure: {GL_DAACETYLENE_PRESSURE_AVG}')
                            else:
                                log.info('Daacetylene pressure is empty')
                        except Exception as e:
                            print(e)

                        payload["data"] = {
                            "inductionTemperature": GL_INDUCTION_TEMP,
                            "O2PressureHeating": GL_PRESSURE_HEAT_AVG1+GL_PRESSURE_HEAT_AVG + GL_PRESSURE_HEAT_AVG2,
                            "O2PressureCutting": GL_PRESSURE_CUTTING_AVG,
                            "propanePressure": GL_PROPANE_PRESSURE_AVG2,
                            "DAAcetylenePressure": GL_DAACETYLENE_PRESSURE_AVG,
                            "formingTemperature": GL_FORMING_TEMP_AVG,
                            "hydraulicPowerPack": GL_HYDROLIC_POWER_PACK_AVG,
                            "powerConsumption": power_consumption,
                        }

                        # publish_values(payload)
                        if serial_number is not None:
                            publish_values(payload)
                            log.info(f"payload published is {payload} ")
                            ob_db.delete_serial_number(serial_number)
                            log.info(f"serial number deleted")
                        else:
                            log.warning(f"[!] {serial_number} Serial Number Found : Data send : Cancel")
                        # log.info(f"payload published is {payload} ")
                        # #publish_values1(payload)
                        # log.info(f"payload published is {payload} ")
                        # ob_db.delete_serial_number(serial_number)
                        # log.info(f"serial number deleted")
                        Cycle_time = 0
                        GL_INDUCTION_TEMP = 0
                        GL_PRESSURE_HEAT_AVG = 0
                        GL_PRESSURE_CUTTING_AVG = 0
                        GL_PNG_PRESSURE = 0
                        GL_PROPANE_PRESSURE_AVG = 0
                        GL_DAACETYLENE_PRESSURE_AVG = 0
                        GL_FORMING_TEMP_AVG = 0
                        power_consumption = 0
                        GL_HYDROLIC_POWER_PACK_AVG = 0

                        GL_AV_PRESSURE_HEAT = []
                        GL_AV_PRESSURE_CUTTING = []
                        GL_AV_PNG_PRESSURE = []
                        GL_AV_PROPANE_PRESSURE = []
                        GL_AV_DAACETYLENE_PRESSURE = []
                        GL_AV_FORMING_TEMP = []
                        GL_AV_HYDROLIC_POWER_PACK = []

                FL_PREV_STATUS = FL_STATUS
            else:
                log.error(f"[-] Machine Disconnected got {data}")
                time.sleep(10)
        except Exception as e:
            time.sleep(5)
            log.error(f"[-] Error in running program{e}")
