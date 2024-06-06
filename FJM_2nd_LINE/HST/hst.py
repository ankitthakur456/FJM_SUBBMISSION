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
import minimalmodbus
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
    'HST2': {
        'pub_topic': 'STG014',
        'sub_topic': 'TRIGGER_STG014',
        'energy_topic': 'ihf_1_em',
        'param_list': ['testingPressure', 'Cycletime'],
        'ip': '192.168.3.250',
        'machine_id': '01',
        'stage': 'HST2',
        'line': 'A',
    },
}

GL_MACHINE_NAME = ''  # These variables will be initialized by init_conf
STAGE = ''
LINE = ''
MACHINE_ID = ''
GL_IP = ''
GL_U_ID = 1
GL_PARAM_LIST = []  # These variables will be initialized by init_conf
# endregion
FULL_WT = 0
EMPTY_WT = 0
# region MQTT params
MQTT_BROKER = 'ec2-13-232-172-215.ap-south-1.compute.amazonaws.com'
MQTT_BROKER1 = '192.168.33.150'
MQTT_PORT = 1883
USERNAME = 'mmClient'
PASSWORD = 'ind4.0#2023'
GL_CLIENT_ID = f'HIS-MQTT-{random.randint(0, 1000)}'
GL_SERIAL_TOPIC = 'Acknowledgements'

PUBLISH_TOPIC = ''  # These variables will be initialized by init\
# _conf
TRIGGER_TOPIC = ''  # These variables will be initialized by init_conf
ENERGY_TOPIC = ''  # These variables will be initialized by init_conf
# endregion

ob_db = DBHelper()  # Object for DBHelper database class

# region Program Global Variables
GL_SEND_DATA = True
# endregion

# region Barcode Params
PARITY = serial.PARITY_NONE
STOP_BITS = serial.STOPBITS_ONE
BYTE_SIZE = serial.EIGHTBITS
BAUD_RATE = 19200
# endregion

# region program global variables

GL_PREV_KWH = 0
GL_MAX_HEATING_TIME = 0
GL_MAX_TEMP = 0
FL_STATUS = False
FL_PREV_STATUS = False
FL_FIRST_CYCLE_RUN = True
GL_SERIAL_NUMBER = ''

GL_MAX_PRESSURE = 0
GL_MIN_PRESSURE = 0
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
log.info(f"[+] Energy topic is {ENERGY_TOPIC}")


# endregion


# region machine_functions
# region Modbus RTU Functions
def get_serial_port():
    try:
        ports = serial.tools.list_ports.comports()
        usb_ports = [p.device for p in ports if "USB" in p.description]
        print(usb_ports)
        if len(usb_ports) < 1:
            raise Exception("Could not find USB ports")
        return usb_ports
    except Exception as e:
        print(f"[-] Error Can't Open Port {e}")
        return None


def initiate_modbus(slaveId):
    com_port = '/dev/serial/by-id/usb-FTDI_FT232R_USB_UART_A10NALW0-if00-port0'
    i = int(slaveId)
    # log.info(f"[+] for modbus Using comport {com_port}")
    instrument = minimalmodbus.Instrument(com_port, i)
    instrument.serial.baudrate = 19200
    instrument.serial.bytesize = 8
    instrument.serial.parity = serial.PARITY_NONE
    instrument.serial.stopbits = 1
    instrument.serial.timeout = 3
    instrument.serial.close_after_each_call = True
    print('Modbus ID Initialized: ' + str(i))
    return instrument


try:
    PORT_WT = '/dev/serial/by-id/usb-1a86_USB2.0-Ser_-if00-port0'
    # PORT_WT = get_serial_port()
    # if PORT_WT:
    #     PORT_WT = PORT_WT[GL_WEIGHT_PORT_ID]
    #     log.info(f"[+] For weight Using comport {PORT_WT}")
    wt_ser = serial.Serial(
        port=PORT_WT,
        baudrate=9600,
        parity=serial.PARITY_NONE,
        stopbits=serial.STOPBITS_ONE,
        bytesize=serial.EIGHTBITS,
        xonxoff=False,
        timeout=1,
        write_timeout=1
    )
except Exception as e:
    time.sleep(10)
    log.error(f'ERROR: {e} Error in opening serial port')


def read_weight():
    global PORT_WT, wt_ser
    try:
        wt_ser.flushOutput()
        wt_ser.flushInput()
        wt_ser.flush()
        weight = wt_ser.read_until()
        log.info(f"Raw weight is {weight}")
        weight = str(weight).replace("b", "").strip("'").replace(r"\r\n", "")
        log.info(f"Got data --- {weight}")
        return weight
    except Exception:
        try:
            time.sleep(2)
            wt_ser.flushOutput()
            wt_ser.flushInput()
            wt_ser.flush()
            wt_ser.close()
        except:
            pass
        try:
            PORT_WT = '/dev/serial/by-id/usb-1a86_USB2.0-Ser_-if00-port0'
            # PORT_WT = get_serial_port()
            # if PORT_WT:
            #     PORT_WT = PORT_WT[GL_WEIGHT_PORT_ID]
            # log.info(f"[+] For weight Using comport {PORT_WT}")

            wt_ser = serial.Serial(
                port=PORT_WT,
                baudrate=9600,
                parity=serial.PARITY_NONE,
                stopbits=serial.STOPBITS_ONE,
                bytesize=serial.EIGHTBITS,
                xonxoff=False,
                timeout=1,
                write_timeout=1

            )
            weight = wt_ser.read_until()
            weight = str(weight).replace("b", "").strip("'").replace(r"\r\n", "")
            weight = float(weight)
            return weight
        except Exception as e:
            log.error(f'ERROR: {e} Error in opening weight serial port')
            return "Error"


# endregion


# region modbus TCP port


def get_machine_data():
    try:
        data_list = []
        param_list = ['testingPressure']

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


    except Exception as e:
        log.error(e)


# endregion
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
        if data is not None and data.get('line') == 'B':
            GL_SERIAL_NUMBER = data.get('serialNumber')

    elif message.topic == ENERGY_TOPIC:  # if message is from energy meter then update the energy value
        if data is not None:
            GL_CURRENT_KWH = data.get('energy')


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
                log.info(f"sending data to {PUBLISH_TOPIC}")
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
            result = ob_client_mqtt.publish(PUBLISH_TOPIC, payload_str, qos =2)  # try to publish the data
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
                        sync_result = ob_client_mqtt.publish(PUBLISH_TOPIC, sync_payload, qos =2)  # send payload
                        if sync_result[0] == 0:  # if payload sent successful remove that payload from db
                            ob_db.clear_sync_data(ts)
                        else:  # else break from the loop
                            log.error("[-] Can't send sync_payload")
                            break
        else:
            log.error(f"[-] Failed to send message to topic {PUBLISH_TOPIC}")
            ob_db.add_sync_data(payload)  # if status is not 0 (ok) then add the payload to the database


# endregion
def try_connect_mqtt1():
    client_mqtt = mqtt.Client(GL_CLIENT_ID)
    client_mqtt.on_connect = on_connect
    client_mqtt.on_message = on_message
    client_mqtt.username_pw_set(USERNAME, PASSWORD)
    for i in range(5):
        try:
            client_mqtt.connect(
                MQTT_BROKER1,
                MQTT_PORT,
                clean_start=mqtt.MQTT_CLEAN_START_FIRST_ONLY,
                keepalive=60,
            )
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
            result = ob_client_mqtt1.publish(
                PUBLISH_TOPIC, payload_str, qos =2
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
            log.info(f"[+] Send `{result}` to topic `{PUBLISH_TOPIC}`")
            sync_data = (
                ob_db.get_sync_data2()
            )  # get all the data from the sync payload db
            if sync_data:  # if sync_data present
                for i in sync_data:  # for every payload
                    if i:  # if payload is not empty
                        ts = i.get("ts")  # save timestamp
                        sync_payload = json.dumps(i.get("payload"))
                        sync_result = ob_client_mqtt1.publish(
                            PUBLISH_TOPIC, sync_payload, qos =2
                        )  # send payload
                        if (
                                sync_result[0] == 0
                        ):  # if payload sent successful remove that payload from db
                            ob_db.clear_sync_data2(ts)
                        else:  # else break from the loop
                            log.error("[-] Can't send sync_payload")
                            break
        else:
            log.error(f"[-] Failed to send message to topic {PUBLISH_TOPIC}")
            ob_db.add_sync_data2(
                payload
            )  # if status is not 0 (ok) then add the payload to the database



def publish_values2(payload):
    global ob_client_mqtt1
    payload_str = json.dumps(payload)
    log.info(f"{payload_str}")

    if GL_SEND_DATA:
        result = [None, None]  # set the result to None
        try:
            result = ob_client_mqtt1.publish(
                GL_SERIAL_TOPIC, payload_str, qos =2
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
                            GL_SERIAL_TOPIC, sync_payload, qos =2
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


# region for testing only

prev_time = time.time()
status = True
tb_len = 0
sqness = 0


def get_machine_status(pressure, prev_status):
    global prev_time, status, tb_len, sqness, GL_MIN_PRESSURE
    if pressure >= 330 and not prev_status:
        prev_time = time.time()
        GL_MIN_PRESSURE = 330
        return True
    # if (time.time() - prev_time) > 20:
    #     prev_time = time.time()
    #     status = not status
    #     tb_len = 0
    #     sqness = 0

    return prev_status


# endregion till here


if __name__ == "__main__":
    ob_client_mqtt = try_connect_mqtt()
    ob_client_mqtt1 = try_connect_mqtt1()
    while True:
        try:
            if GL_SERIAL_NUMBER:
                ob_db.enqueue_serial_number(GL_SERIAL_NUMBER)
                log.info(f'enquing serial number to db {GL_SERIAL_NUMBER}')
                values = {
                    "topic": "TRIGGER_STG014",
                    "message": {
                        "currentStage": "STG014",
                        "serialNumber": GL_SERIAL_NUMBER,
                        "line": "B",
                        "model": "Y9T"
                    }, }
                publish_values2(values)
                GL_SERIAL_NUMBER = ''

            data = get_machine_data()
            log.info(f"data is {data}")
            ser_wt = read_weight()
            ser_wt = float(ser_wt)
            # log.info(f"weight is number {isinstance(ser_wt, float)}")
            log.info(ser_wt)
            if not ser_wt:
                ser_wt = 0
                log.info(f"Got no weight giving 0")
            if 34 < ser_wt < 60:
                EMPTY_WT = ser_wt
            elif ser_wt > FULL_WT:
                FULL_WT = ser_wt
            else:
                pass

            if data:
                full_weight = FULL_WT
                empty_weight = EMPTY_WT

                if FL_FIRST_CYCLE_RUN:  # handling reboots and starts of program if this flag is set
                    FL_FIRST_CYCLE_RUN = False  # then initialize the previous values such as prev_kwh and max_temp
                    GL_MAX_PRESSURE = data.get('testingPressure')

                    Cycletime = data.get('Cycletime')
                    log.info(f'cycletime is {Cycletime}')
                    log.info(f"pressure is number {isinstance(data.get('testingPressure'), float)}")


                if ser_wt > 38:
                    FL_STATUS = True

                    log.info(f'cycle_running weight is {ser_wt}')
                    prev_time = time.time()
                else:
                    FL_STATUS = False

                if FL_PREV_STATUS != FL_STATUS:

                    serial_number = ob_db.get_first_serial_number()

                    if serial_number is None:
                        serial_number = 'NULL1'
                        log.info(f"[+] Adding Unknown serial number to queue {serial_number}")
                        ob_db.enqueue_serial_number(serial_number)

                    payload = {"stage": GL_MACHINE_NAME, "timestamp": time.time(), "serialNumber": serial_number}

                    if FL_STATUS:
                        print(payload)
                        log.info(f"payload is {payload}")
                    if not FL_STATUS:
                        # ob_db.save_running_data(empty_weight, full_weight, GL_MAX_PRESSURE, serial_number)

                        payload['data'] = {
                            "fullWeightOfCylinder": full_weight,
                            "emptyWeigthtOfCylinder": 20,
                            "testingPressure": GL_MAX_PRESSURE
                        }

                        publish_values(payload)
                        publish_values1(payload)
                        log.info(f"payload is {payload}")
                        ob_db.delete_serial_number(serial_number)
                        GL_MAX_PRESSURE = 0
                        GL_MAX_TEMP = 0
                FL_PREV_STATUS = FL_STATUS
                log.info('status is false')
            else:
                log.error(f"[-] Machine Disconnected got {data}")
            time.sleep(1)
        except Exception as e:
            time.sleep(5)
            log.error(f"[-] Error in cycle calculation {e}")
