import random
import serial
import time
from time import sleep
from database import DBHelper
import datetime
import json
import serial.tools.list_ports
import os
import logging
import logging.handlers
from conversions import get_hour, get_shift
from logging.handlers import TimedRotatingFileHandler
import paho.mqtt.client as mqtt

# Setting up Rotating file logging
dirname = os.path.dirname(os.path.abspath(__file__))

log_level = logging.INFO

c = DBHelper()
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
fileHandler.setFormatter(logFormatter)
fileHandler.suffix = "%Y-%m-%d.log"
log.addHandler(fileHandler)

consoleHandler = logging.StreamHandler()
consoleHandler.setFormatter(logFormatter)
log.addHandler(consoleHandler)
log.setLevel(log_level)

# Code for reading barcode
ports = serial.tools.list_ports.comports()
usb_ports = [p.device for p in ports if "USB" in p.device]
log.info(usb_ports)

#PORT_WT = usb_ports[0]
#PORT_WT = "/dev/serial/by-id/usb-1a86_USB2.0-Ser_-if00-port0"
try:
    PORT_WT = usb_ports[0]
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
    sleep(10)
    log.error(f'ERROR: {e} Error in opening serial port')

timer = 0.5
today = (datetime.datetime.now() - datetime.timedelta(hours=8)).strftime("%F")
USERNAME = 'mmClient'
PASSWORD = 'ind4.0#2023'
SAMPLE_RATE = 60
broker = 'ec2-13-232-172-215.ap-south-1.compute.amazonaws.com'
broker1 = '192.168.33.150'
port = 1883
topic = "STG016"
TRIGGER_TOPIC = "TRIGGER_TEST"
FULL_WT = 0
EMPTY_WT = 0
# generate client ID with pub prefix randomly
client_id = f'HIS-MQTT-{random.randint(0, 1000)}'
GL_SERIAL_NUMBER = ''
def read_weight():
    global PORT_WT, wt_ser
    try:
        wt_ser.flushOutput()
        wt_ser.flushInput()
        wt_ser.flush()
        weight = wt_ser.read_until()
        weight = str(weight).replace("b", "").strip("'")
        weight= weight.replace("\\x02N", "")
        weight = weight.replace("\\r\\n", "")
        weight = float(weight)
        log.info(f"Got data --- {weight}")
        return weight
    except Exception:
        try:
            sleep(2)
            log.info('done this state')
            wt_ser.flushOutput()
            wt_ser.flushInput()
            wt_ser.flush()
            wt_ser.close()
        except:
            pass
        try:
            ports = serial.tools.list_ports.comports()
            usb_ports = [p.device for p in ports if "USB" in p.device]
            log.info(usb_ports)
            PORT_WT = usb_ports[0]
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
            weight = str(weight).replace("b", "").strip("'")
            weight = weight.replace("\\x02N", "")
            weight = weight.replace("\\r\\n", "")
            weight = float(weight)
            return weight
        except Exception as e:
            log.error(f'ERROR: {e} Error in opening weight serial port')
            return "Error"

def on_message(client, userdata, message):
    global GL_SERIAL_NUMBER_LIST
    log.info("received message: ", str(message.payload.decode("utf-8")))
    data = json.loads(message.payload)
    # log.info(f"[+] Data is {data}")

    if message.topic == TRIGGER_TOPIC:  # if message is from trigger topic for serial number
        if data is not None:
            GL_SERIAL_NUMBER = data.get('serialNumber')
            log.info(f'serial number is {GL_SERIAL_NUMBER}')
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        log.info("Connected to MQTT Broker!")
        client.subscribe(TRIGGER_TOPIC)
    else:
        log.error("Failed to connect, return code %d\n", rc)


def try_connect_mqtt():
    client_mqtt = mqtt.Client(client_id)
    client_mqtt.on_connect = on_connect
    client_mqtt.username_pw_set(USERNAME, PASSWORD)
    try:
        client_mqtt.connect(broker, port, clean_start=mqtt.MQTT_CLEAN_START_FIRST_ONLY, keepalive=60)
        client_mqtt.connect(broker1, port, clean_start=mqtt.MQTT_CLEAN_START_FIRST_ONLY, keepalive=60)
    except Exception as e:
        log.error(f"[-] Unable to connect to mqtt broker {e}")
    try:
        client_mqtt.loop_start()
    except Exception as e:
        log.error(f"[-] Error while starting loop {e}")
    return client_mqtt

if __name__ == '__main__':
    log.info("Started....")
    client = try_connect_mqtt()

    while True:
        prev_date, prev_shift, prev_hour = c.get_misc_data()
        try:
            if GL_SERIAL_NUMBER:
                c.enqueue_serial_number(GL_SERIAL_NUMBER)
                log.info(f'enquing serial number to db {GL_SERIAL_NUMBER}')
                GL_SERIAL_NUMBER = ''

            serial_number = c.get_first_serial_number()
            if serial_number is None:
                serial_number = 'null1'
                log.info(f"[+] Adding Unknown serial number to queue {serial_number}")
                c.enqueue_serial_number(serial_number)
            error = 0
            weight = read_weight()
            # if weight > 60:
            #     FULL_WT = weight
            # if weight < 60:
            #     EMPTY_WT = weight
            payload = {"stage": "UST2", "timestamp": time.time(), "serialNumber": serial_number,
                       "data": {"fullWeightOfCylinder": weight, "emptyWeigthtOfCylinder": 20, "testingPressure": 30}}
            today = (datetime.datetime.now() - datetime.timedelta(hours=6)).strftime("%F")
            if payload is None:
                payload = {'error': "Machine Disconnected"}
                error += 1

            else:
                error = 0
            # log.info(payload)
            payload_str = json.dumps(payload)
            log.info(f"{payload_str}, error:{error}")

            result = [None, None]  # set the result to None
            try:
                if error == 0 or error >= 10:
                    result = client.publish(topic,
                                            payload_str)  # try to publish the data if publish gives exception
                else:
                    result = [0, 1]
                    time.sleep(2)
            except:
                try:
                    client.disconnect()  # try to disconnect the client
                    log.info(f"[+] Disconnected from Broker")
                    sleep(2)
                except:
                    pass
                if not client.is_connected():  # if client is not connected
                    log.info(f"[+] Retrying....")
                    for _ in range(5):
                        client = try_connect_mqtt()  # retry to connect to the broker
                        sleep(1)
                        if client.is_connected():  # if connected: break
                            break
                            time.sleep(2)
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
        except Exception as e:
            log.error(e)
            # client.unsubscribe("JBMGroup/MachineData")
            client.disconnect()
            sleep(5)
            client.loop_stop()

        sleep(SAMPLE_RATE)
