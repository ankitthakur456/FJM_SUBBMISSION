import paho.mqtt.client as mqtt

# Define MQTT parameters
broker_address ="59.145.138.235" # "192.1.33.120"  # Replace with your broker's address/// 59.145.138.235
port = 1883  # Default MQTT port
topic = "TRIGGER_TEST"  # Replace with the desired MQTT topic
username = "mmClient"  # Replace with your MQTT username
password = "ind4.0#2023"  # Replace with your MQTT password (if required)

# Callback when the client connects to the broker
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to MQTT broker")
        # Subscribe to a topic
        client.subscribe(topic)
    else:
        print(f"Connection failed with error code {rc}")
# Callback when a message is received from the broker
def on_message(client, userdata, message):
    print(f"Received message '{message.payload.decode()}' on topic '{message.topic}'")

# Create an MQTT client
client = mqtt.Client()

# Set up callbacks
client.on_connect = on_connect
client.on_message = on_message

# Set credentials if required
client.username_pw_set(username, password)

# Connect to the MQTT broker
client.connect(broker_address, port, keepalive=60)

# Start the MQTT loop
client.loop_start()

# Publish a message (optional)
message = "Hello, MQTT!"
client.publish(topic, message)

# Keep the program running
try:
    while True:
        pass
except KeyboardInterrupt:
    pass

# Disconnect from the broker
client.loop_stop()
client.disconnect()
