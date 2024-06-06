from pyModbusTCP.client import ModbusClient

# Define the Modbus TCP/IP server parameters
SERVER_HOST = 'your_plc_ip_address'
SERVER_PORT = 502

# Define the Modbus addresses for the holding registers you want to read
START_ADDRESS = 1200  # Address of the first holding register
NUM_REGISTERS = 1  # Number of holding registers to read

# Create a Modbus TCP/IP client
client = ModbusClient(host='192.168.3.250', port=502)

# Connect to the Modbus server
if client.open():
    # Read holding registers from the PLC
    registers = client.read_holding_registers(START_ADDRESS, NUM_REGISTERS)

    if registers:
        # Print the values read from the PLC
        print("Values read from PLC:", registers)
    else:
        print("Failed to read from PLC")

    # Close the connection to the Modbus server
    client.close()
else:
    print("Failed to connect to the Modbus server")
