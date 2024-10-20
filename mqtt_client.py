from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient
import time
import json
import logging

# Configure logging
logger = logging.getLogger("AWSIoTPythonSDK.core")
logger.setLevel(logging.DEBUG)
streamHandler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
streamHandler.setFormatter(formatter)
logger.addHandler(streamHandler)

# Replace with your details
CLIENT_ID = "MyIoTDevice"
ENDPOINT = "a2k3beypvts4gq-ats.iot.us-east-1.amazonaws.com"
ROOT_CA = "AmazonRootCA1.pem"
PRIVATE_KEY = "private.pem.key"
DEVICE_CERT = "certificate.pem.crt"

# Initialize MQTT Client
mqtt_client = AWSIoTMQTTClient(CLIENT_ID)
mqtt_client.configureEndpoint(ENDPOINT, 8883)
mqtt_client.configureCredentials(ROOT_CA, PRIVATE_KEY, DEVICE_CERT)

# MQTT Client Configuration
mqtt_client.configureOfflinePublishQueueing(-1)  # Infinite offline publish queueing
mqtt_client.configureDrainingFrequency(2)  # Draining: 2 Hz
mqtt_client.configureConnectDisconnectTimeout(10)  # 10 sec
mqtt_client.configureMQTTOperationTimeout(5)  # 5 sec

# Define callbacks (optional)
def custom_on_publish(mid):
    print(f"Published message ID: {mid}")

mqtt_client.onMessagePublished = custom_on_publish

# Connect and Publish
mqtt_client.connect()
print("MQTTS Client Connected")

# Publish a message
topic = "iot/topic"
import time

message = {
    "DeviceID": "MyIoTDevice",
    "Timestamp": int(time.time()),
    "data": 20
}


message_json = json.dumps(message)
mqtt_client.publish(topic, message_json, 1)
print("Message Published")

# Wait to ensure the message is sent
time.sleep(2)

# Disconnect
mqtt_client.disconnect()
