import json
import random
import signal
import time

import paho.mqtt.client as mqtt
import paho.mqtt.reasoncodes as mqttrc


# Handler segnale CTRL+C
def signal_handler(sig_num: int, frame):
    sig_name = signal.Signals(sig_num).name

    # Disconnect from the broker
    err = mqttc.disconnect()
    print(f"\nEsecuzione interrotta dal segnale {sig_name} e connessione al broker cessata con " + "successo" if err == mqtt.MQTT_ERR_SUCCESS else "insuccesso")
    exit(0)

# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags: mqtt.ConnectFlags, reason_code: mqttrc.ReasonCode, properties):
    if reason_code.is_failure:
        print(f"\nFailed to connect: {reason_code}. loop_start() will retry connection\n")
    else:
        print(f"\nConnected with result code {reason_code}")
        print("The broker still has the session information for the client: ", end="")
        print("YES\n" if flags.session_present else "NO\n")

# The callback for when an automatic connection made by loop_start() or loop_forvere() failed to establish 
def on_connect_fail(client, userdata):
    print("Failed to establish the automatic TCP (re)connection to the broker made by loop_forever()")

# Installazione handler del segnale CTRL+C
signal.signal(signalnum=signal.SIGINT, handler=signal_handler)

mqttc = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2, client_id="sensor_publisher", clean_session=True)
mqttc.on_connect = on_connect
mqttc.on_connect_fail = on_connect_fail

# Connect to RabbitMQ broker
mqttc.connect(host="localhost", port=1883, keepalive=60)

mqttc.loop_start()

timeout = 4.98
# Publish temperature readings every 5 seconds
while True:
    # Attesa dell'effettiva connessione al broker (CONNACK)
    if mqttc.is_connected():
        temperature = random.randrange(15, 25)
        payload = json.dumps({"device_id": "device-01", "temperature": temperature, "timestamp": time.time()})

        # Publish with QoS 1 to ensure delivery
        msginfo = mqttc.publish(topic="sensors/temperature", payload=payload, qos=1)

        before_wait = time.time()
        msginfo.wait_for_publish(timeout=timeout)
        after_wait = time.time()
        if after_wait - before_wait >= timeout:
            print(f"Uscita da wait_for_publish() a causa del timeout di {timeout} s")
        else:
            print(f"Uscita da wait_for_publish() a causa del successo della pubblicazione sul broker")

        print(f"Published: {payload}\n")
        time.sleep(5)
