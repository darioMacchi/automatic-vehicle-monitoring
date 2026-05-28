import json
import signal
import socket
import sys

import paho.mqtt.client as mqtt
import paho.mqtt.reasoncodes as mqttrc


# Handler segnale CTRL+C
def signal_handler(sig_num: int, frame):
    sig_name = signal.Signals(sig_num).name

    # Disconnect from the broker
    err = mqttc.disconnect()
    print(f"\nEsecuzione interrotta dal segnale {sig_name} e connessione al broker cessata con " + "successo" if err == mqtt.MQTT_ERR_SUCCESS else "insuccesso")
    exit(0)

# The callback for when the client receives a SUBACK response from the broker
def on_subscribe(client, userdata, mid, reason_code_list, properties):
    # Since we subscribed only for a single channel, reason_code_list contains
    # a single entry
    if reason_code_list[0].is_failure:
        print(f"Broker rejected your subscription: {reason_code_list[0]}\n")
    else:
        print(f"Broker granted the following QoS: {reason_code_list[0].value}\n")

# The callback for when the client receives a CONNACK response from the server.
def on_connect(client: mqtt.Client, userdata, flags: mqtt.ConnectFlags, reason_code: mqttrc.ReasonCode, properties):
    if reason_code.is_failure:
        print(f"\nFailed to connect: {reason_code}. loop_forever() will retry connection")
    else:
        print(f"\nConnected with result code {reason_code}")
        print("The broker still has the session information for the client: ", end="")
        print("YES" if flags.session_present else "NO")

        # Subscribing in on_connect() means that if we lose the connection and
        # reconnect then subscriptions will be renewed.
        # We should always subscribe from on_connect callback to be sure
        # our subscribed is persisted across reconnections.
        client.subscribe(topic=[("AVM/telemetry/autobus/termic", 1), ("AVM/telemetry/autobus/hybrid", 1), ("AVM/telemetry/autobus/electric", 1)])

# The callback for when an automatic connection made by loop_start() or loop_forvere() failed to establish 
def on_connect_fail(client, userdata):
    print("Failed to establish the automatic TCP (re)connection to the broker made by loop_forever()")

# The callback for when a PUBLISH message is received from the server.
def on_message(client: mqtt.Client, userdata: dict, msg: mqtt.MQTTMessage):
    payload = json.loads(msg.payload.decode())
    last = 30

    # Verifica messaggio duplicato
    if msg.dup:
        print("Gestione duplicato")

        # Se il messaggio duplicato non è presente nel dizionario degli ultimi 'last' messaggi, allora non è stato
        # elaborato
        if userdata.get(msg.mid) == None:
            # Verifica lunghezza dizionario
            if len(userdata) < last:
                # Se al di sotto di 'last' allora aggiunta del messaggio
                userdata[msg.mid] = payload["timestamp"]
            else:
                # Se uguale o al di sopra di 'last' allora rimozione dei valori più vecchi dal dizionario e aggiunta del
                # nuovo elemento
                earliest = min(userdata.values())
                earliest_keys = [k for k, val in userdata.items() if val == earliest]
                for k in earliest_keys:
                    userdata.pop(k)

                userdata[msg.mid] = payload["timestamp"]
            
            print(f"{msg.topic} {str(payload)} duplicato\n")
    # Messaggio originale
    else:
        # Verifica lunghezza dizionario
        if len(userdata) < last:
            # Se al di sotto di 'last' allora aggiunta del messaggio
            userdata[msg.mid] = payload["timestamp"]
        else:
            # Se uguale o al di sopra di 'last' allora rimozione dei valori più vecchi dal dizionario e aggiunta del
            # nuovo elemento
            earliest = min(userdata.values())
            earliest_keys = [k for k, val in userdata.items() if val == earliest]
            for k in earliest_keys:
                userdata.pop(k)

            userdata[msg.mid] = payload["timestamp"]
        
        print(f"{msg.topic} {str(payload)} originale\n")

# Installazione handler del segnale CTRL+C
signal.signal(signalnum=signal.SIGINT, handler=signal_handler)

mqttc = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2, client_id="AVM_telemetry_consumer", clean_session=False)
mqttc.on_connect = on_connect
mqttc.on_connect_fail = on_connect_fail
mqttc.on_subscribe = on_subscribe
mqttc.on_message = on_message
mqttc.user_data_set({})

try:
    mqttc.connect(host="localhost", port=1883, keepalive=60)
except socket.gaierror:
    sys.stderr.write("Errore! Impossibile risolvere l'indirizzo fornito\n")
    exit(-1)
except ConnectionRefusedError:
    sys.stderr.write("Errore! Connessione rifiutata\n")
    exit(-2)

# Blocking call that processes network traffic, dispatches callbacks and
# handles reconnecting.
# Other loop*() functions are available that give a threaded interface and a
# manual interface.
mqttc.loop_forever()
