import json
import signal
import socket
import sys

import paho.mqtt.client as mqtt
import paho.mqtt.reasoncodes as mqttrc
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from paho.mqtt.enums import MQTTErrorCode


# Oggetto Bridge MQTT to Kafka
bridge_mqtt_to_kafka = None


# Handler segnale CTRL+C
def signal_handler(sig_num: int, frame):
    sig_name = signal.Signals(sig_num).name

    # Utilizzo dell'oggetto globale di bridging MQTT to Kafka per accedere all'istanza creata e agire su di essa per
    # una 'graceful disconnection'
    global bridge_mqtt_to_kafka

    # Stop oggetto bridge MQTT to Kafka
    bridge_mqtt_to_kafka.stop_bridge()

    # Terminazione
    print(f"Esecuzione interrotta dal segnale {sig_name}")
    sys.exit(0)


# Oggetto Bridge MQTT to Kafka - permette di avviare un MQTT consumer che recepisce i messaggi provenienti dal broker
# MQTT e opera da bridge verso il broker Kafka, ossia il layer di Ingestion del sistema di monitoraggio telemetria 
# autobus
class BridgeMQTTKafka:
    def __init__(self, host_mqtt: str, port_mqtt: int, host_kafka: str, port_kafka: int) -> None:
        # MQTT setup
        self._host_mqtt = host_mqtt
        self._port_mqtt = port_mqtt
        self._mqtt_client = self.setup_mqtt()

        # Kafka setup
        self._host_kafka = host_kafka
        self._port_kafka = port_kafka
        self._kafka_producer = self.setup_kafka()

    # Setup MQTT - metodo necessario alla creazione del client MQTT specificando versione delle callback, client_id 
    # e sessione persistente. Vengono inoltre specificate le relative callback necessarie ai fini di corretta gestione
    # di connessione, fallimento alla riconessione automatica, subscription e ricezione di un messaggio
    def setup_mqtt(self):
        # Setup client MQTT
        mqttc = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2, client_id="AVM_telemetry_consumer", clean_session=False)
        mqttc.on_connect = self.on_connect
        mqttc.on_connect_fail = self.on_connect_fail
        mqttc.on_subscribe = self.on_subscribe
        mqttc.on_message = self.on_message
        mqttc.user_data_set({})

        # Inizializzazione var per contenere return value della connessione al broker MQTT
        err = None

        try:
            # Connessione verso il broker MQTT
            err = mqttc.connect(host=self.get_host_mqtt(), port=self.get_port_mqtt(), keepalive=60)
        except socket.gaierror:
            sys.stderr.write("Errore! Impossibile risolvere l'indirizzo fornito\n")
            sys.exit(-8)
        except ConnectionRefusedError:
            sys.stderr.write("Errore! Connessione MQTT rifiutata\n")
            sys.exit(-9)
        else:
            if err != MQTTErrorCode.MQTT_ERR_SUCCESS:
                print("Errore! Connessione non avvenuta\n")
                sys.exit(-10)

            return mqttc

    # Setup Kafka - metodo necessario alla creazione del producer Kafka specificando bootstrap server a cui deve avvenire
    # la connessione, client_id, e numero massimo di richieste "pipelined" verso il Kafka broker
    def setup_kafka(self):
        bootstrap_server = self.get_host_kafka() + ":" + str(self.get_port_kafka())

        # Gestione errore di connessione ad un broker non disponibile alla connessione
        try:
            # Instanziazione Kafka producer con bootstrap server a cui deve avvenire la connessione, client_id, e
            # numero massimo di richieste "pipelined" verso il Kafka broker

            # Setup Kafka producer
            kafka_prod = KafkaProducer(bootstrap_servers=[bootstrap_server], client_id="AVM_telemetry_producer", max_in_flight_requests_per_connection=1)
        except NoBrokersAvailable:
            print("Errore! Nessun broker Kafka disponibile per la connessione")
            sys.exit(-11)

        return kafka_prod

    # Getter 'host_mqtt' parameter
    def get_host_mqtt(self):
        return self._host_mqtt

    # Setter 'host_mqtt' parameter
    def set_host_mqtt(self, host_mqtt: str):
        if type(host_mqtt) is not str:
            raise TypeError(f"Errore! Il tipo del parametro passato deve essere 'str'. Ricevuto {type(host_mqtt)}")
        
        self._host_mqtt = host_mqtt

    # Getter 'port_mqtt' parameter
    def get_port_mqtt(self):
        return self._port_mqtt

    # Setter 'port_mqtt' parameter
    def set_port_mqtt(self, port_mqtt: int):
        if type(port_mqtt) is not int:
            raise TypeError(f"Errore! Il tipo del parametro passato deve essere 'int'. Ricevuto {type(port_mqtt)}")
        
        self._port_mqtt = port_mqtt

    # Getter 'host_kafka' parameter
    def get_host_kafka(self):
        return self._host_kafka

    # Setter 'host_kafka' parameter
    def set_host_kafka(self, host_kafka: str):
        if type(host_kafka) is not str:
            raise TypeError(f"Errore! Il tipo del parametro passato deve essere 'str'. Ricevuto {type(host_kafka)}")
        
        self._host_kafka = host_kafka

    # Getter 'port_kafka' parameter
    def get_port_kafka(self):
        return self._port_kafka

    # Setter 'port_kafka' parameter
    def set_port_kafka(self, port_kafka: int):
        if type(port_kafka) is not int:
            raise TypeError(f"Errore! Il tipo del parametro passato deve essere 'int'. Ricevuto {type(port_kafka)}")
        
        self._port_kafka = port_kafka

    # Getter 'mqtt_client' parameter
    def get_mqtt_client(self):
        return self._mqtt_client

    # Getter 'kafka_client' parameter
    def get_kafka_client(self):
        return self._kafka_producer

    # Loop method - chiamata non bloccante che concede di non preoccuparsi di funzionalità utili come la riconnessione
    # automatica al broker MQTT, ma anche il processamento del traffico di rete e della gestione delle callback.
    # Creazione di un thread separato per effettuare queste operazioni
    def loop_forever(self):
        self.get_mqtt_client().loop_forever()

    # on_subscribe - callback necessaria per il protocollo di comunicazione MQTT per gestire il momento in cui
    # il client riceve una risposta SUBACK dal broker
    def on_subscribe(self, client, userdata, mid, reason_code_list: list[mqttrc.ReasonCodes], properties):
        # Dato che la subscription è multipla (a più topic), reason_code_list contiene
        # più entry
        if reason_code_list[0].is_failure:
            print(f"Il broker ha rifiutato la subscription al topic AVM/telemetry/autobus/termic : {reason_code_list[0]}\n")
        elif reason_code_list[1].is_failure:
            print(f"Il broker ha rifiutato la subscription al topic AVM/telemetry/autobus/hybrid : {reason_code_list[1]}\n")
        elif reason_code_list[2].is_failure:
            print(f"Il broker ha rifiutato la subscription al topic AVM/telemetry/autobus/electric : {reason_code_list[2]}\n")
        else:
            print(f"Il broker ha messo a disposizione la seguente QoS:")
            print(f"\tAVM/telemetry/autobus/termic : {reason_code_list[0].value}")
            print(f"\tAVM/telemetry/autobus/hybrid : {reason_code_list[1].value}")
            print(f"\tAVM/telemetry/autobus/electric : {reason_code_list[2].value}")

    # on_connect - callback necessaria per il protocollo di comunicazione MQTT per gestire il momento in cui 
    # il client riceve una risposta CONNACK dal server (broker RabbitMQ) - firma prestabilita
    def on_connect(self, client: mqtt.Client, userdata, flags: mqtt.ConnectFlags, reason_code: mqttrc.ReasonCode, properties):
        if reason_code.is_failure:
            print(f"\nFallimento connessione: {reason_code}. loop_forever() proverà a riconnettersi\n")
        else:
            print(f"\nConnessione con result code {reason_code}")
            print("Il broker detiene ancora informazioni per il client: ", end="")
            print("SI" if flags.session_present else "NO")

            # Iscrizione ai topic all'interno della callback on_connect() implica che se la connessione viene persa e
            # viene effettuata la riconnessione, allora le iscrizioni saranno effettuate di nuovo. Questo assicura che le
            # iscrizioni siano persistenti alle riconnessioni
            client.subscribe(topic=[("AVM/telemetry/autobus/termic", 1), ("AVM/telemetry/autobus/hybrid", 1), ("AVM/telemetry/autobus/electric", 1)])

    # on_connect_fail - callback necessaria per il protocollo di comunicazione MQTT per gestire il momento in cui
    # avviene il fallimento nello stabilire una connessione automatica da parte di loop_forever()
    def on_connect_fail(self, client, userdata):
        print("Fallito stabilimento della (ri)connessione TCP automatica verso il broker da parte di loop_forever()")

    # -- VERIFICA FUNZIONAMENTO --
    # on_message - callback necessaria per il protocollo di comunicazione MQTT per gestire il momento in cui 
    # un messaggio PUBLISH viene ricevuto dal server
    def on_message(self, client: mqtt.Client, userdata: dict, msg: mqtt.MQTTMessage):
        # Converisone MQTT topic a Kafka topic (rimpiazzo / con .)
        kafka_topic = msg.topic.replace("/", ".")
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
                    # Se uguale o al di sopra di 'last' allora rimozione dei valori più vecchi dal dizionario e aggiunta 
                    # del nuovo elemento
                    earliest = min(userdata.values())
                    earliest_keys = [k for k, val in userdata.items() if val == earliest]
                    for k in earliest_keys:
                        userdata.pop(k)

                    userdata[msg.mid] = payload["timestamp"]
                
                print(f"{msg.topic} {str(payload)} duplicato")

                # Invio del messaggio verso il broker Kafka con inclusione degli header per indicare l'encoding del
                # contenuto
                future = self.get_kafka_client().send(topic=kafka_topic, value=msg.payload, headers=[("content-encoding", b"JSON")])
                # Attesa dell'effettivo invio del messaggio
                result = future.get(timeout=60)
                print(f"\nMessaggio inoltrato dal topic MQTT {msg.topic} al topic Kafka {kafka_topic}, con offset {result.offset}\n")
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
            
            print(f"{msg.topic} {str(payload)} originale")

            # Invio del messaggio verso il broker Kafka con inclusione degli header per indicare l'encoding del
            # contenuto
            future = self.get_kafka_client().send(topic=kafka_topic, value=msg.payload, headers=[("content-encoding", b"JSON")])
            # Attesa dell'effettivo invio del messaggio
            result = future.get(timeout=60)
            print(f"\nMessaggio inoltrato dal topic MQTT {msg.topic} al topic Kafka {kafka_topic}, con offset {result.offset}\n")

    # Stop method - prevede lo stop del bridge a seguito della ricezione di un segnale SIGINT (CTRL+C), per una 
    # graceful disconnection viene eseguito il metodo disconnect(.) per la disconnessione dal broker MQTT e la chiusura
    # del consumer Kafka con il metodo close(.), inoltre viene stampato a video un mesaggio di informazione
    def stop_bridge(self):
        close_timeout = 5
        flush_timeout = 2.5

        # Disconnessione dal broker MQTT
        err = self.get_mqtt_client().disconnect()
        # Chiusura consumer Kafka
        try:
            self.get_kafka_client().flush(timeout=flush_timeout)
        except Exception:
            print("Errore! Flush fallito")
        finally:
            try:
                self.get_kafka_client().close(timeout=close_timeout)
            except Exception:
                print(f"\nCessazione connessione al broker Kafka fallita, e connessione al broker MQTT cessata con ", end="")
                print("successo\n" if err == MQTTErrorCode.MQTT_ERR_SUCCESS else "insuccesso\n")
            else:
                print(f"\nConnessione al broker Kafka interrotta, e connessione al broker MQTT cessata con ", end="")
                print("successo\n" if err == MQTTErrorCode.MQTT_ERR_SUCCESS else "insuccesso\n")


# Check CMD Line Arguments - verifica dei parametri passati da linea di comando, in particolare relativi a host e porta
# del broker MQTT e del broker Kafka; per entrambi gli host viene controllato solamente se l'indirizzo è una stringa
# non vuota, mentre per entrambe le porte si opera un controllo sulla validità del numero e se il numero di porta sia
# uno di quelli standard, per MQTT 1883 e 8883, mentre per Kafka 9092
def check_cmd_line_args(host_mqtt: str, port_mqtt: str, host_kafka: str, port_kafka: str):
    if type(host_mqtt) is not str:
        raise TypeError(f"Errore! Il tipo del parametro 'host_mqtt' passato deve essere 'str'. Ricevuto {type(host_mqtt)}")
    
    if type(port_mqtt) is not str:
        raise TypeError(f"Errore! Il tipo del parametro 'port_mqtt' passato deve essere 'str'. Ricevuto {type(port_mqtt)}")
    
    if type(host_kafka) is not str:
        raise TypeError(f"Errore! Il tipo del parametro 'host_kafka' passato deve essere 'str'. Ricevuto {type(host_kafka)}")
    
    if type(port_kafka) is not str:
        raise TypeError(f"Errore! Il tipo del parametro 'port_kafka' passato deve essere 'str'. Ricevuto {type(port_kafka)}")

    mqtt_host = ""
    mqtt_port = 0

    kafka_host = ""
    kafka_port = 0

    # Host MQTT
    # Check stringa non vuota
    mqtt_host = host_mqtt
    if mqtt_host == "":
        sys.stderr.write("Errore! L'argomento $host_mqtt deve essere un indirizzo non nullo\n")
        sys.exit(-2)

    # Port MQTT
    # Check numero valido
    try:
        mqtt_port = int(port_mqtt)
    except ValueError:
        sys.stderr.write("Errore! L'argomento $porta_mqtt passato da linea di comando non è un numero valido\n")
        sys.exit(-3)

    # Check porta
    if mqtt_port != 1883 and mqtt_port != 8883:
        sys.stderr.write("Errore! L'argomento $porta_mqtt deve essere una porta MQTT valida: 1883 oppure 8883 (connessioni SSL)\n")
        sys.exit(-4)

    # Host Kafka
    # Check stringa non vuota
    kafka_host = host_kafka
    if kafka_host == "":
        sys.stderr.write("Errore! L'argomento $host_kafka deve essere un indirizzo non nullo\n")
        sys.exit(-5)

    # Port Kafka
    # Check numero valido
    try:
        kafka_port = int(port_kafka)
    except ValueError:
        sys.stderr.write("Errore! L'argomento $porta_kafka passato da linea di comando non è un numero valido\n")
        sys.exit(-6)

    # Check porta
    if kafka_port != 9092:
        sys.stderr.write("Errore! L'argomento $porta_kafka deve essere una porta Kafka valida: 9092\n")
        sys.exit(-7)

    return mqtt_host, mqtt_port, kafka_host, kafka_port


# main() method - esecuzione del sistema di bridging tra MQTT e Kafka relativo agli autobus smart, con controllo dei
# parametri passati da linea di comando, instanziazione dell'oggetto Bridge e azionamento del meccanismo di funzionamento
def main():
    # Verifica corretta invocazione del programma
    if len(sys.argv) != 5:
        sys.stderr.write(f"Errore! Uso coretto del programma: python[3] {sys.argv[0]} $host_mqtt $porta_mqtt $host_kafka $porta_kafka\n")
        sys.stderr.write("\t$host_mqtt = 'host_MQTT_broker'\n")
        sys.stderr.write("\t$porta_mqtt = '1883' | '8883'\n")
        sys.stderr.write("\t$host_kafka = 'host_Kafka_broker'\n")
        sys.stderr.write("\t$porta_kafka = '9092'\n")
        sys.exit(-1)

    # Installazione handler del segnale CTRL+C
    signal.signal(signalnum=signal.SIGINT, handler=signal_handler)

    # Verifica validità indirizzo broker MQTT e indirizzo broker Kafka
    host_mqtt, port_mqtt, host_kafka, port_kafka = check_cmd_line_args(host_mqtt=sys.argv[1], port_mqtt=sys.argv[2], host_kafka=sys.argv[3], port_kafka=sys.argv[4])

    # Utilizzo dell'oggetto globale di bridging MQTT to Kafka
    global bridge_mqtt_to_kafka

    # Instanziazione dell'oggetto Bridge MQTT to Kafka
    bridge_mqtt_to_kafka = BridgeMQTTKafka(host_mqtt=host_mqtt, port_mqtt=port_mqtt, host_kafka=host_kafka, port_kafka=port_kafka)

    # Messa in esecuzione del bridge
    bridge_mqtt_to_kafka.loop_forever()


if __name__ == "__main__":
    main()
