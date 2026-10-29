import json
import os
import signal
import socket
import sys
import uuid

import paho.mqtt.client as mqtt
import paho.mqtt.reasoncodes as mqttrc
from kafka.errors import (KafkaError, KafkaTimeoutError, NoBrokersAvailable,
                          TopicAlreadyExistsError)
from paho.mqtt.enums import MQTTErrorCode

from avm.check_helpers import check_brokers, check_mqtt_host, check_mqtt_port
from avm.kafka_helpers import (_close_kafka_admin, _close_kafka_producer,
                               _close_kafka_producer_without_flush,
                               _create_kafka_admin, _create_kafka_producer,
                               _ensure_topics)


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
    def __init__(self, host_mqtt: str, port_mqtt: int, brokers_kafka: list[str]) -> None:
        # MQTT setup
        self._host_mqtt = host_mqtt
        self._port_mqtt = port_mqtt
        self._mqtt_client = self._setup_mqtt()

        # Kafka setup
        self._brokers_kafka = brokers_kafka.copy()
        self._kafka_producer, self._kafka_admin = self._setup_kafka()
        self._partitions = 2
        self._replication = 3
        self._min_insync_replicas = 2

    # Setup MQTT - metodo necessario alla creazione del client MQTT specificando versione delle callback, client_id 
    # e sessione persistente. Vengono inoltre specificate le relative callback necessarie ai fini di corretta gestione
    # di connessione, fallimento alla riconessione automatica, subscription e ricezione di un messaggio
    def _setup_mqtt(self):
        # Preparazione client_id per consumer MQTT
        consumer_client_id = "AVM_telemetry_consumer"

        # Setup client MQTT
        mqttc = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2, client_id=consumer_client_id, clean_session=False)
        mqttc.on_connect = self._on_connect
        mqttc.on_connect_fail = self._on_connect_fail
        mqttc.on_subscribe = self._on_subscribe
        mqttc.on_message = self._on_message
        mqttc.user_data_set({})

        # Inizializzazione var per contenere return value della connessione al broker MQTT
        err = None

        try:
            # Connessione verso il broker MQTT
            err = mqttc.connect(host=self.get_host_mqtt(), port=self.get_port_mqtt(), keepalive=60)
        except socket.gaierror:
            sys.stderr.write("Errore! Impossibile risolvere l'indirizzo fornito\n")
            sys.exit(-9)
        except ConnectionRefusedError:
            sys.stderr.write("Errore! Connessione MQTT rifiutata\n")
            sys.exit(-10)
        else:
            if err != MQTTErrorCode.MQTT_ERR_SUCCESS:
                sys.stderr.write("Errore! Connessione non avvenuta\n")
                sys.exit(-11)

            return mqttc

    # Setup Kafka - metodo necessario alla creazione del producer Kafka specificando bootstrap servers a cui deve avvenire
    # la connessione, client_id, e numero massimo di richieste "pipelined" verso il Kafka broker; inoltre necessario alla
    # creazione dell'admin Kafka specificando bootstrap servers a cui deve avvenire la connessione e client_id
    def _setup_kafka(self):
        bootstrap_servers = self.get_brokers_kafka()
        close_timeout = 5

        # Preparazione client_id per producer e admin Kafka
        #   utilizzo di PID + primi 8 caratteri esadecimali di UUID v4
        #   --> PID pensato per garantire che processi diversi non abbiano stesso UUID (prevenzione collisioni)
        #   --> UUID pensato per evitare che, nel tempo, processi diversi abbiano stesso PID (prevenzione errori dato
        #       il riutilizzo dei PID nel tempo)
        pid = os.getpid()
        short_uuid = uuid.uuid4().hex[:8]
        producer_client_id = f"AVM_telemetry_producer-{pid}-{short_uuid}"
        admin_client_id = f"AVM_telemetry_bridge_admin-{pid}-{short_uuid}"

        # Setup Kafka producer
        kafka_producer = _create_kafka_producer(brokers=bootstrap_servers, client_id=producer_client_id)

        # Setup Kafka admin
        try:
            kafka_admin = _create_kafka_admin(brokers=bootstrap_servers, client_id=admin_client_id)
        except Exception as error:
            # In caso di errore durante la creazione dell'admin Kafka si predispone la chiusura del producer Kafka creato
            # precedentemente
            try:
                _close_kafka_producer_without_flush(producer=kafka_producer, close_timeout=close_timeout)
            except Exception as close_error:
                raise Exception(f"Errore! Chiusura parziale producer fallita: {close_error}") from error
            
            raise
        else:
            return kafka_producer, kafka_admin

    # Getter 'host_mqtt' parameter
    def get_host_mqtt(self):
        return self._host_mqtt

    # Getter 'port_mqtt' parameter
    def get_port_mqtt(self):
        return self._port_mqtt

    # Getter 'brokers_kafka' parameter
    def get_brokers_kafka(self):
        return self._brokers_kafka.copy()

    # Getter 'partitions' parameter
    def get_partitions(self):
        return self._partitions
    
    # Getter 'replication' parameter
    def get_replication(self):
        return self._replication
    
    # Getter 'min_insync_replicas' parameter
    def get_min_insync_replicas(self):
        return self._min_insync_replicas

    # Getter 'mqtt_client' parameter
    def get_mqtt_client(self):
        return self._mqtt_client

    # Getter 'kafka_client' parameter
    def get_kafka_client(self):
        return self._kafka_producer
    
    # Getter 'kafka_admin' parameter
    def get_kafka_admin(self):
        return self._kafka_admin

    # Loop method - chiamata non bloccante che concede di non preoccuparsi di funzionalità utili come la riconnessione
    # automatica al broker MQTT, ma anche il processamento del traffico di rete e della gestione delle callback.
    # Creazione di un thread separato per effettuare queste operazioni
    def loop_forever(self):
        self.get_mqtt_client().loop_forever()

    # Forward message method - metodo necessario all'inoltro dei messaggi verso il sistema di Ingestion Kafka, viene previsto
    # un dizionario utile al mantenimento degli ultimi 'userdata_dimension' messaggi entrati nel bridge, in modo che 
    # nell'eventualità di messaggi duplicati sia possibile distinguere tra messaggio duplicato ma già processato e messaggio
    # duplicato ma ancora non processato, di conseguenza il metodo si occupa dell'aggiornamento di quest'ultimo.
    # Successivamente avviene l'effettivo invio del messaggio al sistema di Ingestion Kafka e l'attesa del risultato 
    # dell'operazione di invio
    def _forward_message(self, mqtt_userdata: dict, userdata_dimension: int, message: mqtt.MQTTMessage, message_payload: dict, fwd_topic: str, fwd_processing_topic: str, dup_flag: bool, result_timeout=60):
        # Verifica lunghezza dizionario
        if len(mqtt_userdata) < userdata_dimension:
            # Se al di sotto di 'userdata_dimension' allora aggiunta del messaggio
            mqtt_userdata[message.mid] = message_payload["timestamp"]
        else:
            # Se uguale o al di sopra di 'userdata_dimension' allora rimozione dei valori più vecchi dal dizionario e
            # aggiunta del nuovo elemento
            earliest = min(mqtt_userdata.values())
            earliest_keys = [k for k, val in mqtt_userdata.items() if val == earliest]
            for k in earliest_keys:
                mqtt_userdata.pop(k)

            mqtt_userdata[message.mid] = message_payload["timestamp"]
        
        print(f"{message.topic} {str(message_payload)} ", end="")
        if dup_flag:
            print("duplicato")
        else:
            print("originale")

        # Invio del messaggio verso il broker Kafka con inclusione degli header per indicare l'encoding del
        # contenuto
        future_telemetry = self.get_kafka_client().send(topic=fwd_topic, value=message.payload, headers=[("content-encoding", b"JSON")])
        # Verifica della ricezione di un messaggio da uno dei topic di allarme, in questo caso il messaggio non deve
        # essere processato, ma solamente memorizzato
        if "alarm" not in fwd_topic:
            # Invio del messaggio verso il broker Kafka al topic dedito al processing della telemetria ricevuta
            future_processing = self.get_kafka_client().send(topic=fwd_processing_topic, value=message.payload, headers=[("content-encoding", b"JSON")])

        try:
            # Attesa dell'effettivo invio del messaggio
            result_telemetry = future_telemetry.get(timeout=result_timeout)
            # Verifica della ricezione di un messaggio da uno dei topic di allarme, in questo caso il messaggio non
            # deve essere processato, ma solamente memorizzato
            if "alarm" not in fwd_topic:
                # Attesa dell'effettivo invio del messaggio
                result_processing = future_processing.get(timeout=result_timeout)
        except KafkaTimeoutError:
            sys.stderr.write("\nErrore! Fallita attesa dell'effettivo invio del messaggio, timeout scaduto\n")
        except KafkaError:
            sys.stderr.write("\nErrore! Fallita attesa dell'effettivo invio del messaggio\n")
        else:
            print(f"\nMessaggio inoltrato dal topic MQTT {message.topic} al topic Kafka {fwd_topic}, con offset {result_telemetry.offset}")
            # Verifica della ricezione di un messaggio da uno dei topic di allarme, in questo caso il messaggio non
            # deve essere processato, ma solamente memorizzato, per cui la stampa che riguarda il topic di
            # processamento non ha alcun senso
            if "alarm" not in fwd_topic:
                print(f"Messaggio inoltrato dal topic MQTT {message.topic} al topic Kafka {fwd_processing_topic}, con offset {result_processing.offset}\n")
            else:
                print()

    # on_subscribe - callback necessaria per il protocollo di comunicazione MQTT per gestire il momento in cui
    # il client riceve una risposta SUBACK dal broker
    def _on_subscribe(self, client, userdata, mid, reason_code_list: list[mqttrc.ReasonCodes], properties):
        # Dato che la subscription è multipla (a più topic), reason_code_list contiene
        # più entry
        if reason_code_list[0].is_failure:
            print(f"Il broker ha rifiutato la subscription al topic AVM/telemetry/autobus/termic : {reason_code_list[0]}\n")
        elif reason_code_list[1].is_failure:
            print(f"Il broker ha rifiutato la subscription al topic AVM/telemetry/autobus/hybrid : {reason_code_list[1]}\n")
        elif reason_code_list[2].is_failure:
            print(f"Il broker ha rifiutato la subscription al topic AVM/telemetry/autobus/electric : {reason_code_list[2]}\n")
        elif reason_code_list[3].is_failure:
            print(f"Il broker ha rifiutato la subscription al topic AVM/alarm/autobus/enginelight : {reason_code_list[3]}\n")
        elif reason_code_list[4].is_failure:
            print(f"Il broker ha rifiutato la subscription al topic AVM/alarm/autobus/panicbutton : {reason_code_list[4]}\n")
        else:
            print(f"Il broker ha messo a disposizione la seguente QoS:")
            print(f"\tAVM/telemetry/autobus/termic : {reason_code_list[0].value}")
            print(f"\tAVM/telemetry/autobus/hybrid : {reason_code_list[1].value}")
            print(f"\tAVM/telemetry/autobus/electric : {reason_code_list[2].value}\n")
            print(f"\tAVM/alarm/autobus/enginelight : {reason_code_list[3].value}")
            print(f"\tAVM/alarm/autobus/panicbutton : {reason_code_list[4].value}\n")

    # on_connect - callback necessaria per il protocollo di comunicazione MQTT per gestire il momento in cui 
    # il client riceve una risposta CONNACK dal server (broker RabbitMQ) - firma prestabilita
    def _on_connect(self, client: mqtt.Client, userdata, flags: mqtt.ConnectFlags, reason_code: mqttrc.ReasonCode, properties):
        if reason_code.is_failure:
            print(f"\nFallimento connessione: {reason_code}. loop_forever() proverà a riconnettersi\n")
        else:
            print(f"\nConnessione con result code {reason_code}")
            print("Il broker detiene ancora informazioni per il client: ", end="")
            if flags.session_present:
                print("SI (Persistent Session Attiva)\n")
            else:
                print("NO")

                # Iscrizione ai topic all'interno della callback on_connect() implica che se la connessione viene persa e
                # viene effettuata la riconnessione, allora le iscrizioni saranno effettuate di nuovo. Questo assicura che le
                # iscrizioni siano persistenti alle riconnessioni
                client.subscribe(topic=[("AVM/telemetry/autobus/termic", 1), ("AVM/telemetry/autobus/hybrid", 1), ("AVM/telemetry/autobus/electric", 1), ("AVM/alarm/autobus/enginelight", 1), ("AVM/alarm/autobus/panicbutton", 1)])

    # on_connect_fail - callback necessaria per il protocollo di comunicazione MQTT per gestire il momento in cui
    # avviene il fallimento nello stabilire una connessione automatica da parte di loop_forever()
    def _on_connect_fail(self, client, userdata):
        print("Fallito stabilimento della (ri)connessione TCP automatica verso il broker da parte di loop_forever()")

    # on_message - callback necessaria per il protocollo di comunicazione MQTT per gestire il momento in cui 
    # un messaggio PUBLISH viene ricevuto dal server
    def _on_message(self, client: mqtt.Client, userdata: dict, msg: mqtt.MQTTMessage):
        # Conversione MQTT topic a Kafka topic (rimpiazzo / con .)
        kafka_topic = msg.topic.replace("/", ".")
        # Set up topic dedito all'inoltro dei messaggi verso il nodo di Processing
        flink_kafka_topic = ""

        # Verifica della ricezione di un messaggio da uno dei topic di allarme, in questo caso il messaggio non deve essere
        # processato, ma solamente memorizzato
        if "alarm" not in kafka_topic:
            # Formazione Kafka topic a cui inviare i dati da processare (rimpiazzo 'telemetry' con 'processing' e inserimento di
            # un livello in più, ossia 'data' al di sopra della tipologia di motorizzazione)
            # --> ramificazione differente nell'albero dei topic
            flink_kafka_topic_temp = kafka_topic.replace("telemetry", "processing")
            elements_flink_kafka_topic = flink_kafka_topic_temp.split(".")
            elements_flink_kafka_topic.insert(-1, "data")
            delim = "."
            flink_kafka_topic = delim.join(elements_flink_kafka_topic)

        payload = json.loads(msg.payload.decode())
        # Dimensionamento dizionario degli ultimi messaggi
        last = 120

        # Creazione del topic di telemetria nel cluster Kafka con i parametri di config appropriati
        _ensure_topics(admin=self.get_kafka_admin(), topics=[kafka_topic], replication=self.get_replication(), partitions=self.get_partitions(), min_insync_replicas=self.get_min_insync_replicas())

        # Verifica della ricezione di un messaggio da uno dei topic di allarme, in questo caso il messaggio non deve essere
        # processato, ma solamente memorizzato
        if "alarm" not in kafka_topic:
            # Creazione del topic di processing nel cluster Kafka con i parametri di config di appropriati (partitions a default)
            _ensure_topics(admin=self.get_kafka_admin(), topics=[flink_kafka_topic], replication=self.get_replication(), min_insync_replicas=self.get_min_insync_replicas())

        # Verifica messaggio duplicato
        if msg.dup:
            print(f"Gestione duplicato, DUP flag: {msg.dup}")

            # Se il messaggio duplicato non è presente nel dizionario degli ultimi 'last' messaggi, allora non è stato
            # elaborato
            if userdata.get(msg.mid) == None:
                # Inoltro del messaggio al sistema di Ingestion e aggiornamento del dizionario degli utlimi 'last' messaggi
                self._forward_message(mqtt_userdata=userdata, userdata_dimension=last, message=msg, message_payload=payload, fwd_topic=kafka_topic, fwd_processing_topic=flink_kafka_topic, dup_flag=msg.dup)
            else:
                # Il messaggio duplicato è presente nel dizionario degli ultimi 'last' messaggi, quindi è già stato
                # elaborato
                print(f"Messaggio già processato e inoltrato dal topic MQTT {msg.topic} al topic Kafka {kafka_topic}")
                # Verifica della ricezione di un messaggio da uno dei topic di allarme, in questo caso il messaggio non
                # deve essere processato, ma solamente memorizzato, per cui la stampa che riguarda il topic di
                # processamento non ha alcun senso
                if "alarm" not in kafka_topic:
                    print(f"Messaggio già processato e inoltrato dal topic MQTT {msg.topic} al topic Kafka {flink_kafka_topic}\n")
                else:
                    print()
        # Messaggio originale
        else:
            # Inoltro del messaggio al sistema di Ingestion e aggiornamento del dizionario degli utlimi 'last' messaggi
            self._forward_message(mqtt_userdata=userdata, userdata_dimension=last, message=msg, message_payload=payload, fwd_topic=kafka_topic, fwd_processing_topic=flink_kafka_topic, dup_flag=msg.dup)

    # Stop method - prevede lo stop del bridge a seguito della ricezione di un segnale SIGINT (CTRL+C), per una 
    # graceful disconnection viene eseguito il metodo disconnect(.) per la disconnessione dal broker MQTT, la chiusura
    # del producer e dell'admin Kafka, inoltre viene stampato a video un messaggio di informazione
    def stop_bridge(self):
        close_timeout = 5
        flush_timeout = 2.5

        # Disconnessione dal broker MQTT
        mqtt_error = self.get_mqtt_client().disconnect()

        producer_error = None
        admin_error = None

        # Chiusura producer e admin Kafka
        try:
            _close_kafka_producer(producer=self.get_kafka_client(), flush_timeout=flush_timeout, close_timeout=close_timeout)
        except KafkaTimeoutError as error:
            producer_error = error
            sys.stderr.write(f"{error}\n")
        finally:
            try:
                _close_kafka_admin(admin=self.get_kafka_admin())
            except KafkaError as error:
                admin_error = error
                sys.stderr.write(f"{error}\n")

        # Predisposizione variabili necessarie a determinare il successo o meno delle operazioni di disconnessione
        mqtt_success = mqtt_error == MQTTErrorCode.MQTT_ERR_SUCCESS
        kafka_success = producer_error is None and admin_error is None

        # Definizione e stampa messaggio di uscita
        if mqtt_success and kafka_success:
            print("\nConnessione al broker Kafka interrotta, e connessione al broker MQTT cessata con successo\n")
        elif mqtt_success:
            print("\nCessazione connessione al broker Kafka fallita, e connessione al broker MQTT cessata con successo\n")
        elif kafka_success:
            print("\nConnessione al broker Kafka interrotta, e connessione al broker MQTT cessata senza successo\n")
        else:
            print("\nCessazione connessioni al broker Kafka e al broker MQTT fallite entrambi\n")


# Check CMD Line Arguments - verifica dei parametri passati da linea di comando, in particolare relativi a host e porta
# del broker MQTT e dei broker Kafka; per tutti gli host viene controllato solamente se l'indirizzo è una stringa
# non vuota, mentre per tutte le porte si opera un controllo sulla validità del numero e se il numero di porta sia
# uno di quelli standard, per MQTT 1883 o 8883, mentre per Kafka 9092, 9094 o 9096. Inoltre viene operata una ulteriore
# verifica sul formato dell'indirizzo IPv4 del broker Kafka, in particolare viene controllato che sia esattamente nella 
# forma 'host:port'
def check_cmd_line_args(host_mqtt: str, port_mqtt: str, brokers_kafka: list[str]):
    if type(host_mqtt) is not str:
        raise TypeError(f"Errore! Il tipo del parametro 'host_mqtt' passato deve essere 'str'. Ricevuto {type(host_mqtt)}")
    
    if type(port_mqtt) is not str:
        raise TypeError(f"Errore! Il tipo del parametro 'port_mqtt' passato deve essere 'str'. Ricevuto {type(port_mqtt)}")
    
    if type(brokers_kafka) is not list:
        raise TypeError(f"Errore! Il tipo del parametro 'brokers_kafka' passato deve essere 'list'. Ricevuto {type(brokers_kafka)}")

    input_check = True

    # Inizializzazione parametri di ritorno
    mqtt_host = host_mqtt
    mqtt_port = 0
    kafka_brokers = []

    # Host MQTT
    check_mqtt_host(host=mqtt_host, input_check=input_check)

    # Port MQTT
    mqtt_port = check_mqtt_port(port=port_mqtt, input_check=input_check)

    # Brokers Kafka
    kafka_brokers = check_brokers(brokers=brokers_kafka)

    return mqtt_host, mqtt_port, kafka_brokers


# main() method - esecuzione del sistema di bridging tra MQTT e Kafka relativo agli autobus smart, con controllo dei
# parametri passati da linea di comando, instanziazione dell'oggetto Bridge e azionamento del meccanismo di funzionamento
def main():
    # Verifica corretta invocazione del programma
    if len(sys.argv) < 4 or len(sys.argv) > 6:
        sys.stderr.write(f"Errore! Uso coretto del programma: python[3] {sys.argv[0]} $host_mqtt $porta_mqtt $host_kafka-1:$porta_kafka-1 [$host_kafka-2:$porta_kafka-2 $host_kafka-3:$porta_kafka-3]\n")
        sys.stderr.write("\t$host_mqtt = 'host_MQTT_broker'\n")
        sys.stderr.write("\t$porta_mqtt = '1883' | '8883'\n")
        sys.stderr.write("\t$host_kafka-* = 'host_Kafka_broker'\n")
        sys.stderr.write("\t$porta_kafka-* = '9092 | 9094 | 9096'\n")
        sys.exit(-1)

    # Installazione handler del segnale CTRL+C
    signal.signal(signalnum=signal.SIGINT, handler=signal_handler)

    # Verifica validità indirizzo broker MQTT e indirizzo broker Kafka
    try:
        host_mqtt, port_mqtt, brokers_kafka_list = check_cmd_line_args(host_mqtt=sys.argv[1], port_mqtt=sys.argv[2], brokers_kafka=sys.argv[3:].copy())
    except TypeError as error:
        sys.stderr.write(f"{error}\n")
        sys.exit(-2)
    except ValueError as error:
        sys.stderr.write(f"{error}\n")
        sys.exit(-3)
    except SyntaxError as error:
        sys.stderr.write(f"{error}\n")
        sys.exit(-4)
    
    # Utilizzo dell'oggetto globale di bridging MQTT to Kafka
    global bridge_mqtt_to_kafka

    # Instanziazione dell'oggetto Bridge MQTT to Kafka
    try:
        bridge_mqtt_to_kafka = BridgeMQTTKafka(host_mqtt=host_mqtt, port_mqtt=port_mqtt, brokers_kafka=brokers_kafka_list)
    except NoBrokersAvailable as error:
        sys.stderr.write(f"{error}\n")
        sys.exit(-5)
    except ValueError as error:
        sys.stderr.write(f"{error}\n")
        sys.exit(-6)
    except KafkaError as error:
        sys.stderr.write(f"{error}\n")
        sys.exit(-7)
    except Exception as error:
        sys.stderr.write(f"{error}\n")
        sys.exit(-8)

    # Messa in esecuzione del bridge
    try:
        bridge_mqtt_to_kafka.loop_forever()
    except TopicAlreadyExistsError as error:
        sys.stderr.write(f"{error}\n")
        sys.exit(-9)
    except KafkaError as error:
        sys.stderr.write(f"{error}\n")
        sys.exit(-10)


if __name__ == "__main__":
    main()
