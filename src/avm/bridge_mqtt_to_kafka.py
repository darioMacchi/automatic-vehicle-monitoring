import json
import os
import signal
import socket
import sys
import uuid

import paho.mqtt.client as mqtt
import paho.mqtt.reasoncodes as mqttrc
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import (KafkaError, KafkaTimeoutError, NoBrokersAvailable,
                          TopicAlreadyExistsError)
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
                print("Errore! Connessione non avvenuta\n")
                sys.exit(-11)

            return mqttc

    # Setup Kafka - metodo necessario alla creazione del producer Kafka specificando bootstrap servers a cui deve avvenire
    # la connessione, client_id, e numero massimo di richieste "pipelined" verso il Kafka broker; inoltre necessario alla
    # creazione dell'admin Kafka specificando bootstrap servers a cui deve avvenire la connessione e client_id
    def _setup_kafka(self):
        bootstrap_servers = self.get_brokers_kafka()

        # Preparazione client_id per producer e admin Kafka
        #   utilizzo di PID + primi 8 caratteri esadecimali di UUID v4
        #   --> PID pensato per garantire che processi diversi non abbiano stesso UUID (prevenzione collisioni)
        #   --> UUID pensato per evitare che, nel tempo, processi diversi abbiano stesso PID (prevenzione errori dato
        #       il riutilizzo dei PID nel tempo)
        pid = os.getpid()
        short_uuid = uuid.uuid4().hex[:8]
        producer_client_id = f"AVM_telemetry_producer-{pid}-{short_uuid}"
        admin_client_id = f"AVM_telemetry_bridge_admin-{pid}-{short_uuid}"

        # Gestione errore di connessione a broker non disponibili alla connessione
        try:
            # Instanziazione Kafka producer con bootstrap servers a cui deve avvenire la connessione, client_id, e
            # numero massimo di richieste "pipelined" verso i Kafka broker

            # Setup Kafka producer
            kafka_prod = KafkaProducer(bootstrap_servers=bootstrap_servers, client_id=producer_client_id, max_in_flight_requests_per_connection=1, allow_auto_create_topics=False)

            # Instanziazione Kafka admin con bootstrap servers a cui deve avvenire la connessione, e client_id

            # Setup Kafka admin
            kafka_admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers, client_id=admin_client_id)
        except NoBrokersAvailable:
            print("Errore! Nessun broker Kafka disponibile per la connessione")
            sys.exit(-12)

        return kafka_prod, kafka_admin

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

    # Metodo dedito alla creazione dei topic con i parametri di configurazione desiderati, ossia per fare in modo che
    # il topic sia gestito tra più broker Kafka, con un certo grado di partizione, replica e repliche in-sync (ISR).
    # Verifica se il topic è già presente nel cluster, altrimenti si incarica della creazione
    def _create_topic_if_not_exist(self, topic: str, partitions=1, replication=1, min_insync_replicas=1):
        admin = self.get_kafka_admin()

        try:
            # Acquisizione topic presenti nel cluster
            topics_already_created = admin.list_topics()
        except KafkaError:
            sys.stderr.write("Errore! Impossibile ottenere il listato dei topic presenti nel cluster\n")
            sys.exit(-13)

        # Verifica topic assente all'interno del cluster
        if topic not in topics_already_created:
            # Creazione topic attraverso l'oggetto NewTopic specificando:
            #   name: nome del topic
            #   num_partitions: numero di partizioni assegnate al topic
            #   replication_factor: grado di replicazione del topic, ossia su quanti broker deve essere replicato
            #   topic_configs: specifica del numero minimo di repliche che deve essere in-sync, ossia allineate
            #                  con il leader rispetto alle partizioni del topic
            topic_to_create = NewTopic(
                name=topic,
                num_partitions=partitions,
                replication_factor=replication,
                topic_configs={
                    "min.insync.replicas":str(min_insync_replicas)
                }
            )

            try:
                # Creazione del topic all'interno del cluster
                admin.create_topics(new_topics=[topic_to_create])
                print(f"\nTopic '{topic}' creato\n")
            except TopicAlreadyExistsError:
                sys.stderr.write("Errore! Impossibile creare un topic che esiste già nel cluster\n")
                sys.exit(-14)
            except KafkaError:
                sys.stderr.write("Errore!\n")
                sys.exit(-15)

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
        else:
            print(f"Il broker ha messo a disposizione la seguente QoS:")
            print(f"\tAVM/telemetry/autobus/termic : {reason_code_list[0].value}")
            print(f"\tAVM/telemetry/autobus/hybrid : {reason_code_list[1].value}")
            print(f"\tAVM/telemetry/autobus/electric : {reason_code_list[2].value}\n")

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
                client.subscribe(topic=[("AVM/telemetry/autobus/termic", 1), ("AVM/telemetry/autobus/hybrid", 1), ("AVM/telemetry/autobus/electric", 1)])

    # on_connect_fail - callback necessaria per il protocollo di comunicazione MQTT per gestire il momento in cui
    # avviene il fallimento nello stabilire una connessione automatica da parte di loop_forever()
    def _on_connect_fail(self, client, userdata):
        print("Fallito stabilimento della (ri)connessione TCP automatica verso il broker da parte di loop_forever()")

    # on_message - callback necessaria per il protocollo di comunicazione MQTT per gestire il momento in cui 
    # un messaggio PUBLISH viene ricevuto dal server
    def _on_message(self, client: mqtt.Client, userdata: dict, msg: mqtt.MQTTMessage):
        # Converisone MQTT topic a Kafka topic (rimpiazzo / con .)
        kafka_topic = msg.topic.replace("/", ".")
        payload = json.loads(msg.payload.decode())
        # TODO
        # Rivedere dimensionamento 
        last = 30

        # Creazione del topic nel cluster Kafka con i parametri di config appropriati
        self._create_topic_if_not_exist(topic=kafka_topic, partitions=self.get_partitions(), replication=self.get_replication(), min_insync_replicas=self.get_min_insync_replicas())

        # Verifica messaggio duplicato
        if msg.dup:
            print(f"Gestione duplicato, DUP flag: {msg.dup}")

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
                try:
                    # Attesa dell'effettivo invio del messaggio
                    result = future.get(timeout=60)
                except KafkaTimeoutError:
                    sys.stderr.write("\nErrore! Fallita attesa dell'effettivo invio del messaggio, timeout scaduto\n")
                except KafkaError:
                    sys.stderr.write("\nErrore! Fallita attesa dell'effettivo invio del messaggio\n")
                else:
                    print(f"\nMessaggio inoltrato dal topic MQTT {msg.topic} al topic Kafka {kafka_topic}, con offset {result.offset}\n")
            else:
                # Il messaggio duplicato è presente nel dizionario degli ultimi 'last' messaggi, quindi è già stato
                # elaborato
                print(f"Messaggio già processato e inoltrato dal topic MQTT {msg.topic} al topic Kafka {kafka_topic}\n")
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
            try:
                # Attesa dell'effettivo invio del messaggio
                result = future.get(timeout=60)
            except KafkaTimeoutError:
                    sys.stderr.write("\nErrore! Fallita attesa dell'effettivo invio del messaggio, timeout scaduto\n")
            except KafkaError:
                sys.stderr.write("\nErrore! Fallita attesa dell'effettivo invio del messaggio\n")
            else:
                print(f"\nMessaggio inoltrato dal topic MQTT {msg.topic} al topic Kafka {kafka_topic}, con offset {result.offset}\n")

    # Stop method - prevede lo stop del bridge a seguito della ricezione di un segnale SIGINT (CTRL+C), per una 
    # graceful disconnection viene eseguito il metodo disconnect(.) per la disconnessione dal broker MQTT, la chiusura
    # del producer e dell'admin Kafka con il metodo close(.), inoltre viene stampato a video un mesaggio di informazione
    def stop_bridge(self):
        close_timeout = 5
        flush_timeout = 2.5

        # Disconnessione dal broker MQTT
        err = self.get_mqtt_client().disconnect()
        # Chiusura producer Kafka
        try:
            self.get_kafka_client().flush(timeout=flush_timeout)
        except Exception:
            print("Errore! Flush fallito")
        finally:
            try:
                self.get_kafka_client().close(timeout=close_timeout)

                # Chiusura admin Kafka
                self.get_kafka_admin().close()
            except Exception:
                print(f"\nCessazione connessione al broker Kafka fallita, e connessione al broker MQTT cessata con ", end="")
                print("successo\n" if err == MQTTErrorCode.MQTT_ERR_SUCCESS else "insuccesso\n")
            else:
                print(f"\nConnessione al broker Kafka interrotta, e connessione al broker MQTT cessata con ", end="")
                print("successo\n" if err == MQTTErrorCode.MQTT_ERR_SUCCESS else "insuccesso\n")


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
    
    # Inizializzazione parametri di ritorno
    mqtt_host = ""
    mqtt_port = 0

    kafka_brokers = []

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

    # Brokers Kafka
    for broker in brokers_kafka:
        # Rimozione eventuali blank spaces
        broker = broker.replace(" ", "")

        try:
            # Controllo formato stringa broker strettamente del tipo 'host:porta'
            host, port = broker.split(":")
        except Exception:
            sys.stderr.write("Errore! Gli argomenti $host_kafka-* e $port_kafka-* passati da linea di comando devono essere nel formato '$host_kafka-*:port_kafka-*'\n")
            sys.exit(-5)

        # Host Kafka
        # Check stringa non vuota
        if host == "":
            sys.stderr.write("Errore! L'argomento $host_kafka-* deve essere un indirizzo non nullo\n")
            sys.exit(-6)

        # Port Kafka
        # Check numero valido
        try:
            port = int(port)
        except ValueError:
            sys.stderr.write("Errore! L'argomento $porta_kafka-* passato da linea di comando non è un numero valido\n")
            sys.exit(-7)

        # Check porta
        if port != 9092 and port != 9094 and port != 9096:
            sys.stderr.write("Errore! L'argomento $porta_kafka-* deve essere una porta Kafka valida: 9092 | 9094 | 9096\n")
            sys.exit(-8)
        
        kafka_brokers.append(broker)

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
    host_mqtt, port_mqtt, brokers_kafka_list = check_cmd_line_args(host_mqtt=sys.argv[1], port_mqtt=sys.argv[2], brokers_kafka=sys.argv[3:].copy())

    # Utilizzo dell'oggetto globale di bridging MQTT to Kafka
    global bridge_mqtt_to_kafka

    # Instanziazione dell'oggetto Bridge MQTT to Kafka
    bridge_mqtt_to_kafka = BridgeMQTTKafka(host_mqtt=host_mqtt, port_mqtt=port_mqtt, brokers_kafka=brokers_kafka_list)

    # Messa in esecuzione del bridge
    bridge_mqtt_to_kafka.loop_forever()


if __name__ == "__main__":
    main()
