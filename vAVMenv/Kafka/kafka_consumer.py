import json
import os
import signal
import sys
import uuid

from kafka import KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import (KafkaError, NoBrokersAvailable,
                          TopicAlreadyExistsError)


# 
consumer = None
admin = None


# Handler segnale CTRL+C
def signal_handler(sig_num: int, frame):
    sig_name = signal.Signals(sig_num).name

    global consumer
    global admin

    close_timeout = 5000

    try:
        # Chiusura consumer
        consumer.close(timeout_ms=close_timeout)
        # Chiusura admin
        admin.close()
    except Exception:
        print("Errore! Cessazione connessione al broker Kafka fallita")
    finally:
        print(f"\nEsecuzione consumer interrotta dal segnale {sig_name}")
        sys.exit(0)

# Metodo dedito alla creazione dei topic con i parametri desiderati, ossia per fare in modo che il topic sia gestito
# tra più broker Kafka, con un certo grado di partizione, replica e repliche in-sync (ISR). Verifica se il topic è già
# presente nel cluster, altrimenti si incarica della creazione
def create_topics_if_not_exist(admin: KafkaAdminClient, topics: list[str], partitions=1, replication=1, min_insync_replicas=1):
    try:
        # Acquisizione topic presenti nel cluster
        topics_created = admin.list_topics()
    except KafkaError:
        sys.stderr.write("Errore! Impossibile ottenere il listato dei topic presenti nel cluster\n")
        sys.exit(-7)

    # 
    new_topics = []

    # Per ogni topic presente all'interno della lista di topic da creare passata alla funzione, se non è già presente
    # nel cluster avviene la creazione
    for topic in topics:
        # Verifica topic assente all'interno del cluster
        if topic not in topics_created:
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

            new_topics.append(topic_to_create)

    try:
        # Creazione del topic all'interno del cluster
        admin.create_topics(new_topics=new_topics)
    except TopicAlreadyExistsError:
        sys.stderr.write("Errore! Impossibile creare un topic che esiste già nel cluster\n")
        sys.exit(-8)
    except KafkaError:
        sys.stderr.write("Errore!\n")
        sys.exit(-9)
    else:
        # 
        for topic in topics:
            # 
            if topic in new_topics:
                print(f"Topic '{topic}' creato")
            else:
                # Topic già presente nel cluster, per cui non viene fatta nessuna azione riguardante quest'ultimo
                print(f"Topic '{topic}' già presente nel cluster")

# 
def consumer_admin_setup(brokers_kafka: list[str]):
    topics_to_subscribe = ["AVM.telemetry.autobus.termic",
                          "AVM.telemetry.autobus.hybrid",
                          "AVM.telemetry.autobus.electric"]

    partitions = 2
    replication = 3
    min_insyinc_replicas = 2

    # Preparazione client_id per producer e admin Kafka
    #   utilizzo di PID + primi 8 caratteri esadecimali di UUID v4
    #   --> PID pensato per garantire che processi diversi non abbiano stesso UUID (prevenzione collisioni)
    #   --> UUID pensato per evitare che, nel tempo, processi diversi abbiano stesso PID (prevenzione errori dato
    #       il riutilizzo dei PID nel tempo)
    pid = os.getpid()
    short_uuid = uuid.uuid4().hex[:8]
    consumer_client_id = f"AVM_telemetry_consumer-{pid}-{short_uuid}"
    admin_client_id = f"AVM_telemetry_consumer_admin-{pid}-{short_uuid}"

    # Gestione errore di connessione a broker non disponibili alla connessione
    try:
        # Instanziazione Kafka consumer con iscrizione topic, assegnazione ad un consumer group, bootstrap servers a cui
        # deve avvenire la connessione, client_id, intervallo di auto commit a 4s dato che i messaggi vengono prodotti
        # ogni 5s dal sistema AVM di telemetria, e auto offset reset a earliest in modo che per politica nel momento in
        # cui avviene un errore OffsetOutOfRange ci si sposta al messaggio più vecchio possibile
        consumer = KafkaConsumer(group_id='AVM_telemetry_group', bootstrap_servers=brokers_kafka, client_id=consumer_client_id, auto_commit_interval_ms=4000, auto_offset_reset="earliest", allow_auto_create_topics=False)

        # Instanziazione Kafka admin con bootstrap server a cui deve avvenire la connessione, e client_id
        admin = KafkaAdminClient(bootstrap_servers=brokers_kafka, client_id=admin_client_id)
    except NoBrokersAvailable:
        print("Errore! Nessun broker disponibile per la connessione")
        sys.exit(-6)
    else:
        # 
        create_topics_if_not_exist(admin=admin, topics=topics_to_subscribe, partitions=partitions, replication=replication, min_insync_replicas=min_insyinc_replicas)

        # Subscription ai topic di interesse
        consumer.subscribe(topics=topics_to_subscribe)

        return consumer, admin

# 
def process_messages(consumer: KafkaConsumer):
    # Elaborazione messaggio
    for msg in consumer:
        json_formatted_payload = msg.value.decode()
        payload = json.loads(json_formatted_payload)

        print(f"Topic: {msg.topic}")
        print(f"Partition: {msg.partition}")
        print(f"Offset: {msg.offset}")
        print(f"Timestamp: {msg.timestamp}")
        print(f"Payload: {payload}")
        print(f"Headers:")
        print(f"\t{msg.headers[0][0]}: {msg.headers[0][1].decode()}\n")

# Check CMD Line Arguments - verifica dei parametri passati da linea di comando, in particolare relativi a host e porta
# dei broker Kafka; per tutti gli host viene controllato solamente se l'indirizzo è una stringa non vuota, mentre per
# tutte le porte si opera un controllo sulla validità del numero e se il numero di porta sia uno di quelli standard,
# cioè 9092, 9094 o 9096. Inoltre viene operata una ulteriore verifica sul formato dell'indirizzo IPv4 del broker, in
# particolare viene controllato che sia esattamente nella forma 'host:port'
def check_cmd_line_args(brokers_kafka: list[str]):
    if type(brokers_kafka) is not list:
        raise TypeError(f"Errore! Il tipo del parametro 'brokers_kafka' passato deve essere 'list'. Ricevuto {type(brokers_kafka)}")
    
    # Inizializzazione parametri di ritorno
    kafka_brokers = []

    # Brokers Kafka
    for broker in brokers_kafka:
        # Rimozione eventuali blank spaces
        broker = broker.replace(" ", "")

        try:
            # Controllo formato stringa broker strettamente del tipo 'host:porta'
            host, port = broker.split(":")
        except Exception:
            sys.stderr.write("Errore! Gli argomenti $host_kafka-* e $port_kafka-* passati da linea di comando devono essere nel formato '$host_kafka-*:port_kafka-*'\n")
            sys.exit(-2)

        # Host Kafka
        # Check stringa non vuota
        if host == "":
            sys.stderr.write("Errore! L'argomento $host_kafka-* deve essere un indirizzo non nullo\n")
            sys.exit(-3)

        # Port Kafka
        # Check numero valido
        try:
            port = int(port)
        except ValueError:
            sys.stderr.write("Errore! L'argomento $porta_kafka-* passato da linea di comando non è un numero valido\n")
            sys.exit(-4)

        # Check porta
        if port != 9092 and port != 9094 and port != 9096:
            sys.stderr.write("Errore! L'argomento $porta_kafka-* deve essere una porta Kafka valida: 9092 | 9094 | 9096\n")
            sys.exit(-5)
        
        kafka_brokers.append(broker)

    return kafka_brokers

# 
def main():
    if len(sys.argv) < 2 or len(sys.argv) > 4:
        sys.stderr.write(f"Errore! Uso corretto del programma: python[3] {sys.argv[0]} host_kafka-1:porta_kafka-1 [host_kafka-2:porta_kafka-2 host_kafka-3:porta_kafka-3]\n")
        sys.stderr.write("\t$host_kafka-* = 'host_Kafka_broker'\n")
        sys.stderr.write("\t$porta_kafka-* = '9092 | 9094 | 9096'\n")
        sys.exit(-1)

    # 
    brokers_kafka = check_cmd_line_args(brokers_kafka=sys.argv[1:].copy())

    # Installazione handler del segnale CTRL+C
    signal.signal(signalnum=signal.SIGINT, handler=signal_handler)

    # 
    global consumer
    global admin

    # 
    consumer, admin = consumer_admin_setup(brokers_kafka=brokers_kafka)

    print("\nConsumer in esecuzione...\n")

    # Elaborazione messaggi
    process_messages(consumer=consumer)


if __name__ == "main":
    main()
