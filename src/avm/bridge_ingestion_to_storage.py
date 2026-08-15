import json
import os
import signal
import sys
import uuid

from kafka import KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import (KafkaError, NoBrokersAvailable,
                          TopicAlreadyExistsError)


# Oggetto Bridge Ingestion to Storage
bridge_ingestion_to_storage = None


# Handler segnale CTRL+C
def signal_handler(sig_num: int, frame):
    sig_name = signal.Signals(sig_num).name

    # Utilizzo dell'oggetto globale di bridging Ingestion to Storage per accedere all'istanza creata e agire su di essa per una
    # 'graceful disconnection'
    global bridge_ingestion_to_storage

    # Stop oggetto bridge Ingestion to Storage
    bridge_ingestion_to_storage.stop_bridge()

    # Terminazione
    print(f"Esecuzione consumer interrotta dal segnale {sig_name}")
    sys.exit(0)


# Oggetto Bridge Ingestion to Storage - permette di avviare un Kafka consumer che recepisce i messaggi provenienti dal
# cluster Kafka e opera da bridge verso il MongoDB, ossia il layer di Storage del sistema di monitoraggio telemetria 
# autobus
class BridgeIngestionStorage:
    def __init__(self, brokers_kafka: list[str]) -> None:
        # Topics declaration
        self._topics_to_subscribe = ["AVM.telemetry.autobus.termic",
                            "AVM.telemetry.autobus.hybrid",
                            "AVM.telemetry.autobus.electric"]

        # Setup Kafka
        self._brokers_kafka = brokers_kafka.copy()
        self._partitions = 2
        self._replication = 3
        self._min_insync_replicas = 2
        self._kafka_consumer, self._kafka_admin = self._consumer_admin_setup()

    # Metodo consumer_admin_setup(.) - dedito alla configurazione e istanziazione degli oggetti Consumer e AdminClient, e
    # subscription ai topic di interesse per l'oggetto Consumer. Viene integrata una logica per quanto riguarda gli ID degli
    # oggetti creati per assicurare che questi abbiano un identificativo univoco nel caso vengano lanciati più processi in cui
    # esegue lo script. Avviene inoltre la creazione dei topic solamente se assenti dal cluster, questo è il motivo
    # dell'utilizzo dell'oggetto AdminClient di Kafka
    def _consumer_admin_setup(self):
        topics_to_subscribe = self.get_topics_to_subscribe()
        brokers_kafka = self.get_brokers_kafka()

        # Setup parametri di configurazione topics
        partitions = self.get_partitions()
        replication = self.get_replication()
        min_insync_replicas = self.get_min_insync_replicas()

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
            sys.stderr.write("Errore! Nessun broker disponibile per la connessione\n")
            sys.exit(-6)
        else:
            # Creazione dei topic di interesse nel momento in cui non sono presenti nel cluster
            self._create_topics_if_not_exist(admin=admin, topics=topics_to_subscribe, partitions=partitions, replication=replication, min_insync_replicas=min_insync_replicas)

            # Subscription ai topic di interesse
            consumer.subscribe(topics=topics_to_subscribe)

            # Stampa a video delle subscription dell'oggetto Consumer
            print("\nSubscriptions:")
            subs = consumer.subscription()
            for sub in subs:
                print(f"\t{sub}")

            return consumer, admin

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

    # Getter 'kafka_client' parameter
    def get_kafka_client(self):
        return self._kafka_consumer
    
    # Getter 'kafka_admin' parameter
    def get_kafka_admin(self):
        return self._kafka_admin

    # Getter 'topics_to_subscribe' parameter
    def get_topics_to_subscribe(self):
        return self._topics_to_subscribe.copy()

    # Metodo create_topics_if_not_exist(., ., ., .) - dedito alla creazione dei topic con i parametri desiderati, ossia per
    # fare in modo che il topic sia gestito tra più broker Kafka, con un certo grado di partizione, replica e repliche in-sync
    # (ISR). Verifica se il topic è già presente nel cluster, altrimenti si incarica della creazione
    def _create_topics_if_not_exist(self, admin: KafkaAdminClient, topics: list[str], partitions=1, replication=1, min_insync_replicas=1):
        try:
            # Acquisizione topic presenti nel cluster
            topics_created = admin.list_topics()
        except KafkaError:
            sys.stderr.write("Errore! Impossibile ottenere il listato dei topic presenti nel cluster\n")
            sys.exit(-7)

        # Inizializzazione lista contenente i topic da creare
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

                # Aggiunta del topic da creare nella lista contenente questi ultimi
                new_topics.append(topic_to_create)

        try:
            # Creazione dei topic all'interno del cluster
            admin.create_topics(new_topics=new_topics)
        except TopicAlreadyExistsError:
            sys.stderr.write("Errore! Impossibile creare un topic che esiste già nel cluster\n")
            sys.exit(-8)
        except KafkaError:
            sys.stderr.write("Errore!\n")
            sys.exit(-9)
        else:
            # Scorrimento lista topic da creare per verificare quali topic sono stati creati tra i tre prestabiliti,
            # ossia:
            #   --> AVM.telemetry.autobus.termic
            #   --> AVM.telemetry.autobus.hybrid
            #   --> AVM.telemetry.autobus.electric
            for topic in topics:
                # Verifica che il topic non sia già stato creato, e quindi presente nel cluster, in questo caso è 
                # effettivamente stato creato
                if topic not in topics_created:
                    print(f"Topic '{topic}' creato")
                else:
                    # Topic già presente nel cluster, per cui non viene fatta nessuna azione riguardante quest'ultimo
                    print(f"Topic '{topic}' già presente nel cluster")

    # Metodo process_messages(.) - processamento di ogni messaggio che viene ricevuto dall'oggetto Consumer Kafka, il ciclo
    # contenuto all'interno del metodo consente di rimanere in esecuzione indefinitamente fino all'arrivo di un segnale di
    # interrupt, ossia una volta chiamato il metodo l'esecuzione rimarrà bloccata all'interno del ciclo in attesa di nuovi
    # messaggi per il Consumer Kafka, nel momento in cui arriva un messaggio vengono mostrate a video alcune informazioni
    # relative al messaggio:
    #   --> topic di appartenenza
    #   --> partizione di appartenenza
    #   --> offset a cui è presente il messaggio all'interno della partizione di appartenenza
    #   --> timestamp di memorizzazione del messaggio nel log da parte del broker Kafka
    #   --> payload del messaggio ricevuto
    #   --> headers del messaggio ricevuto
    def process_messages(self):
        consumer = self.get_kafka_client()
        
        # Elaborazione messaggio
        for msg in consumer:
            json_formatted_payload = msg.value.decode()
            payload = json.loads(json_formatted_payload)

            print(f"Topic: {msg.topic}")
            print(f"Partition: {msg.partition}")
            print(f"Offset: {msg.offset}")
            print(f"Timestamp: {msg.timestamp / 1000.00}")
            print(f"Payload: {payload}")
            print(f"Headers:")
            print(f"\t{msg.headers[0][0]}: {msg.headers[0][1].decode()}\n")

    # Stop method - prevede lo stop del bridge a seguito della ricezione di un segnale SIGINT (CTRL+C), per una 
    # graceful disconnection viene eseguita la chiusura del consumer e dell'admin Kafka con il metodo close(.), inoltre 
    # viene stampato a video un messaggio di informazione
    def stop_bridge(self):
        close_timeout = 5000
        
        try:
            # Chiusura consumer
            self.get_kafka_client().close(timeout_ms=close_timeout)
            # Chiusura admin
            self.get_kafka_admin().close()
        except Exception:
            sys.stderr.write("\nErrore! Cessazione connessione al broker Kafka fallita\n\n")
        finally:
            print(f"\nConnessione al broker Kafka interrotta correttamente\n")


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


# Metodo main() - consente di controllare gli argomenti passati da linea di comando (CLI), ed eseguire il setup dell'oggetto
# Bridge Ingestion to Storage. Successivamente avviene l'elaborazione dei messaggi ricevuti dall'oggetto di bridging
def main():
    # Verifica corretta invocazione del programma
    if len(sys.argv) < 2 or len(sys.argv) > 4:
        sys.stderr.write(f"Errore! Uso corretto del programma: python[3] {sys.argv[0]} host_kafka-1:porta_kafka-1 [host_kafka-2:porta_kafka-2 host_kafka-3:porta_kafka-3]\n")
        sys.stderr.write("\t$host_kafka-* = 'host_Kafka_broker'\n")
        sys.stderr.write("\t$porta_kafka-* = '9092 | 9094 | 9096'\n")
        sys.exit(-1)

    # Verifica validità indirizzi broker Kafka
    brokers_kafka = check_cmd_line_args(brokers_kafka=sys.argv[1:].copy())

    # Installazione handler del segnale CTRL+C
    signal.signal(signalnum=signal.SIGINT, handler=signal_handler)

    # Utilizzo dell'oggetto globale di bridging Ingestion to Storage
    global bridge_ingestion_to_storage

    # Instanziazione dell'oggetto Bridge Ingestion to Storage
    bridge_ingestion_to_storage = BridgeIngestionStorage(brokers_kafka=brokers_kafka)

    print("\nConsumer in esecuzione...\n")

    # Messa in esecuzione del bridge (elaborazione messaggi)
    bridge_ingestion_to_storage.process_messages()


if __name__ == "__main__":
    main()
