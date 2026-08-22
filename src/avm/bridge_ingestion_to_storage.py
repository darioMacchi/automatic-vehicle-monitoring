import json
import os
import signal
import sys
import uuid
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path

import certifi
from dotenv import load_dotenv
from kafka import KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import (KafkaError, NoBrokersAvailable,
                          TopicAlreadyExistsError)
from pymongo import MongoClient, server_api
from pymongo.errors import CollectionInvalid, OperationFailure


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
# cluster Kafka e opera da bridge verso MongoDB, ossia il layer di Storage del sistema di monitoraggio telemetria 
# autobus
class BridgeIngestionStorage:
    def __init__(self, brokers_kafka: list[str], mongodb_uri: str) -> None:
        # Setup Kafka
        self._brokers_kafka = brokers_kafka.copy()
        self._telemetry_partitions = 2
        self._processing_partitions = 1
        self._replication = 3
        self._min_insync_replicas = 2
        self._kafka_consumer, self._kafka_admin = self._consumer_admin_setup()

        # Topics initialization
        self._telemetry_topics_to_subscribe = ["AVM.telemetry.autobus.termic",
                            "AVM.telemetry.autobus.hybrid",
                            "AVM.telemetry.autobus.electric"]
        self._processing_topics_to_subscribe = ["AVM.processing.autobus.storage"]
        self._setup_topics()

        # Setup MongoDB
        self._mongodb_client = self._setup_mongodb(mongodb_uri=mongodb_uri)
        self._mongodb_db_name = "AVM_database"

    # Metodo consumer_admin_setup() - dedito alla configurazione e istanziazione degli oggetti Consumer e AdminClient.
    # Viene integrata una logica per quanto riguarda gli ID degli oggetti creati per assicurare che questi abbiano un
    # identificativo univoco nel caso vengano lanciati più processi in cui esegue lo script
    def _consumer_admin_setup(self):
        brokers_kafka = self.get_brokers_kafka()

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
            return consumer, admin

    # Metodo setup_topics() - dedito alla subscription ai topic di interesse per l'oggetto Consumer, inoltre avviene la
    # creazione dei topic solamente se assenti dal cluster, questo motiva l'utilizzo dell'oggetto AdminClient di Kafka
    def _setup_topics(self):
        consumer = self.get_kafka_client()
        telemetry_topics_to_subscribe = self.get_telemetry_topics_to_subscribe()
        processing_topics_to_subscribe = self.get_processing_topics_to_subscribe()

        # Setup parametri di configurazione topics
        telemetry_partitions = self.get_telemetry_partitions()
        processing_partitions = self.get_processing_partitions()
        replication = self.get_replication()
        min_insync_replicas = self.get_min_insync_replicas()

        # Creazione dei topic di interesse nel momento in cui non siano presenti nel cluster
        self._create_topics_if_not_exist(topics=telemetry_topics_to_subscribe, partitions=telemetry_partitions, replication=replication, min_insync_replicas=min_insync_replicas)
        self._create_topics_if_not_exist(topics=processing_topics_to_subscribe, partitions=processing_partitions, replication=replication, min_insync_replicas=min_insync_replicas)

        # Formazione lista completa di topic di interesse
        topics_to_subscribe = telemetry_topics_to_subscribe
        topics_to_subscribe.extend(processing_topics_to_subscribe)
        # Subscription ai topic di interesse
        consumer.subscribe(topics=topics_to_subscribe)

        # Stampa a video delle subscription dell'oggetto Consumer
        print("\nSubscriptions:")
        subs = consumer.subscription()
        for sub in subs:
            print(f"\t{sub}")

    # Metodo setup_mongodb(.) - necessario al fine di creare un client MongoDB utile per la comunicazione verso il cluster
    # MongoDB remoto, necessari quindi la connection string ('uri') verso il cluster, prelevata dal file di env presente
    # nel progetto, e la specifica del luogo del certificato TLS/SSL necessario per la connessione sicura
    def _setup_mongodb(self, mongodb_uri: str):
        try:
            # Setup connection string a partire dal 'mongodb_uri' prelevato dal file env, a cui viene concatenata la
            # option dell'applicazione di default a cui accedere, in questo caso l'unico cluster presente in remoto
            uri = f"{mongodb_uri}/?appName=Cluster0"

            # Inizializzazione client MongoDB con specifica di:
            #   --> uri: connection string
            #   --> tls
            #   --> tlsCAFile: locazione del certificato TLS/SSL
            #   --> server_api
            client = MongoClient(
                uri,
                tls=True,
                tlsCAFile=certifi.where(),
                server_api=server_api.ServerApi(
                    version="1", strict=True, deprecation_errors=True
                )
            )
        except Exception as e:
            sys.stderr.write(f"Errore! Verificato malfunzionamento: {e}\n")
            sys.exit(-10)
        else:
            return client

    # Getter 'brokers_kafka' parameter
    def get_brokers_kafka(self):
        return self._brokers_kafka.copy()

    # Getter 'telemetry_partitions' parameter
    def get_telemetry_partitions(self):
        return self._telemetry_partitions

    # Getter 'processing_partitions' parameter
    def get_processing_partitions(self):
        return self._processing_partitions
    
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

    # Getter 'telemetry_topics_to_subscribe' parameter
    def get_telemetry_topics_to_subscribe(self):
        return self._telemetry_topics_to_subscribe.copy()

    # Getter 'processing_topics_to_subscribe' parameter
    def get_processing_topics_to_subscribe(self):
        return self._processing_topics_to_subscribe.copy()

    # Getter 'mongodb_client' parameter
    def get_mongodb_client(self):
        return self._mongodb_client

    # Getter 'mongodb_database' parameter
    def get_mongodb_db_name(self):
        return self._mongodb_db_name

    # Metodo create_topics_if_not_exist(., ., ., .) - dedito alla creazione dei topic con i parametri desiderati, ossia per
    # fare in modo che il topic sia gestito tra più broker Kafka, con un certo grado di partizione, replica e repliche in-sync
    # (ISR). Verifica se il topic è già presente nel cluster, altrimenti si incarica della creazione
    def _create_topics_if_not_exist(self, topics: list[str], partitions=1, replication=1, min_insync_replicas=1):
        admin = self.get_kafka_admin()

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
            # Scorrimento lista topic da creare per verificare quali topic sono stati creati tra i prestabiliti,
            # ossia:
            #   --> AVM.telemetry.autobus.termic
            #   --> AVM.telemetry.autobus.hybrid
            #   --> AVM.telemetry.autobus.electric
            #   --> AVM.processing.autobus.storage
            for topic in topics:
                # Verifica che il topic non sia già stato creato, e quindi presente nel cluster, in questo caso è 
                # effettivamente stato creato
                if topic not in topics_created:
                    print(f"Topic '{topic}' creato")
                else:
                    # Topic già presente nel cluster, per cui non viene fatta nessuna azione riguardante quest'ultimo
                    print(f"Topic '{topic}' già presente nel cluster")

    # Metodo store(., .) - storage di ogni 'document' all'interno della collection MongoDB con nome 'collection_name'.
    # Viene reperito / creato il database, successivamente viene reperita la lista di collection contenute al suo interno
    # e dopo di ché viene verificata la presenza della collection desiderata al suo interno. Successivamente viene
    # adattato il documento passato per rispettare le regole del formato di documenti di MongoDB (BSON) e inserito nella
    # collection.
    # Il metodo prevede la restituzione del buon fine o meno dell'operazione di inserimento
    def _store(self, collection_name: str, document: dict):
        if type(collection_name) is not str:
            raise TypeError(f"Errore! Il tipo del parametro 'collection_name' passato deve essere 'str'. Ricevuto {type(collection_name)}")

        if type(document) is not dict:
            raise TypeError(f"Errore! Il tipo del parametro 'document' passato deve essere 'dict'. Ricevuto {type(document)}")

        # Reperimento db
        database = self.get_mongodb_client().get_database(name=self.get_mongodb_db_name())

        # Reperimento lista di collection presenti nel db
        collection_list = database.list_collection_names()

        # Reperimento key 'type' dai dati, se presente il dato è appartenente al topic di processing, altrimenti appartiene
        # al topic di telemetria
        data_type = document.get("type")

        # Verifica 'data_type' valorizzato e presenza della sottostringa 'processing' all'interno del topic, in quel caso viene
        # aggiunto il tipo di motorizzazione al nome della collection MongoDB
        if data_type and "processing" in collection_name:
            collection_name = collection_name + f"_{data_type}"

        # Verifica della presenza della collection d'interesse all'interno del db, nel caso in cui sia presente viene
        # solamente reperita, mentre se non presente viene creata come timeseries collection, con annessa specifica di campo
        # del timestamp della timeseries, campo di metadati della timeseries e granularità di quest'ultima.
        # Per la granularità si è inserita quella di più basso livello possibile
        ts_collection = None
        if collection_name not in collection_list:
            # Creazione della collection inserita all'interno di un blocco try-except perché nel momento in cui vengono
            # eseguiti più processi potrebbe scatenare una race condition, ad esempio un processo sta per creare la
            # collection ma viene deschedulato subito prima, dopo di cheé viene schedulato un altro processo che crea
            # esattamente la stessa collection, quando viene rischedulato il primo processo la creazione della collection
            # fallisce perché esiste già    -->     inserimento nel blocco try-except catturando le eccezioni corrispondenti
            # a 'collection already exists'
            try:
                ts_collection = database.create_collection(name=collection_name, timeseries={"timeField": "timestamp", "metaField": "metadata", "granularity": "seconds"})
            except CollectionInvalid:
                print("Collection already exists --> get_collection")
                ts_collection = database.get_collection(name=collection_name)
            except OperationFailure as e:
                if getattr(e, "code", None) == 48:
                    print("Collection already exists --> get_collection")
                    ts_collection = database.get_collection(name=collection_name)
                else:
                    raise
        else:
            ts_collection = database.get_collection(name=collection_name)

        document_to_insert = {}

        # Verifica topic di telemetria o topic di processing basato sulla presenza della key 'type' o meno
        if data_type:
            # Topic di PROCESSING

            # Preparazione documento da inserire, formato a partire dal documento ricevuto, con l'adattamento al formato
            # MongoDB, in cui viene inserito:
            #   --> timestamp: fine della finestra temporale
            #   --> metadata: targa dell'autobus smart
            #   --> window: inizio e fine della finestra temporale dei dati considerati
            #   --> tyre_pressure
            #   --> engine_status
            #   --> brake_status
            document_to_insert = {
                "timestamp": datetime.fromtimestamp( document["window_end"] / 1000 , tz=timezone.utc),
                "metadata": {
                    "license_plate": document["license_plate"]
                },
                "window": {
                    "start": datetime.fromtimestamp( document["window_start"] / 1000 , tz=timezone.utc),
                    "end": datetime.fromtimestamp( document["window_end"] / 1000 , tz=timezone.utc)
                },
                "tyre_pressure": {
                    "avg": document["avg_tyre_press"],
                    "alarm": document["alarm_tyre_press"]
                },
                "engine_status": {
                    "count": document["count_engine_stat"],
                    "alarm": document["alarm_engine_stat"]
                },
                "brake_status": {
                    "count": document["count_brake_stat"],
                    "alarm": document["alarm_brake_stat"]
                }
            }
    
            # Verifica del tipo di motorizzazione elettrica o ibrida, e solo in questi due casi aggiunta dei dati 
            # riguardanti la temperatura del pacco batterie 
            if data_type == "electric" or data_type == "hybrid":
                document_to_insert.update(
                    {
                        "battery_temperature": {
                            "avg": document["avg_battery_temp"],
                            "alarm": document["alarm_battery_temp"]
                        }
                    }
                )
        else:
            # Topic di TELEMETRIA
            
            # Inizializzazione documento da inserire nella collection MongoDB
            document_to_insert = document

            # Aggiunta metadati e rimozione della key 'license_plate' che rappresenta proprio i metadata in MongoDB
            document_to_insert["metadata"] = {
                "license_plate": document["license_plate"]
            }
            document_to_insert.pop("license_plate")

            # Adattamento del campo timestamp per essere conforme al formato desiderato da MongoDB
            document_to_insert["timestamp"] = datetime.fromtimestamp( document["timestamp"] , tz=timezone.utc)

        # Inserimento 'document_to_insert' all'interno della collection
        ts_result = ts_collection.insert_one(document=document_to_insert)

        return ts_result.acknowledged

    # Metodo process_messages(.) - processamento di ogni messaggio che viene ricevuto dall'oggetto Consumer Kafka, il ciclo
    # contenuto all'interno del metodo consente di rimanere in esecuzione indefinitamente fino all'arrivo di un segnale di
    # interrupt, ossia una volta chiamato il metodo l'esecuzione rimarrà bloccata all'interno del ciclo in attesa di nuovi
    # messaggi per il Consumer Kafka; nel momento in cui arriva un messaggio viene innescata l'operazione di storage e
    # successivamente vengono mostrate a video alcune informazioni relative al messaggio:
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

            # Preparazione nome collection MongoDB
            collection_name = msg.topic.replace(".", "_")

            # Storage documento elaborato
            if self._store(collection_name=collection_name, document=deepcopy(payload)):
                print(f"Inserimento del documento all'interno della collection {collection_name} RIUSCITO")
            else:
                sys.stderr.write(f"Inserimento del documento all'interno della collection {collection_name} FALLITO\n")

            print(f"Topic: {msg.topic}")
            print(f"Partition: {msg.partition}")
            print(f"Offset: {msg.offset}")
            print(f"Timestamp: {msg.timestamp / 1000.00}")
            print(f"Payload: {payload}")
            # Verifica topic da cui sono stati ricevuti i dati, nel caso in cui sia il topic di telemetria l'header
            # è presente, in caso contrario è assente e quindi non viene visualizzato a video
            if "processing" not in msg.topic:
                print(f"Headers:")
                print(f"\t{msg.headers[0][0]}: {msg.headers[0][1].decode()}\n")
            else:
                print()

    # Stop method - prevede lo stop del bridge a seguito della ricezione di un segnale SIGINT (CTRL+C), per una 
    # graceful disconnection viene eseguita la chiusura del consumer e dell'admin Kafka con il metodo close(.), e del
    # client MongoDB con il metodo close(), inoltre viene stampato a video un messaggio di informazione
    def stop_bridge(self):
        close_timeout = 5000
        
        try:
            # Chiusura consumer
            self.get_kafka_client().close(timeout_ms=close_timeout)
            # Chiusura admin
            self.get_kafka_admin().close()

            # Chiusura MongoDB client
            self.get_mongodb_client().close()
        except Exception:
            sys.stderr.write("\nErrore! Cessazione connessione al broker Kafka / cluster MongoDB fallita\n\n")
        finally:
            print(f"\nConnessione a broker Kafka e cluster MongoDB interrotte correttamente\n")


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


# Check Environment Variables - metodo necessario alla verifica della presenza delle variabili d'ambiente necessarie al 
# corretto funzionamento dello script. Prevede il passaggio di una lista di env vars necessarie e restituisce un dizionario
# contenente coppie key-value con chiave l'env var passata e con value il valore corrispondente a questa (nel momento in cui
# fosse presente)
def check_env_vars(vars: list):
    if type(vars) is not list:
        raise TypeError(f"Errore! Il tipo del parametro passato deve essere 'list'. Ricevuto {type(vars)}")

    env_vars = {}

    # Per ognuna delle variabili d'ambiente passate viene prelevato il valore corrispondente se presente, altrimenti viene
    # alzata un'eccezione 'ValueError'
    for var in vars:
        value = os.getenv(var)
        if not value:
            raise ValueError(f"Errore! {var} non trovata nel file env")

        env_vars.update({var: value})

    return env_vars


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

    # Caricamento file di environment
    project_root = Path(__file__).resolve().parents[2]
    env_path = project_root / ".env"
    load_dotenv(dotenv_path=env_path)

    # Verifica robusta della presenza della variabile di environment necessaria
    env_vars = check_env_vars(["MONGODB_URI"])

    # Installazione handler del segnale CTRL+C
    signal.signal(signalnum=signal.SIGINT, handler=signal_handler)

    # Utilizzo dell'oggetto globale di bridging Ingestion to Storage
    global bridge_ingestion_to_storage

    # Instanziazione dell'oggetto Bridge Ingestion to Storage
    bridge_ingestion_to_storage = BridgeIngestionStorage(brokers_kafka=brokers_kafka, mongodb_uri=env_vars["MONGODB_URI"])

    print("\nConsumer in esecuzione...\n")

    # Messa in esecuzione del bridge (elaborazione messaggi)
    bridge_ingestion_to_storage.process_messages()


if __name__ == "__main__":
    main()
