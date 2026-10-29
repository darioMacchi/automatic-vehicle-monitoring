from kafka import KafkaConsumer, KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import (KafkaError, KafkaTimeoutError, NoBrokersAvailable,
                          TopicAlreadyExistsError)


# Creazione Producer Kafka - funzione utile alla creazione di un oggetto KafkaProducer, distribuito sulla lista di broker
# fornita in ingresso, e identificato dal parametro 'client_id'. La funzione prevede la restituzione di un oggetto
# KafkaProducer creato con altri due parametri specifici, ossia il numero massimo di richieste "pipelined" verso i broker, e
# la disabilitazione della creazione automatica dei topic all'interno del cluster
def _create_kafka_producer(brokers: list[str], client_id: str) -> KafkaProducer:
    try:
        # Instanziazione Kafka producer con bootstrap servers a cui deve avvenire la connessione, client_id, e
        # numero massimo di richieste "pipelined" verso i Kafka broker
        return KafkaProducer(bootstrap_servers=brokers, client_id=client_id, max_in_flight_requests_per_connection=1, allow_auto_create_topics=False)
    except NoBrokersAvailable as error:
        # Gestione errore di connessione a broker non disponibili alla connessione
        raise NoBrokersAvailable("Errore! Nessun broker Kafka disponibile per la connessione") from error
    except ValueError as error:
        raise ValueError("Errore! Configurazione cluster Kafka errata") from error


# Creazione Consumer Kafka - funzione utile alla creazione di un oggetto KafkaConsumer, distribuito sulla lista di broker
# fornita in ingresso, identificato dal parametro 'client_id', e inserito in un ConsumerGroup identificato dal parametro
# 'group_id'. La funzione prevede la restituzione di un oggetto KafkaConsumer creato con altri parametri specifici, ossia
# l'intervallo di auto commit, l'auto offset che denota la politica di gestione errori OffsetOutOfRange, e la disabilitazione
# della creazione automatica dei topic all'interno del cluster
def _create_kafka_consumer(brokers: list[str], client_id: str, group_id: str) -> KafkaConsumer:
    try:
        # Instanziazione Kafka consumer con disabilitazione creazione automatica topic, assegnazione ad un consumer group,
        # bootstrap servers a cui deve avvenire la connessione, client_id, intervallo di auto commit a 4s dato che i messaggi
        # vengono prodotti ogni 5s dal sistema AVM di telemetria, e auto offset reset a earliest in modo che per politica nel
        # momento in cui avviene un errore OffsetOutOfRange ci si sposta al messaggio più vecchio possibile
        return KafkaConsumer(bootstrap_servers=brokers, client_id=client_id, group_id=group_id, auto_commit_interval_ms=4000, auto_offset_reset="earliest", allow_auto_create_topics=False)
    except NoBrokersAvailable as error:
        # Gestione errore di connessione a broker non disponibili alla connessione
        raise NoBrokersAvailable("Errore! Nessun broker Kafka disponibile per la connessione") from error
    except ValueError as error:
        raise ValueError("Errore! Configurazione cluster Kafka errata") from error


# Creazione Admin Kafka - funzione utile alla creazione di un oggetto KafkaAdminClient, distribuito sulla lista di broker
# fornita in ingresso, e identificato dal parametro 'client_id'
def _create_kafka_admin(brokers: list[str], client_id: str) -> KafkaAdminClient:
    try:
        # Instanziazione Kafka admin con bootstrap servers a cui deve avvenire la connessione, e client_id
        return KafkaAdminClient(bootstrap_servers=brokers, client_id=client_id)
    except NoBrokersAvailable as error:
        # Gestione errore di connessione a broker non disponibili alla connessione
        raise NoBrokersAvailable("Errore! Nessun broker Kafka disponibile per la connessione") from error
    except ValueError as error:
        raise ValueError("Errore! Configurazione cluster Kafka errata") from error


# Ensure topics - funzione dedita alla creazione dei topic con i parametri di configurazione desiderati, per fare in modo che
# il topic sia gestito tra più broker Kafka, con un certo grado di partizione, replica e repliche in-sync (ISR). Verifica la
# presenza del topic nel cluster, in caso negativo la funzione si incarica della creazione
def _ensure_topics(admin: KafkaAdminClient, topics: list[str], replication: int = 1, partitions: int = 1, min_insync_replicas: int = 1) -> None:
    try:
        # Acquisizione topic presenti nel cluster
        existing_topics = admin.list_topics()
    except KafkaError as error:
        raise KafkaError("Errore! Impossibile ottenere il listato dei topic presenti nel cluster") from error

    # Per ogni topic presente all'interno della lista di topic da creare passata alla funzione, se non è già presente
    # nel cluster, avviene la creazione
    topics_to_create = [
        # Creazione topic attraverso l'oggetto NewTopic specificando:
        #   name: nome del topic
        #   num_partitions: numero di partizioni assegnate al topic
        #   replication_factor: grado di replicazione del topic, ossia su quanti broker deve essere replicato
        #   topic_configs: specifica del numero minimo di repliche che deve essere in-sync, ossia allineate
        #                  con il leader rispetto alle partizioni del topic
        NewTopic(
            name=topic,
            num_partitions=partitions,
            replication_factor=replication,
            topic_configs={
                "min.insync.replicas":str(min_insync_replicas)
            }
        )
        for topic in topics
        if topic not in existing_topics
    ]

    # Verifica del contenuto della lista 'topics_to_create' al fine di evitare le operazioni successive nel momento in cui
    # la lista non dovesse contenere nulla al suo interno
    if not topics_to_create:
        return

    try:
        # Creazione dei topic all'interno del cluster
        admin.create_topics(new_topics=topics_to_create)
    except TopicAlreadyExistsError as error:
        # La cattura dell'eccezione TopicAlreadyExistsError è necessaria per intercettare una possibile race condition, 
        # quest'ultima può avvenire nel momento in cui un processo A ottiene il listato dei topic, almeno uno dei topic
        # contenuti nella lista 'topics' è mancante nel cluster, per cui è necessaria la creazione di questo. Prima che 
        # avvenga la creazione viene deschedulato A e viene assegnata la CPU al processo B, che esegue la funzione con
        # gli stessi parametri, crea lo stesso topic che doveva essere creato da A e termina, a quel punto viene 
        # nuovamente schedulato A che cerca di creare il topic già presente nel cluster, a seguito però della creazione da
        # parte di B. L'operazione fallisce sollevando l'eccezione TopicAlreadyExistsError

        # Formazione lista di topic non ancora creati a seguito dell'avvenimento di una race condition
        current_topics = admin.list_topics()
        still_missing = [topic.name for topic in topics_to_create if topic.name not in current_topics]

        # Verifica del contenuto della lista 'still_missing' al fine di sollevare un'eccezione segnalando che qualcosa è
        # andato storto nel momento in cui la lista dovesse contenere elementi al suo interno
        if still_missing:
            raise RuntimeError("Errore! Topic ancora mancanti nel cluster in seguito a una race condition") from error

        for topic in topics:
            if topic in current_topics:
                print(f"Topic '{topic}' già presente nel cluster")
        print()
    except KafkaError as error:
        raise KafkaError("Errore! Creazione dei topic Kafka fallita") from error
    else:
        # Scorrimento lista topic da creare per verificare quali topic sono stati creati tra i prestabiliti
        for topic in topics_to_create:
            print(f"Topic '{topic.name}' creato")
        print()


# Chiusura Producer Kafka - funzione utile alla chiusura di un oggetto KafkaProducer, attraverso l'esecuzione di un flush
# dei messaggi eventualmente ancora da comunicare, per poi eseguire l'effettiva chiusura. La funzione prevede due parametri:
#   --> 'flush_timeout' utile alla definizione del timeout della funzione flush, da specificare in secondi
#   --> 'close_timeout' utile alla definizione del timeout della funzione close, da specificare in secondi
def _close_kafka_producer(producer: KafkaProducer, flush_timeout: float, close_timeout: float) -> None:
    # Flush e conseguente chiusura producer Kafka
    try:
        producer.flush(timeout=flush_timeout)
    except KafkaTimeoutError as error:
        raise KafkaTimeoutError("Errore! Flush fallito") from error
    finally:
        producer.close(timeout=close_timeout)


# Chiusura Producer Kafka alternativa - funzione utile alla chiusura di un oggetto KafkaProducer, senza l'esecuzione di un
# flush dei messaggi eventualmente ancora da comunicare, eseguendo solo l'effettiva chiusura. Questo approccio risulta utile
# per evitare una chiamata a flush nel momento in cui il producer sia stato appena creato, non avendo quindi nessuna necessità
# di comunicare messaggi. La funzione prevede un solo parametro:
#   --> 'close_timeout' utile alla definizione del timeout della funzione close, da specificare in secondi
def _close_kafka_producer_without_flush(producer: KafkaProducer, close_timeout: float) -> None:
    # Chiusura producer Kafka
    try:
        producer.close(timeout=close_timeout)
    except KafkaError as error:
        raise KafkaError("Errore! Chiusura del producer Kafka fallita") from error


# Chiusura Consumer Kafka - funzione utile alla chiusura di un oggetto KafkaConsumer, attraverso l'esecuzione dell'apposita
# funzione di chiusura. La funzione prevede un solo parametro:
#   --> 'close_timeout' utile alla definizione del timeout della funzione close, da specificare in millisecondi
def _close_kafka_consumer(consumer: KafkaConsumer, close_timeout: int) -> None:
    # Chiusura consumer Kafka
    try:
        consumer.close(timeout_ms=close_timeout)
    except KafkaError as error:
        raise KafkaError("Errore! Chiusura del consumer Kafka fallita") from error


# Chiusura Admin Kafka - funzione utile alla chiusura di un oggetto KafkaAdminClient, attraverso l'esecuzione dell'opportuna
# funzione di chiusura
def _close_kafka_admin(admin: KafkaAdminClient) -> None:
    # Chiusura admin Kafka
    try:
        admin.close()
    except KafkaError as error:
        raise KafkaError("Errore! Chiusura dell'admin Kafka fallita") from error
