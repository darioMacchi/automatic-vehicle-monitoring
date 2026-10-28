from kafka import KafkaConsumer, KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import (KafkaError, KafkaTimeoutError, NoBrokersAvailable,
                          TopicAlreadyExistsError)


# 
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


# 
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


# 
def _create_kafka_admin(brokers: list[str], client_id: str) -> KafkaAdminClient:
    try:
        # Instanziazione Kafka admin con bootstrap servers a cui deve avvenire la connessione, e client_id
        return KafkaAdminClient(bootstrap_servers=brokers, client_id=client_id)
    except NoBrokersAvailable as error:
        # Gestione errore di connessione a broker non disponibili alla connessione
        raise NoBrokersAvailable("Errore! Nessun broker Kafka disponibile per la connessione") from error
    except ValueError as error:
        raise ValueError("Errore! Configurazione cluster Kafka errata") from error


# Funzione dedita alla creazione dei topic con i parametri di configurazione desiderati, ossia per fare in modo che
# il topic sia gestito tra più broker Kafka, con un certo grado di partizione, replica e repliche in-sync (ISR).
# Verifica se il topic è già presente nel cluster, altrimenti si incarica della creazione
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

    # 
    if not topics_to_create:
        return

    try:
        # Creazione dei topic all'interno del cluster
        admin.create_topics(new_topics=topics_to_create)
    except TopicAlreadyExistsError as error:
        # TODO
        # Gestione possibile race-condition
        raise TopicAlreadyExistsError("Errore! Impossibile creare un topic che esiste già nel cluster") from error
    except KafkaError as error:
        raise KafkaError("Errore! Creazione dei topic Kafka fallita") from error
    else:
        # Scorrimento lista topic fornita per verificare quali topic sono stati creati tra i prestabiliti
        for topic in topics:
            # Verifica che il topic non sia già stato creato, e quindi presente nel cluster, in questo caso è 
            # effettivamente stato creato
            if topic not in existing_topics:
                print(f"Topic '{topic}' creato")
        print()


# 
def _close_kafka_producer(producer: KafkaProducer, flush_timeout: float, close_timeout: float) -> None:
    # Flush e conseguente chiusura producer Kafka
    try:
        producer.flush(timeout=flush_timeout)
    except KafkaTimeoutError as error:
        raise KafkaTimeoutError("Errore! Flush fallito") from error
    finally:
        producer.close(timeout=close_timeout)


# 
def _close_kafka_producer_without_flush(producer: KafkaProducer, close_timeout: float) -> None:
    # Chiusura producer Kafka
    try:
        producer.close(timeout=close_timeout)
    except KafkaError as error:
        raise KafkaError("Errore! Chiusura del producer Kafka fallita") from error


# 
def _close_kafka_consumer(consumer: KafkaConsumer, close_timeout: int) -> None:
    # Chiusura consumer Kafka
    try:
        consumer.close(timeout_ms=close_timeout)
    except KafkaError as error:
        raise KafkaError("Errore! Chiusura del consumer Kafka fallita") from error


# 
def _close_kafka_admin(admin: KafkaAdminClient) -> None:
    # Chiusura admin Kafka
    try:
        admin.close()
    except KafkaError as error:
        raise KafkaError("Errore! Chiusura dell'admin Kafka fallita") from error


def main():
    admin = _create_kafka_admin(brokers=["localhost:9092"], client_id="Test")
    _close_kafka_admin(admin=admin)
    print("Closed")


if __name__ == "__main__":
    main()
