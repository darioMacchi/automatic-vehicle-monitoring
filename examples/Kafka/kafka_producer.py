import json

from kafka import KafkaProducer

# Instanziazione Kafka producer con bootstrap server a cui deve avvenire la connessione, client_id, e numero massimo di
# richieste "pipelined" verso il Kafka broker
producer = KafkaProducer(bootstrap_servers='localhost:9092', client_id="kafka_producer_test", max_in_flight_requests_per_connection=1)

# Verifica connessione del bootstrap server
if producer.bootstrap_connected():
    # Invio messaggi
    for i in range(3):
        # Preparazione messaggio
        diz = {
            "ID": i,
            "temperature": 20 + (2*i + 1),
            "humidity": 50 + (2*i)
        }
        data_to_send = json.dumps(diz)
        # Invio del messaggio con inclusione degli header per indicare l'encoding del contenuto
        future = producer.send(topic='measurements.test', value=data_to_send.encode(), headers=[("content-encoding", b"JSON")])
        # Attesa dell'effettivo invio del messaggio
        result = future.get(timeout=60)
        print(f"Inviato messaggio con offset {result.offset}\n")

# Chiusura consumer
producer.close()
