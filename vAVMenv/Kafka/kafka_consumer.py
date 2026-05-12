import json
import signal

from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable


# Handler segnale CTRL+C
def signal_handler(sig_num: int, frame):
    sig_name = signal.Signals(sig_num).name

    close_timeout = 5000

    # Chiusura consumer
    try:
        consumer.close(timeout_ms=close_timeout)
    except Exception:
        print("Errore! Cessazione connessione al broker Kafka fallita")
    finally:
        print(f"\nEsecuzione consumer interrotta dal segnale {sig_name}")
        exit(0)

# Installazione handler del segnale CTRL+C
signal.signal(signalnum=signal.SIGINT, handler=signal_handler)

# Gestione errore di connessione ad un broker non disponibile alla connessione
try:
    # Instanziazione Kafka consumer con iscrizione topic, assegnazione ad un consumer group, bootstrap server a cui deve
    # avvenire la connessione, client_id, intervallo di auto commit a 4s dato che i messaggi vengono prodotti ogni 5s dal
    # sistema AVM di telemetria, e auto offset reset a earliest in modo che per politica nel momento in cui avviene un errore
    # OffsetOutOfRange ci si sposta al messaggio più vecchio possibile
    consumer = KafkaConsumer(group_id='AVM_telemetry_group', bootstrap_servers=['localhost:9092'], client_id="AVM_telemetry_consumer", auto_commit_interval_ms=4000, auto_offset_reset="earliest")
except NoBrokersAvailable:
    print("Errore! Nessun broker disponibile per la connessione")
    exit(-1)

# Subscription ai topic di interesse
consumer.subscribe(["AVM.telemetry.autobus.termic", "AVM.telemetry.autobus.hybrid", "AVM.telemetry.autobus.electric"])

print("Consumer started...\n")

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
