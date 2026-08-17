import datetime as dt
import json
import os
import signal
import sys
import uuid

from kafka import KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import (KafkaError, NoBrokersAvailable,
                          TopicAlreadyExistsError)
from rich.errors import NotRenderableError
from rich.live import Live
from rich.table import Table


# Oggetto Live Dashboard
live_dashboard = None


# Handler segnale CTRL+C
def signal_handler(sig_num: int, frame):
    # Utilizzo dell'oggetto globale di live dashboarding per accedere all'istanza creata e agire su di essa per una
    # 'graceful disconnection'
    global live_dashboard

    # Stop oggetto live dashboard
    live_dashboard.stop_dashboard()

    # Terminazione
    sys.exit(0)


# Oggetto Live Dashboard - permette di avviare un Kafka consumer che recepisce i messaggi provenienti dal
# cluster Kafka e opera da sistema di dashboarding live, in modo da poter visualizzare i dati processati attraverso il
# sistema di processing, tale Flink, ed eventuali allarmi sempre geenrati attraverso l'analisi del sistema di processing
class LiveDashboard:
    def __init__(self, brokers_kafka: list[str]) -> None:
        # Setup Kafka
        self._brokers_kafka = brokers_kafka.copy()
        self._partitions = 1
        self._replication = 3
        self._min_insync_replicas = 2
        self._kafka_consumer, self._kafka_admin = self._consumer_admin_setup()

        # Topics declaration
        self._topics_to_subscribe = ["AVM.processing.autobus.dashboard"]
        self._setup_topics()

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
        consumer_client_id = f"AVM_dashboarding_consumer-{pid}-{short_uuid}"
        admin_client_id = f"AVM_dashboarding_consumer_admin-{pid}-{short_uuid}"

        # Gestione errore di connessione a broker non disponibili alla connessione
        try:
            # Instanziazione Kafka consumer con iscrizione topic, assegnazione ad un consumer group, bootstrap servers a cui
            # deve avvenire la connessione, client_id, intervallo di auto commit a 4s dato che i messaggi vengono prodotti
            # ogni 5s dal sistema AVM di telemetria (di conseguenza la finestra di processing scorre ogni 5s), e auto offset
            # reset a earliest in modo che per politica nel momento in cui avviene un errore OffsetOutOfRange ci si sposta
            # al messaggio più vecchio possibile
            consumer = KafkaConsumer(group_id='AVM_dashboarding_consumer_group', bootstrap_servers=brokers_kafka, client_id=consumer_client_id, auto_commit_interval_ms=4000, auto_offset_reset="earliest", allow_auto_create_topics=False)

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
        topics_to_subscribe = self.get_topics_to_subscribe()

        # Setup parametri di configurazione topics
        partitions = self.get_partitions()
        replication = self.get_replication()
        min_insync_replicas = self.get_min_insync_replicas()

        # Creazione dei topic di interesse nel momento in cui non siano presenti nel cluster
        self._create_topics_if_not_exist(topics=topics_to_subscribe, partitions=partitions, replication=replication, min_insync_replicas=min_insync_replicas)

        # Subscription ai topic di interesse
        consumer.subscribe(topics=topics_to_subscribe)

        # Stampa a video delle subscription dell'oggetto Consumer
        print("\nSubscriptions:")
        subs = consumer.subscription()
        for sub in subs:
            print(f"\t{sub}")

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
            # Scorrimento lista topic da creare per verificare quali topic sono stati creati tra i tre prestabiliti,
            # ossia:
            #   --> AVM.processing.autobus.dashboard
            for topic in topics:
                # Verifica che il topic non sia già stato creato, e quindi presente nel cluster, in questo caso è 
                # effettivamente stato creato
                if topic not in topics_created:
                    print(f"Topic '{topic}' creato")
                else:
                    # Topic già presente nel cluster, per cui non viene fatta nessuna azione riguardante quest'ultimo
                    print(f"Topic '{topic}' già presente nel cluster")

    # Metodo 'to_string_alarm' - consente di predisporre una stringa testuale al posto del solo booleano con cui viene
    # comunicato l'allarme, in modo da rendere più human friendly l'interazione utente
    def _to_string_alarm(self, alarm: bool):
        alarm_str = ""

        if alarm is True:
            alarm_str = "!!! ALLARME !!!"
        else:
            alarm_str = "Falso"

        return alarm_str

    # Metodo 'generate_table' - necessario per generare la tabella in cui visualizzare le metriche ricevute dal sistema di
    # processing del sistema di telemetria degli autobus smart, vengono visualizzate diverse informazioni che permettono di
    # creare la dashboard in cui vengono presentati i dati processati ed eventuali allarmi
    def _generate_table(self, data: list):
        if type(data) is not list:
            raise TypeError(f"Errore! Il tipo del parametro passato deve essere 'list', ricevuto {type(data)}")

        # Setup intestazione tabella
        table = Table(title="Automatic Vehicle Monitoring Dashboard")
        table.add_column("")
        table.add_column("Window Start")
        table.add_column("Window End")
        table.add_column("Media Pressione Gomme")
        table.add_column("# Stato Critico Motore")
        table.add_column("# Stato Critico Freni")
        table.add_column("Media Temperatura Batteria")
        table.add_column("Allarme Pressione Gomme")
        table.add_column("Allarme Stato Critico Motore")
        table.add_column("Allarme Stato Critico Freni")
        table.add_column("Allarme Temperatura Batteria")

        # Setup parametri necessari alla formazione delle row della tabella
        index_avg_battery_temp = 6
        alarm_tp_str = ""
        alarm_es_str = ""
        alarm_bs_str = ""
        alarm_bt_str = ""
        fallback_row = ["", "", "", "", "", "", "", "", "", "", ""]
        row = None

        # Verifica presenza dati all'interno della lista contenente le misure e gli eventuali allarmi da mostrare
        if len(data) > 0:
            for el in data:
                # Verifica della presenza di tutti i dati necessari
                if el.get("license_plate") is not None and el.get("window_start") is not None and el.get("window_end") is not None and el.get("avg_tyre_press") is not None and el.get("count_engine_stat") is not None and el.get("count_brake_stat") is not None and el.get("alarm_tyre_press") is not None and el.get("alarm_engine_stat") is not None and el.get("alarm_brake_stat") is not None:
                    # Setup stringhe corrispondenti agli allarmi
                    alarm_tp_str = self._to_string_alarm(el["alarm_tyre_press"])
                    alarm_es_str = self._to_string_alarm(el["alarm_engine_stat"])
                    alarm_bs_str = self._to_string_alarm(el["alarm_brake_stat"])

                    # Setup parametri da mostrare all'interno della dashboard
                    row = [f"{el['license_plate']}", str( dt.datetime.fromtimestamp( el["window_start"] / 1000) ), str( dt.datetime.fromtimestamp( el["window_end"] / 1000) ), f"{el['avg_tyre_press']} bar", f"{el['count_engine_stat']}", f"{el['count_brake_stat']}", alarm_tp_str, alarm_es_str, alarm_bs_str]

                    # Verifica presenza 'avg_battery_temp' e di conseguenza certezza di motorizzazione ibrida o elettrica
                    if el.get("avg_battery_temp") is not None:
                        # Solo in questo caso viene aggiunta la misura corrispondente
                        row.insert(index_avg_battery_temp, f"{el['avg_battery_temp']} °C")
                    else:
                        # Altrimenti una stringa vuota
                        row.insert(index_avg_battery_temp, "")

                    # Verifica presenza 'alarm_battery_temp' e di conseguenza certezza di motorizzazione ibrida o elettrica
                    if el.get("alarm_battery_temp") is not None:
                        # Solo in questo caso viene aggiunto l'allarme corrispondente
                        alarm_bt_str = self._to_string_alarm(el["alarm_battery_temp"])

                        row.append(alarm_bt_str)
                    else:
                        # Altrimenti una stringa vuota
                        row.append("")
                else:
                    # In caso di assenza di tutti i dati necessari viene mostrata a video la dashboard priva di dati
                    row = fallback_row

                # Operazione di aggiunta della row alla dashboard
                try:
                    table.add_row(*row)
                except NotRenderableError:
                    sys.stderr.write("Errore! Non è stato possibile renderizzare la row\n")
                    sys.exit(-10)
        else:
            # In caso di assenza di dati viene mostrata a video la dashboard priva di dati
            row = fallback_row

            # Operazione di aggiunta della row alla dashboard
            try:
                table.add_row(*row)
            except NotRenderableError:
                sys.stderr.write("Errore! Non è stato possibile renderizzare la row\n")
                sys.exit(-11)
        
        return table

    # Metodo process_messages() - processamento di ogni messaggio che viene ricevuto dall'oggetto Consumer Kafka, il ciclo
    # contenuto all'interno del metodo consente di rimanere in esecuzione indefinitamente fino all'arrivo di un segnale di
    # interrupt, ossia una volta chiamato il metodo l'esecuzione rimarrà bloccata all'interno del ciclo in attesa di nuovi
    # messaggi per il Consumer Kafka, nel momento in cui arriva un messaggio può essere innescato il meccanismo di 
    # presentazione della dashboard oppure il messaggio viene inserito nella lista dei messaggi che appartengono alla stessa
    # finestra temporale, e solamente nel momento in cui non arrivano più dati di quella finestra viene innescato il 
    # meccanismo di presentazione della dashboard.
    # Così facendo la dashboard viene aggiornata quando arrivano dati nuovi, e in quel momento vedo i dati vecchi di 5
    # secondi nella dashboard, quindi la dashboard non è propriamente live, o meglio lo è però in ritardo di 5 secondi
    def process_messages(self):
        consumer = self.get_kafka_client()

        # Lista contenente messaggi appartenenti alla stessa finestra temporale
        same_window_messages = []

        # Predisposizione contesto dashboarding con un refresh al secondo
        with Live(self._generate_table([]), refresh_per_second=1) as live:
            # Elaborazione messaggio
            for msg in consumer:
                # Estrazione payload
                json_formatted_payload = msg.value.decode()
                payload = json.loads(json_formatted_payload)

                # Verifica presenza di messaggi nella lista e verifica della corrispondenza della finestra temporale del
                # dato appena letto con quella dei dati presenti nella lista
                if len(same_window_messages) > 0 and payload["window_start"] == same_window_messages[0]["window_start"] and payload["window_end"] == same_window_messages[0]["window_end"]:
                    # In questo caso si accoda il messaggio
                    same_window_messages.append(payload)
                else:
                    # Al contrario nel momento in cui la lista è vuota oppure la finestra temporale cambia viene innescato
                    # l'aggiornamento della dashboard e successivamente la pulizia della lista e l'inserimento dell'ultimo
                    # dato ricevuto al suo interno, che diventa il rappresentante della nuova finestra temporale
                    live.update(self._generate_table(data=same_window_messages))

                    same_window_messages.clear()
                    same_window_messages.append(payload)

    # Stop method - prevede lo stop della dashboard a seguito della ricezione di un segnale SIGINT (CTRL+C), per una 
    # graceful disconnection viene eseguita la chiusura del consumer e dell'admin Kafka con il metodo close(.), inoltre 
    # viene stampato a video un messaggio di informazione
    def stop_dashboard(self):
        close_timeout = 5000
        
        try:
            # Chiusura consumer
            self.get_kafka_client().close(timeout_ms=close_timeout)
            # Chiusura admin
            self.get_kafka_admin().close()
        except Exception:
            sys.stderr.write("\nErrore! Cessazione connessione al broker Kafka fallita\n\n")


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
# Live Dashboard. Successivamente avviene l'elaborazione dei messaggi ricevuti dall'oggetto di dashboarding
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

    # Utilizzo dell'oggetto globale di live dashboarding
    global live_dashboard

    # Instanziazione dell'oggetto Live Dashboard
    live_dashboard = LiveDashboard(brokers_kafka=brokers_kafka)

    print("\nDashboard in esecuzione...\n")

    # Messa in esecuzione della dashboard (elaborazione messaggi)
    live_dashboard.process_messages()


if __name__ == "__main__":
    main()
