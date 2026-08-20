import json
import os
import signal
import sys
import uuid
from pathlib import Path
from typing import Iterable

from dotenv import load_dotenv
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import (KafkaError, NoBrokersAvailable,
                          TopicAlreadyExistsError)
from pyflink.common import Duration, Time, Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import (ProcessWindowFunction,
                                StreamExecutionEnvironment)
from pyflink.datastream.connectors.kafka import (
    DeliveryGuarantee, KafkaOffsetsInitializer, KafkaRecordSerializationSchema,
    KafkaSink, KafkaSource)
from pyflink.datastream.window import SlidingEventTimeWindows, TimeWindow


# Oggetto globale Edge Processing / Processor che permette di contenere un'istanza della classe EdgeProcessing pensata per
# il processamento on edge del sistema di telemetria autobus smart
edge_processing = None


# Handler segnale CTRL+C
def signal_handler(sig_num: int, frame):
    sig_name = signal.Signals(sig_num).name

    # Utilizzo dell'oggetto globale edge processor per accedere all'istanza creata e agire su di essa per
    # una 'graceful disconnection'
    global edge_processing

    # Stop oggetto Processor
    edge_processing.stop_processor()

    # Terminazione
    print(f"Esecuzione interrotta dal segnale {sig_name}")
    sys.exit(0)


# MyTimestampAssigner - classe necessaria per l'assegnamento del corretto timestamp a tutti gli oggetti ricevuti nel 
# datastream, che sarà il timestamp presente all'interno del dato di telemetria ricevuto in modo da abilitare un processing
# all'event time
class MyTimestampAssigner(TimestampAssigner):

    # Metodo extract_timestamp - permette di estrapolare il timestamp dal dato di telemetria
    def extract_timestamp(self, value, record_timestamp) -> int:
        try:
            # Deserializzazione contenuto
            payload = json.loads(value) if isinstance(value, str) else value
            # Estrazione del campo 'timestamp'
            ts_field = payload.get("timestamp")
            ts = float(ts_field) if ts_field is not None else 0.0
        except Exception as e:
            print(f"Errore! Errore di parsing={e} value={value}")
            ts_field = None
            ts = 0.0

        # Predisposizione timestamp in millisecondi [ms] da assegnare
        ts_ms = int(ts * 1000) if ts < 1e12 else int(ts)

        return ts_ms


# MyProcessWindowFunction - classe necessaria per l'elaborazione del datastream una volta suddiviso per chiave, in questo caso
# la targa, permette di mediare i dati di pressione delle gomme e temperatura delle batterie (per quegli autobus smart che ne
# dispongono), e tenere monitorato il numero di failure di impianto frenante e motore
class MyProcessWindowFunction(ProcessWindowFunction):
    # Predisposizione stati di failure per impianto frenante e motore
    status_failure = ["pessimo", "mediocre"]

    def __init__(self, tp_thres: float, bs_thres: int, es_thres: int, bt_thres: float):
        # Setup thresholds
        self._tp_thres = tp_thres
        self._bs_thres = bs_thres
        self._es_thres = es_thres
        self._bt_thres = bt_thres

    # Getter 'tp_thres' parameter
    def get_tp_thres(self):
        return self._tp_thres

    # Getter 'bs_thres' parameter
    def get_bs_thres(self):
        return self._bs_thres

    # Getter 'es_thres' parameter
    def get_es_thres(self):
        return self._es_thres

    # Getter 'bt_thres' parameter
    def get_bt_thres(self):
        return self._bt_thres

    # Metodo process - permette l'effettiva elaborazione dei dati provenienti dal KeyedDataStream
    def process(self, key: str, context: ProcessWindowFunction.Context[TimeWindow], elements: Iterable) -> Iterable:
        # Predisposizione var necessarie
        acc_tp = 0.0
        acc_bt = 0.0
        count_tp = 0
        count_es = 0
        count_bs = 0
        count_bt = 0
        avg_tp = None
        avg_bt = None
        alarm_tp = False
        alarm_es = False
        alarm_bs = False
        alarm_bt = False
        data_type = ""

        for element in elements:
            # element è un dict (JSON parsato [deserializzato])

            # Raccoglimento dati necessari
            tyre_pressure = element.get('collected_metrics', {}).get('tyre_pressure')
            engine_status = element.get('collected_metrics', {}).get('engine_status')
            brake_status = element.get('collected_metrics', {}).get('brake_status')

            hybrid = element.get('collected_metrics', {}).get('hybrid')
            electric = element.get('collected_metrics', {}).get('electric')

            # Verifica se 'tyre_pressure' non è None e in caso in cui non lo è aumento del contatore di dati di pressione
            # delle gomme e somma del dato di pressione delle gomme
            if tyre_pressure is not None:
                acc_tp += float(tyre_pressure)
                count_tp += 1

            # Verifica se il valore di 'engine_status' è presente nella lista di status_failure, in caso in cui lo fosse
            # aumento del contatore di monitoraggio della criticità del parametro suddetto
            if engine_status in MyProcessWindowFunction.status_failure:
                count_es += 1

            # Verifica se il valore di 'brake_status' è presente nella lista di status_failure, in caso in cui lo fosse
            # aumento del contatore di monitoraggio della criticità del parametro suddetto
            if brake_status in MyProcessWindowFunction.status_failure:
                count_bs += 1

            # Verifica della presenza di motorizzazione ibrida o elettrica e assegnazione 'data_type'
            if hybrid is not None or electric is not None:
                # Nel caso in cui l'autobus smart sia ibrido o elettrico aumento del contatore di dati di temperatura
                # delle batterie e somma del dato di temperatura delle batterie
                battery_temp = None
                if hybrid is not None:
                    battery_temp = element.get('collected_metrics', {}).get('hybrid').get('battery_temperature')
                    data_type = "hybrid"
                else:
                    battery_temp = element.get('collected_metrics', {}).get('electric').get('battery_temperature')
                    data_type = "electric"
                
                acc_bt += battery_temp
                count_bt += 1
            else:
                data_type = "termic"

        # Calcolo media per pressione delle gomme
        if count_tp > 0:
            avg_tp = acc_tp / count_tp

        # Calcolo media per temperatura delle batterie
        if count_bt > 0:
            avg_bt = acc_bt / count_bt

        # Verifica dati di pressione delle gomme --> ALLARME se al di sotto di 'tp_thres'
        if avg_tp <= self.get_tp_thres():
            alarm_tp = True

        # Verifica dati di stato del motore --> ALLARME se al di sopra di 'es_thres'
        if count_es >= self.get_es_thres():
            alarm_es = True

        # Verifica dati di stato dell'impianto frenante --> ALLARME se al di sopra di 'bs_thres'
        if count_bs >= self.get_bs_thres():
            alarm_bs = True

        # Predisposizione dato da restituire fornito di:
        #   --> targa
        #   --> tipo del dato
        #   --> inizio della finestra in considerazione
        #   --> fine della finestra in considerazione
        #   --> media di pressione delle gomme
        #   --> conteggio di stati del motore critici
        #   --> conteggio di stati dell'impianto frenante critici
        #   --> allarme di pressione delle gomme
        #   --> allarme di stato del motore
        #   --> allarme di stato dell'impianto frenante
        result = {
            "license_plate": key,
            "type": data_type,
            "window_start": context.window().start,
            "window_end": context.window().end,
            "avg_tyre_press": round(avg_tp, 4),
            "count_engine_stat": count_es,
            "count_brake_stat": count_bs,
            "alarm_tyre_press": alarm_tp,
            "alarm_engine_stat": alarm_es,
            "alarm_brake_stat": alarm_bs
        }

        # Verifica della presenza di motorizzazione ibrida o elettrica
        if hybrid is not None or electric is not None:
            # Verifica dati di temperatura delle batterie --> ALLARME se al di sopra di 'bt_thres'
            if avg_bt >= self.get_bt_thres():
                alarm_bt = True

            # Aggiunta al dato da restituire di:
            #   --> media di temperatura delle batterie
            #   --> allarme di temperatura delle batterie
            result.update(
                {
                    "avg_battery_temp": round(avg_bt, 4),
                    "alarm_battery_temp": alarm_bt
                }
            )

        yield result


# Oggetto EdgeProcessing - processor pensato per mettere a disposizione un Kafka administrator e un environment Flink, il
# primo necessario per la creazione dei topic a cui la Kafka source deve esprimere subscription, altrimenti restituisce
# errore nel caso non siano già presenti nel cluster Kafka; il secondo pensato per eseguire il processing effettivo
# dell'oggetto. Infine mette a disposizione una graceful disconnection attraverso il metodo di stop apposito
class EdgeProcessing:
    def __init__(self, brokers_kafka: list[str], flink_connector_kafka_jar: str) -> None:
        # Kafka setup
        self._brokers_kafka = brokers_kafka.copy()
        self._kafka_admin = self._setup_kafka()
        self._partitions = 1
        self._replication = 3
        self._min_insync_replicas = 2

        # Flink setup
        self._flink_env = self._setup_flink_env(flink_connector=flink_connector_kafka_jar)

        # Parameter thresholds
        self._tyre_pressure_threshold = 2.0
        self._brake_status_threshold = 15
        self._engine_status_threshold = 12
        self._battery_temp_threshold = 45.0
        self._ranges = {
            "battery_temp_min": 5.0,
            "battery_temp_max": 55.0,
            "tyre_pressure_min": 1.0,
            "tyre_pressure_max": 4.5
        }

        # Topics initialization
        self._source_topics = ['AVM.processing.autobus.data.termic', 'AVM.processing.autobus.data.hybrid',
                        'AVM.processing.autobus.data.electric']
        self._create_topic_if_not_exist(topics=self.get_source_topics(), partitions=self.get_partitions(), replication=self.get_replication(), min_insync_replicas=self.get_min_insync_replicas())
        
        self._sink_topics = ['AVM.processing.autobus.dashboard', "AVM.processing.autobus.storage"]
        self._create_topic_if_not_exist(topics=self.get_sink_topics(), partitions=self.get_partitions(), replication=self.get_replication(), min_insync_replicas=self.get_min_insync_replicas())

    # Setup Kafka - metodo necessario alla creazione dell'admin Kafka specificando bootstrap servers a cui deve
    # avvenire la connessione e client_id
    def _setup_kafka(self):
        bootstrap_servers = self.get_brokers_kafka()

        # Preparazione client_id per admin Kafka
        #   utilizzo di PID + primi 8 caratteri esadecimali di UUID v4
        #   --> PID pensato per garantire che processi diversi non abbiano stesso UUID (prevenzione collisioni)
        #   --> UUID pensato per evitare che, nel tempo, processi diversi abbiano stesso PID (prevenzione errori dato
        #       il riutilizzo dei PID nel tempo)
        pid = os.getpid()
        short_uuid = uuid.uuid4().hex[:8]
        admin_client_id = f"AVM_processing_on_edge_admin-{pid}-{short_uuid}"

        # Gestione errore di connessione a broker non disponibili alla connessione
        try:
            # Instanziazione Kafka admin con bootstrap servers a cui deve avvenire la connessione, e client_id

            # Setup Kafka admin
            kafka_admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers, client_id=admin_client_id)
        except NoBrokersAvailable:
            sys.stderr.write("Errore! Nessun broker Kafka disponibile per la connessione\n")
            sys.exit(-6)
        else:
            return kafka_admin

    # Setup Flink - metodo necessario per l'apertura della connessione dell'environment Flink, definire l'intervallo di
    # emissione watermark e l'aggiunta dell'archivio Java (JAR) che contiene la libreria necessaria per sfruttare i
    # plugin di Kafka messi a dispozione da parte di Flink
    def _setup_flink_env(self, flink_connector: str):
        try:
            flink_env = StreamExecutionEnvironment.get_execution_environment()
            flink_env.set_parallelism(1)
            # Emit watermark ogni 100 ms
            flink_env.get_config().set_auto_watermark_interval(100)
            flink_env.add_jars(flink_connector)
        except Exception:
            sys.stderr.write("Errore! Impossibile aprire la connessione verso l'environment Flink\n")
            sys.exit(-7)
        else:
            return flink_env

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

    # Getter 'kafka_admin' parameter
    def get_kafka_admin(self):
        return self._kafka_admin

    # Getter 'flink_env' parameter
    def get_flink_env(self):
        return self._flink_env

    # Getter 'source_topics' parameter
    def get_source_topics(self):
        return self._source_topics.copy()

    # Getter 'sink_topics' parameter
    def get_sink_topics(self):
        return self._sink_topics.copy()

    # Getter 'ranges' parameter
    def get_ranges(self):
        return self._ranges.copy()

    # Getter 'tyre_pressure_threshold' parameter
    def get_tyre_pressure_threshold(self):
        return self._tyre_pressure_threshold

    # Setter 'tyre_pressure_threshold' parameter
    def set_tyre_pressure_threshold(self, threshold: float):
        if type(threshold) is not float:
            raise TypeError(f"Errore! Il tipo del parametro passato deve essere 'float', ricevuto {type(threshold)}")

        min = self.get_ranges()["tyre_pressure_min"]
        max = self.get_ranges()["tyre_pressure_max"]
        if threshold <= min or threshold >= max:
            sys.stderr.write(f"Errore! La soglia passata deve essere maggiore di {min} e minore di {max}, ricevuto {threshold}\n")
            sys.exit(-11)
        else:
            self._tyre_pressure_threshold = threshold

    # Getter 'engine_status_threshold' parameter
    def get_engine_status_threshold(self):
        return self._engine_status_threshold

    # Setter 'engine_status_threshold' parameter
    def set_engine_status_threshold(self, threshold: int):
        if type(threshold) is not int:
            raise TypeError(f"Errore! Il tipo del parametro passato deve essere 'int', ricevuto {type(threshold)}")

        if threshold <= 0:
            sys.stderr.write(f"Errore! La soglia passata deve essere maggiore di zero, ricevuto {threshold}\n")
            sys.exit(-12)
        else:
            self._engine_status_threshold = threshold

    # Getter 'brake_status_threshold' parameter
    def get_brake_status_threshold(self):
        return self._brake_status_threshold

    # Setter 'brake_status_threshold' parameter
    def set_brake_status_threshold(self, threshold: int):
        if type(threshold) is not int:
            raise TypeError(f"Errore! Il tipo del parametro passato deve essere 'int', ricevuto {type(threshold)}")

        if threshold <= 0:
            sys.stderr.write(f"Errore! La soglia passata deve essere maggiore di zero, ricevuto {threshold}\n")
            sys.exit(-13)
        else:
            self._brake_status_threshold = threshold

    # Getter 'battery_temp_threshold' parameter
    def get_battery_temp_threshold(self):
        return self._battery_temp_threshold

    # Setter 'battery_temp_threshold' parameter
    def set_battery_temp_threshold(self, threshold: float):
        if type(threshold) is not float:
            raise TypeError(f"Errore! Il tipo del parametro passato deve essere 'float', ricevuto {type(threshold)}")

        min = self.get_ranges()["battery_temp_min"]
        max = self.get_ranges()["battery_temp_max"]
        if threshold <= min or threshold >= max:
            sys.stderr.write(f"Errore! La soglia passata deve essere maggiore di {min} e minore di {max}, ricevuto {threshold}\n")
            sys.exit(-14)
        else:
            self._battery_temp_threshold = threshold

    # Metodo dedito alla creazione dei topic con i parametri di configurazione desiderati, ossia per fare in modo che
    # il topic sia gestito tra più broker Kafka, con un certo grado di partizione, replica e repliche in-sync (ISR).
    # Verifica se il topic è già presente nel cluster, altrimenti si incarica della creazione
    def _create_topic_if_not_exist(self, topics: list[str], partitions=1, replication=1, min_insync_replicas=1):
        admin = self.get_kafka_admin()

        try:
            # Acquisizione topic presenti nel cluster
            topics_already_created = admin.list_topics()
        except KafkaError:
            sys.stderr.write("Errore! Impossibile ottenere il listato dei topic presenti nel cluster\n")
            sys.exit(-8)

        # Predisposizione lista di oggetti NewTopic da creare
        topics_to_create = []
        for topic in topics:
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
                # Riempimento lista di oggetti NewTopic da creare
                topics_to_create.append(topic_to_create)

        try:
            # Creazione del topic all'interno del cluster
            admin.create_topics(new_topics=topics_to_create)

            for topic_to_create in topics_to_create:
                print(f"\nTopic '{topic_to_create.name}' creato")
            if len(topics_to_create) > 0:
                print()
        except TopicAlreadyExistsError:
            sys.stderr.write("Errore! Impossibile creare un topic che esiste già nel cluster\n")
            sys.exit(-9)
        except KafkaError:
            sys.stderr.write("Errore!\n")
            sys.exit(-10)

    # Metodo define_sink - necessario per la scrittura all'interno del topic Kafka specifica per il job Flink
    # che esegue al di sopra della JVM, per cui necessita di diverse informazioni che normalmente in Python non sarebbero
    # necessarie, come ad esempio il 'KafkaRecordSerializationSchema'
    def _define_sink(self, topic: str, kafka_brokers: str):
        if type(topic) is not str:
            raise TypeError(f"Errore! Il tipo del parametro 'topic' passato deve essere 'str'. Ricevuto {type(topic)}")

        if type(kafka_brokers) is not str:
            raise TypeError(f"Errore! Il tipo del parametro 'kafka_brokers' passato deve essere 'str'. Ricevuto {type(kafka_brokers)}")
        
        # Setup stringa rappresentante l'id del gruppo di cui fa parte il Kafka sink
        group_id = "AVM_dashboarding_processing_producer_group"

        # Setup dello schema di serializzazione necessario per serializzare e comunicare l'oggetto attraverso il sink Kafka
        # verso il topic desiderato
        record_serializer = KafkaRecordSerializationSchema.builder() \
            .set_topic(topic=topic) \
            .set_value_serialization_schema(SimpleStringSchema()) \
            .build()

        # Setup del sink Kafka specificando:
        #   --> record_serializer
        #   --> bootstrap_servers
        #   --> property
        #   --> delivery_guarantee
        kafka_sink = (
            KafkaSink.builder()
            .set_record_serializer(record_serializer)
            .set_bootstrap_servers(kafka_brokers)
            .set_property("group.id", group_id)
            .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
            .build()
        )

        return kafka_sink

    # Metodo process - metodo necessario al fine di predisporre il data stream, ricevuto in ingresso attraverso la
    # Kafka source, per il processamento di quest'ultimo. Eseguite le operazoni di:
    #   --> map
    #   --> key_by
    #   --> windowing
    #   --> process
    #   --> filter
    def process(self):
        # Setup parametri necessari
        #   --> topics
        #   --> environment Flink
        #   --> brokers Kafka
        source_topics = self.get_source_topics()
        sink_topics = self.get_sink_topics()
        env = self.get_flink_env()

        # Setup stringa dei brokers Kafka comma separated da inserire come bootstrap_servers all'interno del Kafka
        # source / sink
        brokers = self.get_brokers_kafka()
        bootstrap_servers = ""
        for broker in brokers:
            bootstrap_servers = bootstrap_servers + broker + ","

        # Setup sink per la scrittura dei data stream all'interno dei topic appropriati al fine di presentazione di una 
        # dashboard live di reporting dei dati ricevuti e di eventuali allarmi
        kafka_sinks = {}
        for topic in sink_topics:
            topic_levels = topic.split(".")

            # Formazione entry dizionario con coppia key - value:
            #   --> key: ultimo livello del topic considerato
            #   --> value: sink verso il topic desiderato
            sink = {
                topic_levels[-1] : self._define_sink(topic=topic, kafka_brokers=bootstrap_servers)
            }

            kafka_sinks.update(sink)

        # Setup informazioni di tipo contenute nel data stream a posteriori del map in JSON dei risultati ricevuti in 
        # seguito al processamento, per cui windowing, calcolo medie, conteggio di status failure ed eventuali allarmi.
        # Fondamentale per comunicare il tipo di dati contenuto nel data stream tra un data stream e l'altro, senza questa
        # informazione il tipo non viene rilevato automaticamente e di conseguenza comporta un fallimento
        type_info = Types.STRING()

        # Setup stringa rappresentante l'id del gruppo di cui fa parte la Kafka source
        group_id = "AVM_processing_consumer_group"

        # Setup soglie necessarie per l'operazione di filter
        tp_thres = self.get_tyre_pressure_threshold()
        es_thres = self.get_engine_status_threshold()
        bs_thres = self.get_brake_status_threshold()
        bt_thres = self.get_battery_temp_threshold()

        # Creazione Kafka source con specifica di topics, deserializer, proprietà, bootstrap_servers e starting_offsets per
        # le partizioni dei topic Kafka
        kafka_source = (
            KafkaSource.builder()
            .set_topics(*source_topics)
            .set_value_only_deserializer(SimpleStringSchema())
            .set_properties({'group.id': group_id})
            .set_bootstrap_servers(bootstrap_servers=bootstrap_servers)
            .set_starting_offsets(KafkaOffsetsInitializer.latest())
            .build()
        )

        # --> for_bounded_out_of_orderness() consente di ammettere messaggi out of order ed attendere questi fino ad un tempo
        #     massimo passato come argomento
        # --> with_idleness() consente di ignorare partizioni idle del topic Kafka (per non tenere bloccato il watermark
        #     globale)
        watermark_strategy = WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(30)) \
                                            .with_idleness(Duration.of_seconds(60)) \
                                            .with_timestamp_assigner(MyTimestampAssigner())

        # Assegnamento watermarks basati sul timestamp del dato di telemetria
        ds = env.from_source(
            source=kafka_source,
            watermark_strategy=watermark_strategy,
            source_name="kafka_source_for_processing"
        )

        # Parse delle stringhe JSON a dizionari Python
        ds_parsed = ds.map(lambda s: json.loads(s))

        # key by license_plate, sliding window dimensionata a 60s, slide 5s, process per window
        ds_windowed_processed = (
            ds_parsed
            .key_by(lambda d: d['license_plate'], key_type=Types.STRING()) \
            .window(SlidingEventTimeWindows.of(Time.seconds(60), Time.seconds(5))) \
            .allowed_lateness(time_ms=90000) \
            .process(MyProcessWindowFunction(tp_thres=tp_thres, bs_thres=bs_thres, es_thres=es_thres, bt_thres=bt_thres))
        )

        # Stampa dei risultati con utilizzo del suffisso per ogni dato presente nel data stream
        # Predisposizione sinking verso il sink Kafka appropriato
        ds_windowed_processed.print("Dati di telemetria nella finestra di interesse:\n")
        # Serializzazione JSON e specifica dell'output type ('type_info') attraverso l'operazione di mapping
        ds_windowed_processed_json = ds_windowed_processed.map(lambda s: json.dumps(s), output_type=type_info)
        # Sinking
        ds_windowed_processed_json.sink_to(kafka_sinks["dashboard"])
        ds_windowed_processed_json.sink_to(kafka_sinks["storage"])

        # Filtraggio dati di pressione delle gomme --> ALLARME se al di sotto di 'tp_thres'
        ds_filtered_tyre_press = ds_windowed_processed.filter(lambda rec: rec.get('alarm_tyre_press') is not None and rec.get('alarm_tyre_press', False) is True)

        # Filtraggio dati di stato del motore --> ALLARME se al di sopra di 'es_thres'
        ds_filtered_engine_stat = ds_windowed_processed.filter(lambda rec: rec.get('alarm_engine_stat', False) is True)

        # Filtraggio dati di stato dell'impianto frenante --> ALLARME se al di sopra di 'bs_thres'
        ds_filtered_brake_stat = ds_windowed_processed.filter(lambda rec: rec.get('alarm_brake_stat', False) is True)

        # Filtraggio dati di temperatura delle batterie --> ALLARME se al di sopra di 'bt_thres'
        ds_filtered_battery_temp = ds_windowed_processed.filter(lambda rec: rec.get('alarm_battery_temp') is not None and rec.get('alarm_battery_temp', False) is True)

        # Stampa degli eventuali risultati filtrati con utilizzo del suffisso per ogni dato presente nel data stream
        ds_filtered_tyre_press.print(f"Dati di telemetria la cui pressione delle gomme mediata non supera i {tp_thres} bar:\n")
        ds_filtered_engine_stat.print("Dati di telemetria il cui stato del motore è in condizioni critiche:\n")
        ds_filtered_brake_stat.print("Dati di telemetria il cui stato dell'impianto frenante è in condizioni critiche:\n")
        ds_filtered_battery_temp.print(f"Dati di telemetria la cui temperatura delle batterie mediata supera i {bt_thres} °C:\n")

        # Esecuzione del job Flink
        env.execute("kafka_sliding_window_process_tp_es_bs_bt")

    # Stop method - prevede lo stop del processor a seguito della ricezione di un segnale SIGINT (CTRL+C), per una 
    # graceful disconnection viene eseguita la chiusura dell'admin Kafka con il metodo close(.), la chiusura
    # dell'environment Flink, inoltre viene stampato a video un messaggio di informazione
    def stop_processor(self):
        try:
            # Chiusura admin Kafka
            self.get_kafka_admin().close()
            # Chiusura environment Flink
            self.get_flink_env().close()
        except Exception:
            sys.stderr.write(f"\nCessazione connessione al broker Kafka / all'environment Flink fallita\n\n")
        else:
            print(f"\nConnessione al broker Kafka e connessione all'environment Flink interrotte correttamente\n")


# Check CMD Line Arguments - verifica dei parametri passati da linea di comando, in particolare relativi ai broker Kafka; per
# tutti gli host viene controllato solamente se l'indirizzo è una stringa non vuota, mentre per tutte le porte si opera un
# controllo sulla validità del numero e se il numero di porta sia uno di quelli standard, ossia 9092, 9094 o 9096. Inoltre
# viene operata una ulteriore verifica sul formato dell'indirizzo IPv4 del broker Kafka, in particolare viene controllato che
# sia esattamente nella forma 'host:port'
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


# main() method - esecuzione del sistema di edge processing relativo agli autobus smart, con controllo dei parametri
# passati da linea di comando, instanziazione dell'oggetto Processor e azionamento del meccanismo di funzionamento
def main():
    # Verifica corretta invocazione del programma
    if len(sys.argv) < 2 or len(sys.argv) > 4:
        sys.stderr.write(f"Errore! Uso coretto del programma: python[3] {sys.argv[0]} $host_kafka-1:$porta_kafka-1 [$host_kafka-2:$porta_kafka-2 $host_kafka-3:$porta_kafka-3]\n")
        sys.stderr.write("\t$host_kafka-* = 'host_Kafka_broker'\n")
        sys.stderr.write("\t$porta_kafka-* = '9092 | 9094 | 9096'\n")
        sys.exit(-1)

    # Installazione handler del segnale CTRL+C
    signal.signal(signalnum=signal.SIGINT, handler=signal_handler)

    # Verifica validità indirizzo broker Kafka
    brokers_kafka_list = check_cmd_line_args(brokers_kafka=sys.argv[1:].copy())

    # Caricamento file di environment
    project_root = Path(__file__).resolve().parents[2]
    env_path = project_root / ".env"
    load_dotenv(dotenv_path=env_path)

    # Verifica robusta della presenza della variabile di environment necessaria
    env_vars = check_env_vars(["FLINK_CONNECTOR_KAFKA_JAR"])

    # Utilizzo dell'oggetto globale di edge processing
    global edge_processing

    # Instanziazione dell'oggetto Processor
    edge_processing = EdgeProcessing(brokers_kafka=brokers_kafka_list, flink_connector_kafka_jar=env_vars["FLINK_CONNECTOR_KAFKA_JAR"])

    # Messa in esecuzione del processor
    edge_processing.process()


if __name__ == '__main__':
    main()
