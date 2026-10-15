import json
import random
import sys
import time
from copy import deepcopy

import numpy as np
import paho.mqtt.client as mqtt
import paho.mqtt.reasoncodes as mqttrc
from paho.mqtt.enums import MQTTErrorCode


# Oggetto Autobus - superclasse che identifica l'oggetto autobus smart indipendentemente dalla sua motorizzazione, 
# raccoglie le funzioni comuni a tutte le motorizzazioni, in particolare:
#   1. Simulazione metriche
#   2. Preparazione "pacchetto" dati con formati differenti (JSON o binario)
#   3. Stampa metriche
class Autobus:
    def __init__(self, ranges: dict, timeout: float, host: str, port: int) -> None:
        # Check del tipo del parametro 'ranges' passato
        if type(ranges) is not dict:
            raise TypeError(f"Errore! Il tipo del parametro 'ranges' passato deve essere 'dict'. Ricevuto {type(ranges)}")
        # Check del tipo del parametro 'timeout' passato
        if type(timeout) is not float:
            raise TypeError(f"Errore! Il tipo del parametro 'timeout' passato deve essere 'float'. Ricevuto {type(timeout)}")

        # Timestamp event time sorgente
        self._timestamp = 0.0
        # Targa identificativa
        self._LP = ""

        # Ranges entro cui deve avvenire la generazione delle misure
        # Metodo deepcopy() utilizzato per creare una copia profonda del dizionario, per fare in modo che qualsiasi
        # utilizzo e modifica venga effettuata all'interno dell'oggetto Autobus non si rifletta sull'oggetto originale.
        # Forzato all'utilizzo di deepcopy() e non della sola copy(), dato che quest'ultima effettua una copia
        # superficiale del dizionario, che però contiene liste, ossia oggetti mutabili, e di conseguenza eventuali
        # modifiche su queste verrebbero riportate anche nel dizionario originale
        self._ranges = deepcopy(ranges)

        # Metriche da raccogliere
        self._gps = {
            "latitude": 0.0,
            "longitude": 0.0
        }
        self._speed = 0.0
        self._tyre_pressure = 0.0
        self._brake_status = ""
        self._engine_status = ""
        self._num_psg = 0
        self._environmental_data = {
            "temperature": 0.0,
            "humidity": 0.0
        }

        # Direzioni spostamento coordinate di latitudine e longitudine, necessarie per la simulazione delle metriche
        self._lat_direction = "NORD"
        self._long_direction = "EST"

        # Preparazione "pacchetto" dati da inviare verso il sistema di Ingestion
        self._data_to_send = {
            "license_plate": self._LP,
            "timestamp": self._timestamp,
            "collected_metrics": {
                "gps": deepcopy(self._gps),
                "speed": self._speed,
                "tyre_pressure": self._tyre_pressure,
                "brake_status": self._brake_status,
                "engine_status": self._engine_status,
                "num_psg": self._num_psg,
                "environmental": deepcopy(self._environmental_data),
            },
        }
        self._formatted_data_to_send = ""

        # Client, timeout, host e porta MQTT
        self._mqtt_client = self._setup_mqtt()
        self._timeout = timeout
        self._host = host
        self._port = port

        # Prefisso del topic a cui inviare gli eventi di allarme
        self._critic_topic_prefix = "AVM/alarm/autobus"
    
    # on_connect - callback necessaria per il protocollo di comunicazione MQTT per gestire il momento in cui 
    # il client riceve una risposta CONNACK dal server (broker RabbitMQ) - firma prestabilita
    def _on_connect(self, client, userdata, flags: mqtt.ConnectFlags, reason_code: mqttrc.ReasonCode, properties):
        if reason_code.is_failure:
            print(f"Fallimento connessione: {reason_code}. loop_start() proverà a riconnettersi\n")
        else:
            print(f"Connessione con result code {reason_code}")
            print("Il broker detiene ancora informazioni per il client: ", end="")
            print("SI\n" if flags.session_present else "NO\n")

    # on_connect_fail - callback necessaria per il protocollo di comunicazione MQTT per gestire il momento in cui
    # avviene il fallimento nello stabilire una connessione automatica da parte di loop_start()
    def _on_connect_fail(self, client, userdata):
        print("Fallito stabilimento della (ri)connessione TCP automatica verso il broker da parte di loop_start()")

    # Setup MQTT - metodo necessario alla creazione del client MQTT specificando versione delle callback, client_id 
    # (modificato dalle sottoclassi) e sessione pulita (ossia non persistente). Vengono inoltre specificate le relative
    # callback necessarie ai fini di corretta gestione di connessione e fallimento alla riconessione automatica
    def _setup_mqtt(self):
        client_id = "autobus_"

        # Setup client MQTT
        mqttc = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2, client_id=client_id, clean_session=True)
        mqttc.on_connect = self._on_connect
        mqttc.on_connect_fail = self._on_connect_fail

        return mqttc

    # Getter 'mqtt_client' parameter
    def get_mqtt_client(self):
        return self._mqtt_client

    # Setter 'mqtt_client' parameter
    def set_mqtt_client(self, mqtt_client: mqtt.Client):
        if not isinstance(mqtt_client, mqtt.Client):
            raise TypeError(f"Errore! Il tipo del parametro passato deve essere 'paho.mqtt.client.Client'. Ricevuto {type(mqtt_client)}")

        self._mqtt_client = mqtt_client
        
    # Getter 'mqtt_client_id' parameter
    def get_mqtt_client_id(self):
        return self._mqtt_client._client_id

    # Setter 'mqtt_client_id' parameter
    def set_mqtt_client_id(self, client_id: bytes):
        if type(client_id) is not bytes:
            raise TypeError(f"Errore! Il tipo del parametro passato deve essere 'bytes'. Ricevuto {type(client_id)}")

        self._mqtt_client._client_id = client_id

    # Getter 'timeout' parameter
    def get_timeout(self):
        return self._timeout

    # Setter 'timeout' parameter
    def set_timeout(self, timeout: float):
        if type(timeout) is not float:
            raise TypeError(f"Errore! Il tipo del parametro passato deve essere 'float'. Ricevuto {type(timeout)}")

        self._timeout = timeout

    # Getter 'host' parameter
    def get_host(self):
        return self._host

    # Setter 'host' parameter
    def set_host(self, host: str):
        if type(host) is not str:
            raise TypeError(f"Errore! Il tipo del parametro passato deve essere 'str'. Ricevuto {type(host)}")

        self._host = host

    # Getter 'port' parameter
    def get_port(self):
        return self._port

    # Setter 'port' parameter
    def set_port(self, port: int):
        if type(port) is not int:
            raise TypeError(f"Errore! Il tipo del parametro passato deve essere 'int'. Ricevuto {type(port)}")

        self._port = port

    # Getter 'LP' parameter
    def get_LP(self):
        return self._LP

    # Setter 'LP' parameter
    def set_LP(self, LP: str):
        if type(LP) is not str:
            raise TypeError(f"Errore! Il tipo del parametro passato deve essere 'str'. Ricevuto {type(LP)}")

        self._LP = LP
        self._data_to_send["license_plate"] = LP

    # Getter 'timestamp' parameter
    def get_timestamp(self):
        return self._timestamp

    # Setter 'timestamp' parameter
    def set_timestamp(self, timestamp: float):
        if type(timestamp) is not float:
            raise TypeError(f"Errore! Il tipo del parametro passato deve essere 'float'. Ricevuto {type(timestamp)}")

        self._timestamp = timestamp
        self._data_to_send["timestamp"] = timestamp

    # Getter 'gps' parameter
    def get_gps(self):
        return deepcopy(self._gps)

    # Setter 'gps' parameter
    def set_gps(self, gps: dict):
        if type(gps) is not dict:
            raise TypeError(f"Errore! Il tipo del parametro passato deve essere 'dict'. Ricevuto {type(gps)}")

        self._gps.update(gps)
        self._data_to_send["collected_metrics"]["gps"].update(gps)

    # Getter 'latitude' key of 'gps' parameter
    def get_latitude(self):
        return self._gps["latitude"]

    # Setter 'latitude' key of 'gps' parameter
    def set_latitude(self, latitude: float):
        if type(latitude) is not float:
            raise TypeError(f"Errore! Il tipo del parametro passato deve essere 'float'. Ricevuto {type(latitude)}")

        self._gps["latitude"] = latitude
        self._data_to_send["collected_metrics"]["gps"]["latitude"] = latitude

    # Getter 'longitude' key of 'gps' parameter
    def get_longitude(self):
        return self._gps["longitude"]

    # Setter 'longitude' key of 'gps' parameter
    def set_longitude(self, longitude: float):
        if type(longitude) is not float:
            raise TypeError(f"Errore! Il tipo del parametro passato deve essere 'float'. Ricevuto {type(longitude)}")

        self._gps["longitude"] = longitude
        self._data_to_send["collected_metrics"]["gps"]["longitude"] = longitude

    # Getter 'speed' parameter
    def get_speed(self):
        return self._speed

    # Setter 'speed' parameter
    def set_speed(self, speed: float):
        if type(speed) is not float:
            raise TypeError(f"Errore! Il tipo del parametro passato deve essere 'float'. Ricevuto {type(speed)}")

        self._speed = speed
        self._data_to_send["collected_metrics"]["speed"] = speed

    # Getter 'tyre_pressure' parameter
    def get_tyre_pressure(self):
        return self._tyre_pressure

    # Setter 'tyre_pressure' parameter
    def set_tyre_pressure(self, tyre_pressure: float):
        if type(tyre_pressure) is not float:
            raise TypeError(f"Errore! Il tipo del parametro passato deve essere 'float'. Ricevuto {type(tyre_pressure)}")

        self._tyre_pressure = tyre_pressure
        self._data_to_send["collected_metrics"]["tyre_pressure"] = tyre_pressure

    # Getter 'brake_status' parameter
    def get_brake_status(self):
        return self._brake_status

    # Setter 'brake_status' parameter
    def set_brake_status(self, brake_status: str):
        if type(brake_status) is not str:
            raise TypeError(f"Errore! Il tipo del parametro passato deve essere 'str'. Ricevuto {type(brake_status)}")

        self._brake_status = brake_status
        self._data_to_send["collected_metrics"]["brake_status"] = brake_status

    # Getter 'engine_status' parameter
    def get_engine_status(self):
        return self._engine_status

    # Setter 'engine_status' parameter
    def set_engine_status(self, engine_status: str):
        if type(engine_status) is not str:
            raise TypeError(f"Errore! Il tipo del parametro passato deve essere 'str'. Ricevuto {type(engine_status)}")

        self._engine_status = engine_status
        self._data_to_send["collected_metrics"]["engine_status"] = engine_status

    # Getter 'num_psg' parameter
    def get_num_psg(self):
        return self._num_psg

    # Setter 'num_psg' parameter
    def set_num_psg(self, num_psg: int):
        if type(num_psg) is not int:
            raise TypeError(f"Errore! Il tipo del parametro passato deve essere 'int'. Ricevuto {type(num_psg)}")

        self._num_psg = num_psg
        self._data_to_send["collected_metrics"]["num_psg"] = num_psg

    # Getter 'environmental_data' parameter
    def get_environmental_data(self):
        return deepcopy(self._environmental_data)

    # Setter 'environmental_data' parameter
    def set_environmental_data(self, env_data: dict):
        if type(env_data) is not dict:
            raise TypeError(f"Errore! Il tipo del parametro passato deve essere 'dict'. Ricevuto {type(env_data)}")

        self._environmental_data.update(env_data)
        self._data_to_send["collected_metrics"]["environmental"].update(env_data)

    # Getter 'temperature' parameter
    def get_temperature(self):
        return self._environmental_data["temperature"]

    # Setter 'temperature' parameter
    def set_temperature(self, temperature: float):
        if type(temperature) is not float:
            raise TypeError(f"Errore! Il tipo del parametro passato deve essere 'float'. Ricevuto {type(temperature)}")

        self._environmental_data["temperature"] = temperature
        self._data_to_send["collected_metrics"]["environmental"]["temperature"] = temperature

    # Getter 'humidity' parameter
    def get_humidity(self):
        return self._environmental_data["humidity"]

    # Setter 'humidity' parameter
    def set_humidity(self, humidity: float):
        if type(humidity) is not float:
            raise TypeError(f"Errore! Il tipo del parametro passato deve essere 'float'. Ricevuto {type(humidity)}")

        self._environmental_data["humidity"] = humidity
        self._data_to_send["collected_metrics"]["environmental"]["humidity"] = humidity

    # Getter 'lat_direction' parameter
    def get_lat_direction(self):
        return self._lat_direction

    # Setter 'lat_direction' parameter
    def set_lat_direction(self, lat_direction: str):
        if type(lat_direction) is not str:
            raise TypeError(f"Errore! Il tipo del parametro passato deve essere 'str'. Ricevuto {type(lat_direction)}")

        # Verifica parametro 'lat_direction' sia all'interno dell'insieme di stringhe {NORD, SUD, nord, sud}
        lat_direction = lat_direction.upper()
        if lat_direction != "NORD" and lat_direction != "SUD":
            sys.stderr.write("Errore! Il parametro passato deve essere: NORD | SUD | nord | sud\n")
            sys.exit(-1)

        self._lat_direction = lat_direction

    # Getter 'long_direction' parameter
    def get_long_direction(self):
        return self._long_direction

    # Setter 'long_direction' parameter
    def set_long_direction(self, long_direction: str):
        if type(long_direction) is not str:
            raise TypeError(f"Errore! Il tipo del parametro passato deve essere 'str'. Ricevuto {type(long_direction)}")

        # Verifica parametro 'long_direction' sia all'interno dell'insieme di stringhe {EST, OVEST, est, ovest}
        long_direction = long_direction.upper()
        if long_direction != "EST" and long_direction != "OVEST":
            sys.stderr.write("Errore! Il parametro passato deve essere: EST | OVEST | est | ovest\n")
            sys.exit(-2)

        self._long_direction = long_direction

    # Getter 'data_to_send' parameter
    def get_data_to_send(self):
        return deepcopy(self._data_to_send)

    # Getter 'formatted_data_to_send' parameter
    def get_formatted_data_to_send(self):
        return self._formatted_data_to_send

    # Setter 'formatted_data_to_send' parameter
    def set_formatted_data_to_send(self, formatted_data_to_send: str):
        if type(formatted_data_to_send) is not str:
            raise TypeError(f"Errore! Il tipo del parametro passato deve essere 'str'. Ricevuto {type(formatted_data_to_send)}")

        self._formatted_data_to_send = formatted_data_to_send

    # Getter 'critic_topic_prefix' parameter
    def get_critic_topic_prefix(self):
        return self._critic_topic_prefix

    # Generazione randomici in un intervallo - il metodo prevede i parametri che definiscono i bound entro cui generare il
    # numero randomico, inoltre desidera in ingresso un flag 'integer' che discrimina i casi di generazione di un randomico
    # intero o floating point, e mette a disposizione anche il parametro 'digit_to_round' per specificare il numero di cifre
    # da mantenere dopo la virgola
    def _generate_random_number_in_a_range(self, upper_bound: float | int, lower_bound: float | int, integer: bool, digit_to_round=1) -> float | int:
        rand_number = None

        # Verifica flag 'integer' al fine di appurare se la chiamata riguarda la generazione di un randomico intero o 
        # in virgola mobile all'interno di in un intervallo
        if integer:
            rand_number = random.randrange(lower_bound, upper_bound)
        else:
            rand_number = round( random.random() * (upper_bound - lower_bound) + lower_bound, digit_to_round )

        return rand_number

    # Algoritmo generazione coordinate - prevede la generazione di coordinate GPS all'interno di un percorso unico
    # identificato dai parametri di limite superiore e inferiore del range. Il metodo predisposto pretende in ingresso anche
    # il parametro di direzione attuale 'direction', la coordinata precedente 'past_coordinate', la quantità da aggiungere o
    # rimuovere alla coordinata 'coordinate_span', infine la direzione positiva e la direzione negativa 'positive_direction'
    # e 'negative_direction', che permettono di astrarre la logica delle direzioni distinte tra latitudine e longitudine,
    # ossia NORD/SUD per la latitudine, mentre EST/OVEST per la longitudine
    def _generate_gps_coordinate(self, direction: str, past_coordinate: float, coordinate_span: float, positive_direction: str, negative_direction: str, low_limit_coordinate: float, up_limit_coordinate: float) -> tuple[float, str]:
        coordinate = 0.0

        # Impostazione delle coordinate con aggiunta e sottrazione di una quantità fissata
        added_coordinate = round(past_coordinate + coordinate_span, 5)
        subtracted_coordinate = round(past_coordinate - coordinate_span, 5)
        
        # Finché lo spostamento avviene in una direzione e il limite massimo non viene superato si continua ad
        # aggiungere o sottrarre, nel momento in cui il limite massimo viene superato si inverte la direzione e 
        # l'operazione di addizione o sottrazione
        if direction == positive_direction and added_coordinate <= up_limit_coordinate and added_coordinate >= low_limit_coordinate:
            coordinate = added_coordinate
        elif direction == positive_direction and added_coordinate > up_limit_coordinate and added_coordinate >= low_limit_coordinate:
            direction = negative_direction
            coordinate = subtracted_coordinate
        elif direction == negative_direction and subtracted_coordinate >= low_limit_coordinate and subtracted_coordinate <= up_limit_coordinate:
            coordinate = subtracted_coordinate
        elif direction == negative_direction and subtracted_coordinate < low_limit_coordinate and subtracted_coordinate <= up_limit_coordinate:
            direction = positive_direction
            coordinate = added_coordinate
        else:
            # Alternativa prevista nel caso in cui venga utilizzato l'oggetto con chiamata unica al metodo e non
            # all'interno di un ciclo come nell'utilizzo previsto, per cui nel caso in cui i metodi vengano utilizzati
            # come una sorta di API. Per questo stesso motivo è stata aggiunta la condizione che riguarda l'estremo opposto
            # , ossia quella di essere effettivamente all'interno del range
            coordinate = low_limit_coordinate

        return coordinate, direction

    # Algoritmo generazione metriche - prevede la generazione di metriche randomiche all'interno di un intervallo per uno
    # scostamento limitato dalla precedente metrica, in modo da simulare un funzionamento verosimile. I parametri richiesti
    # sono i limiti dell'intervallo, la metrica precedente, la quantità da aggiungere e sottrarre alla metrica precedente per
    # definire l'intervallo di scostamento, alcuni flag che discrimano i casi di metrica intera, ambientale o che abbia 
    # necessità di un valore iniziale, e infine il parametro necessario per specificare il numero di cifre da mantenere dopo
    # la virgola 
    def _generate_floating_point_or_integer_metric(self, up_limit: float | int, low_limit: float | int, past_metric: float | int, metric_span: float | int, integer: bool, initial: bool, environment: bool, initial_metric=0.0, digit_to_round=1) -> float | int:
        metric = self._generate_random_number_in_a_range(upper_bound=up_limit, lower_bound=low_limit, integer=integer, digit_to_round=digit_to_round)

        # Impostazione delle metriche con aggiunta e sottrazione di una quantità fissata
        added_metric = past_metric + metric_span
        subtracted_metric = past_metric - metric_span

        # Impostazione dell'intervallo da cui attingere la metrica random
        metric_range = None
        if integer:
            metric_range = range(subtracted_metric, added_metric + 1)
        else:
            metric_range = np.arange(subtracted_metric, added_metric + 0.1, 0.1)
        
        # Verifica metrica generata randomicamente all'interno dell'intervallo di discostamento massimo dalla
        # precedente misura della metrica
        if metric not in metric_range:

            # Verifica presenza dell'intervallo [past_metric - metric_span, past_metric + metric_span] all'interno
            # dell'intervallo generale
            if subtracted_metric >= low_limit and added_metric <= up_limit and added_metric >= subtracted_metric:
                metric = self._generate_random_number_in_a_range(upper_bound=added_metric, lower_bound=subtracted_metric, integer=integer, digit_to_round=digit_to_round)
            # Verifica uscita dall'intervallo generale dell'estremo inferiore
            elif subtracted_metric < low_limit and added_metric <= up_limit and added_metric >= low_limit:
                metric = self._generate_random_number_in_a_range(upper_bound=added_metric, lower_bound=low_limit, integer=integer, digit_to_round=digit_to_round)
            # Uscita dall'intervallo generale dell'estremo superiore
            elif added_metric > up_limit and subtracted_metric >= low_limit and up_limit >= subtracted_metric:
                metric = self._generate_random_number_in_a_range(upper_bound=up_limit, lower_bound=subtracted_metric, integer=integer, digit_to_round=digit_to_round)
            else:
                # Alternativa prevista nel caso in cui venga utilizzato l'oggetto con chiamata unica al metodo e non
                # all'interno di un ciclo come nell'utilizzo previsto, per cui nel caso in cui i metodi vengano utilizzati
                # come una sorta di API. Per questo stesso motivo è stata aggiunta la condizione che riguarda i due estremi
                # , ossia che l'estremo superiore utilizzato per l'aggiornamento sia effettivamente maggiore o uguale di
                # quello inferiore utilizzato nell'aggiornamento
                
                # Verifica presenza metrica ambientale
                if environment:
                    metric = self._generate_random_number_in_a_range(upper_bound=up_limit, lower_bound=low_limit, integer=integer, digit_to_round=digit_to_round)
                # Verifica presenza valore iniziale per la metrica
                elif initial:
                    # Assicurazione del tipo intero del valore iniziale passato
                    if integer:
                        try:
                            initial_metric = int(initial_metric)
                        except ValueError:
                            initial_metric = 0
                    
                    metric = initial_metric
                # Fallback impostazione valore massimo per la metrica
                else:
                    metric = up_limit

        return metric

    # Algoritmo generazione metriche di stato - prevede la generazione di metriche riguardo alla stato di impianti o sistemi,
    # per generare stati successivi verosimili viene prelevato il nuovo stato a partire dal precedente e dal suo intorno,
    # dove il suo intorno è definito dallo stato alla sua sinistra e alla sua destra. I parametri richiesti sono la lista
    # contenente tutti gli stati possibili, e la metrica precedente
    def _generate_status_metric(self, status_list: list, past_metric: str) -> str:
        new_status_metric = None
        status_list_to_pick_from = []
        
        # Verifica che la stringa precedente sia non vuota, questo per assicurare che l'utilizzo avvenga all'interno di un
        # ciclo, come da utilizzo inteso dell'oggetto
        if past_metric != "":
            past_index = status_list.index(past_metric)

            # Impostazione degli indici con aggiunta e sottrazione di una quantità fissata
            added_past_index = past_index + 1
            subtracted_past_index = past_index - 1

            # Verifica indice stato precedente, se posto ai boundaries viene escluso uno stato nella lista da cui 
            # prelevare lo stato successivo, in particolare se lo stato precedente è a indice 0 viene escluso quello alla
            # sua sinistra ( index: -1 ), mentre se lo stato precedente è a indice len(status_list) - 1 viene escluso quello
            # alla sua destra ( index: len(status_list) )
            if subtracted_past_index >= 0 and added_past_index < len(status_list):
                status_list_to_pick_from = [status_list[subtracted_past_index], past_metric, status_list[added_past_index]]
            elif subtracted_past_index < 0:
                status_list_to_pick_from = [past_metric, status_list[added_past_index]]
            elif added_past_index >= len(status_list):
                status_list_to_pick_from = [status_list[subtracted_past_index], past_metric]
        else:
            # Alternativa prevista nel caso in cui venga utilizzato l'oggetto con chiamata unica al metodo e non
            # all'interno di un ciclo come nell'utilizzo previsto, per cui nel caso in cui i metodi vengano utilizzati
            # come una sorta di API. Per questo stesso motivo è stata aggiunta la verifica sulla stringa precedente
            # , ossia che sia non vuota per prevenire eventuali errori indesiderati
            status_list_to_pick_from = [status_list[-3], status_list[-2], status_list[-1]]
        
        new_status_metric = random.choice(status_list_to_pick_from)

        return new_status_metric

    # Simulazione Metriche - metodo che permette di generare ed aggiornare le metriche da comprendere successivamente nel
    # "pacchetto" dati da inviare, attraverso il protocollo di comunicazione MQTT, al sistema di Ingestion. L'aggiornamento
    # avviene in maniera elaborata per sembrare maggiormente verosimile
    def simulate(self, first_exec: bool, fermata_bus: int):
        if type(first_exec) is not bool:
            raise TypeError(f"Errore! Il tipo del parametro passato deve essere 'bool'. Ricevuto {type(first_exec)}")
        
        if type(fermata_bus) is not int:
            raise TypeError(f"Errore! Il tipo del parametro passato deve essere 'int'. Ricevuto {type(fermata_bus)}")

        # Logica:
        # 1. copertura di un percorso unico: latitudine e longitudine
        # 2. discostamento massimo all'interno di un intervallo: velocità, pressione gomme, numero passeggeri, temperatura,
        #                                                        umidità, stato impianto frenante, stato motore

        # Dati prima simulazione hard-coded
        initial_speed = 10.0
        initial_num_psg = 10
        # Dati precedente simulazione
        prec_latitude = self.get_latitude()
        prec_longitude = self.get_longitude()
        prec_speed = self.get_speed()
        prec_tyre_pressure = self.get_tyre_pressure()
        prec_num_psg = self.get_num_psg()
        prec_temperature = self.get_temperature()
        prec_humidity = self.get_humidity()
        prec_brake_status = self.get_brake_status()
        prec_engine_status = self.get_engine_status()
        # Limiti massimi di discostamento
        lat_span = 0.00005
        long_span = 0.00005
        speed_span = 20.0
        tyre_span = 0.15
        temp_span = 2.5
        hum_span = 10
        psg_span = 20

        if first_exec:
            # Set dati prima simulazione hard-coded
            self.set_latitude(self._ranges["gps"]["latitude_low"])
            self.set_longitude(self._ranges["gps"]["longitude_low"])
            self.set_speed(initial_speed)
            self.set_tyre_pressure(self._ranges["tyre_pressure_up"])
            self.set_num_psg(initial_num_psg)
            self.set_temperature( round( random.random() * (self._ranges["environmental"]["temp_up"] - self._ranges["environmental"]["temp_low"]) + self._ranges["environmental"]["temp_low"], 3 ) )
            self.set_humidity( round( random.random() * (self._ranges["environmental"]["hum_up"] - self._ranges["environmental"]["hum_low"]) + self._ranges["environmental"]["hum_low"], 3 ) )
            self.set_brake_status(self._ranges["brake_status"][-1])
            self.set_engine_status(self._ranges["engine_status"][-1])
        else:
            # Aggiornamento dati successivo alla prima simulazione, fornito di discostamento massimo

            # LATITUDINE - copertura di un percorso unico tra le due coordinate limite
            new_lat, new_lat_direction = self._generate_gps_coordinate(direction=self.get_lat_direction(), past_coordinate=prec_latitude, coordinate_span=lat_span, positive_direction="NORD", negative_direction="SUD", low_limit_coordinate=self._ranges["gps"]["latitude_low"], up_limit_coordinate=self._ranges["gps"]["latitude_up"])

            self.set_lat_direction(new_lat_direction)
            self.set_latitude(new_lat)

            # LONGITUDINE - copertura di un percorso unico tra le due coordinate limite
            new_long, new_long_direction = self._generate_gps_coordinate(direction=self.get_long_direction(), past_coordinate=prec_longitude, coordinate_span=long_span, positive_direction="EST", negative_direction="OVEST", low_limit_coordinate=self._ranges["gps"]["longitude_low"], up_limit_coordinate=self._ranges["gps"]["longitude_up"])

            self.set_long_direction(new_long_direction)
            self.set_longitude(new_long)

            # VELOCITà - discostamento massimo in un determinato intervallo, in questa maniera rispetto al valore 
            # precedente si mantiene un aggiornamento più sensato
            new_speed = self._generate_floating_point_or_integer_metric(up_limit=self._ranges["speed_up"], low_limit=self._ranges["speed_low"], past_metric=prec_speed, metric_span=speed_span, integer=False, initial=True, environment=False, initial_metric=initial_speed, digit_to_round=1)
            
            self.set_speed( new_speed )

            # PRESSIONE GOMME - discostamento massimo in un determinato intervallo, in questa maniera rispetto al valore 
            # precedente si mantiene un aggiornamento più sensato
            new_tyre_pressure = self._generate_floating_point_or_integer_metric(up_limit=self._ranges["tyre_pressure_up"], low_limit=self._ranges["tyre_pressure_low"], past_metric=prec_tyre_pressure, metric_span=tyre_span, integer=False, initial=False, environment=False, digit_to_round=2)

            self.set_tyre_pressure( new_tyre_pressure )

            # Verifica sulla frequenza delle fermate, in modo che la fermata del bus avvenga solamente una volta ogni
            # 120s, il che è più verosimile della variazione ogni 5 secondi come per le altre metriche.
            # Il 25 dipende dalla frequenza di produzione metriche, che al momento è di 5 secondi, per cui 5*24 = 120, e
            # di conseguenza per fare in modo che quei 120 secondi passino le esecuzioni devono essere 24 + 1, perché il
            # ciclo è produzione - delay, quindi se facessi 24 avrei in realtà che alla 24esima esecuzione faccio la 
            # fermata, quando però sono passati 115 secondi e non 120.
            # Questo è vero per il primo "ciclo", dopo di ché se restituissi 0 ne farei uno in più, perché ho il ciclo in
            # cui ho cambiato i passeggeri e resitituisco 0, poi devo fare altre 25 esecuzioni del Ciclo azioni per 
            # arrivare di nuovo a 25 e cambiare. Per questo motivo restituisco 1, che idealmente rappresenta anche il 
            # numero di esecuzioni con lo stesso numero di passeggeri, quella appena prodotta è la prima e poi ne voglio
            # altre 24
            if fermata_bus == 25:
                # NUMERO PASSEGGERI - discostamento massimo in un determinato intervallo, in questa maniera rispetto al valore 
                # precedente si mantiene un aggiornamento più sensato
                new_psg = self._generate_floating_point_or_integer_metric(up_limit=self._ranges["num_psg_up"], low_limit=self._ranges["num_psg_low"], past_metric=prec_num_psg, metric_span=psg_span, integer=True, initial=True, environment=False, initial_metric=initial_num_psg)

                self.set_num_psg( new_psg )

                fermata_bus = 1

            # TEMPERATURA - discostamento massimo in un determinato intervallo, in questa maniera rispetto al valore 
            # precedente si mantiene un aggiornamento più sensato
            new_temp = self._generate_floating_point_or_integer_metric(up_limit=self._ranges["environmental"]["temp_up"], low_limit=self._ranges["environmental"]["temp_low"], past_metric=prec_temperature, metric_span=temp_span, integer=False, initial=False, environment=True, digit_to_round=3)

            self.set_temperature( new_temp )

            # UMIDITà - discostamento massimo in un determinato intervallo, in questa maniera rispetto al valore 
            # precedente si mantiene un aggiornamento più sensato
            new_hum = self._generate_floating_point_or_integer_metric(up_limit=self._ranges["environmental"]["hum_up"], low_limit=self._ranges["environmental"]["hum_low"], past_metric=prec_humidity, metric_span=hum_span, integer=False, initial=False, environment=True, digit_to_round=3)

            self.set_humidity( new_hum )

            # STATO IMPIANTO FRENANTE - discostamento massimo in un determinato intervallo, in questa maniera rispetto al
            # valore precedente si mantiene un aggiornamento più sensato. Leggera differenza operando su di una lista, si 
            # prende direttamente il valore precedente e si forma una nuova lista da cui pescare random solamente con
            # l'elemento precedente e il successivo e il precedente
            new_brake_status = self._generate_status_metric(status_list=self._ranges["brake_status"], past_metric=prec_brake_status)

            self.set_brake_status(new_brake_status)

            # STATO MOTORE - discostamento massimo in un determinato intervallo, in questa maniera rispetto al valore 
            # precedente si mantiene un aggiornamento più sensato. Leggera differenza operando su di una lista, si prende
            # direttamente il valore precedente e si forma una nuova lista da cui pescare random solamente con l'elemento
            # precedente e il successivo e il precedente
            new_engine_status = self._generate_status_metric(status_list=self._ranges["engine_status"], past_metric=prec_engine_status)

            self.set_engine_status(new_engine_status)

        return fermata_bus

    # Formattazione Dati - formattazione e preparazione del "pacchetto" dati da inviare attraverso il protocollo di
    # comunicazione MQTT, formato dati JSON, che è largamente utilizzato nell'Internet per il suo formato portabile e
    # human-readable largamente riconosciuto dalla community. Disponibile anche il formato dati binario
    def format_data(self, data_type: str):
        if type(data_type) is not str:
            raise TypeError(f"Errore! Il tipo del parametro passato deve essere 'str'. Ricevuto {type(data_type)}")

        # Set timestamp in modo che sia valorizzato il più vicino possibile alla comunicazione, ma al contempo prima
        # di formattare il "pacchetto" dati da inviare
        self.set_timestamp(time.time())

        if data_type.upper() == "JSON":
            self._formatted_data_to_send = json.dumps(self.get_data_to_send())
        elif data_type.upper() == "BINARY":
            self._formatted_data_to_send = json.dumps(self.get_data_to_send()).encode()

    # Stampa a Video - stampa a video delle metriche simulate da parte dell'oggetto Autobus
    def show(self):
        print(f"Metriche raccolte per l'autobus con targa {self.get_LP()} all'event time {self.get_timestamp()}:")
        print("Dati satellitari:")
        print(f"\tLatitudine: {self.get_latitude()} °N")
        print(f"\tLongitudine: {self.get_longitude()} °E")
        print(f"Velocità: {self.get_speed()} km/h")
        print(f"Pressione pneumatici: {self.get_tyre_pressure()} bar")
        print(f"Stato dell'impianto frenante: {self.get_brake_status()}")
        print(f"Stato del motore: {self.get_engine_status()}")
        print(f"Numero di passeggeri a bordo: {self.get_num_psg()}")
        print("Dati ambientali:")
        print(f"\tTemperatura: {self.get_temperature()} °C")
        print(f"\tUmidità: {self.get_humidity()} %")

    # Gestione Eventi - gestione evento avvenuto sull'autobus comunicando alla centrale operativa l'evento accaduto
    # e terminando l'esecuzione dell'autobus. Terminazione a seguito dell'evento di spia motore per evitare eventuali
    # problemi di natura meccanica che potrebbero diventare catastrofici, mentre terminazione a seguito dell'evento di 
    # panic button per far evacuare i passeggeri presenti a bordo
    def handle_critic_events(self, event_msg: dict):
        if type(event_msg) is not dict:
            raise TypeError(f"Errore! Il tipo del parametro passato deve essere 'dict'. Ricevuto {type(event_msg)}")

        # Finalizzazione topic di allarme a cui inviare l'evento avvenuto
        topic = self.get_critic_topic_prefix() + "/" + event_msg["type"]
        event_msg.update(
            {
                "timestamp": time.time()
            }
        )
        event_msg.pop("happened")

        print("Avvenuto evento di tipo: ", end="")
        print("SPIA MOTORE" if event_msg["type"] == "enginelight" else "PANIC BUTTON")

        # Comunicazione dell'evento di allarme
        self.communicate(alarm=True, topic=topic, message=json.dumps(event_msg))

        # Fermo dell'autobus a seguito dell'evento accaduto
        self.stop_autobus()

    # Stop Autobus - terminazione delle connnessioni dell'oggetto Autobus, in particolare cessazione della connessione col
    # broker MQTT per la comunicazione, e terminazione del background thread previsto per la gestione del traffico di rete,
    # inoltre stampa a video di un messaggio informativo
    def stop_autobus(self):
        # Disconnessione broker MQTT
        err_d = self.get_mqtt_client().disconnect()
        # Terminazione background thread
        err_s = self.get_mqtt_client().loop_stop()

        print("Connessione al broker cessata con ", end="")
        print("successo " if err_d == MQTTErrorCode.MQTT_ERR_SUCCESS else "insuccesso ", end="")
        print("e terminazione background thread avvenuta con ", end="")
        print("successo " if err_s == MQTTErrorCode.MQTT_ERR_SUCCESS else "insuccesso ", end="")
        print(f"per l'autobus con targa {self.get_LP()}\n")
