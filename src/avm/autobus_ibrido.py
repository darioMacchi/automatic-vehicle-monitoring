import random
from copy import deepcopy

from avm.autobus import Autobus


# Oggetto Autobus Ibrido - sottoclasse che identifica l'oggetto autobus smart di motorizzazione ibrida, 
# estende con le proprie specifiche legate al carburante e alle batterie le funzioni di:
#   1. Simulazione metriche
#   2. Comunicazione "pacchetto" dati via protocollo MQTT (connessione e pubblicazione del "pacchetto" dati)
#   3. Stampa metriche
class AutobusIbrido(Autobus):
    # Pool di targhe da cui attingere. La decisione è quella di un attributo di classe per garantire l'assegnamento
    # unico ad ogni istanza, attraverso l'eliminazione di una targa dal pool a seguito dell'assegnamento
    pool_hybrid_license_plates = [
        "AB742CD",
        "FM905ZT",
        "GX310QV",
        "PL456RN",
        "TR039KM",
        "DS881YU",
        "VN124HB",
        "QW207ER",
        "MC670LS",
        "ZY315OP"
    ]

    def __init__(self, ranges: dict, timeout: float, host: str, port: int) -> None:
        super().__init__(ranges=ranges, timeout=timeout, host=host, port=port)

        # Assegnamento unico targa
        license_plate = random.choice(AutobusIbrido.pool_hybrid_license_plates)
        AutobusIbrido.pool_hybrid_license_plates.remove(license_plate)
        self.set_LP(license_plate)

        # Metriche specifiche per la motorizzazione ibrida
        self._battery_lvl = 0.0
        self._battery_temp = 0.0
        self._fuel_lvl = 0.0
        self._fuel_consumption = 0.0

        # Set up soglie per gestione livello carburante
        static_threshold = 50.0
        self._initialize_thresholds(list_lower_bound=self._ranges["hybrid_fuel_lvl_low"], static_threshold=static_threshold)

        # "Pacchetto" dati specifico per la motorizzazione ibrida
        self._updated_data = {
            "battery_level": self._battery_lvl,
            "battery_temperature": self._battery_temp,
            "fuel_level": self._fuel_lvl,
            "fuel_consumption": self._fuel_consumption
        }

        # Aggiornamento "pacchetto" dati generale con l'aggiunta di quello specifico
        self._data_to_send["collected_metrics"]["hybrid"] = deepcopy(self._updated_data)
        # Sarebbe più adatto utilizzare il metodo copy con copia superficiale perché i valori nelle coppie
        # chiave - valore non sono mai liste o dizionari, sono sempre oggetti immutabili, però per avere un maggiore
        # grado di sicurezza ed essere coperti da eventuali modifiche si utilizza la copia profonda

        # Definizione topic MQTT
        topic = "AVM/telemetry/autobus/hybrid"
        self._set_topic(topic)

        # Connessione al broker MQTT
        self._connect_to_mqtt_broker()

    # Getter 'battery_lvl' parameter
    def get_battery_lvl(self):
        return self._battery_lvl

    # Setter 'battery_lvl' parameter
    def set_battery_lvl(self, battery_lvl: float):
        if type(battery_lvl) is not float:
            raise TypeError(f"Errore! Il tipo del parametro passato deve essere 'float'. Ricevuto {type(battery_lvl)}")
        
        self._battery_lvl = battery_lvl
        self._updated_data["battery_level"] = battery_lvl
        self._data_to_send["collected_metrics"]["hybrid"]["battery_level"] = battery_lvl

    # Getter 'battery_temp' parameter
    def get_battery_temp(self):
        return self._battery_temp

    # Setter 'battery_temp' parameter
    def set_battery_temp(self, battery_temp: float):
        if type(battery_temp) is not float:
            raise TypeError(f"Errore! Il tipo del parametro passato deve essere 'float'. Ricevuto {type(battery_temp)}")
        
        self._battery_temp = battery_temp
        self._updated_data["battery_temperature"] = battery_temp
        self._data_to_send["collected_metrics"]["hybrid"]["battery_temperature"] = battery_temp

    # Getter 'fuel_lvl' parameter
    def get_fuel_lvl(self):
        return self._fuel_lvl

    # Setter 'fuel_lvl' parameter
    def set_fuel_lvl(self, fuel_lvl: float):
        if type(fuel_lvl) is not float:
            raise TypeError(f"Errore! Il tipo del parametro passato deve essere 'float'. Ricevuto {type(fuel_lvl)}")
        
        self._fuel_lvl = fuel_lvl
        self._updated_data["fuel_level"] = fuel_lvl
        self._data_to_send["collected_metrics"]["hybrid"]["fuel_level"] = fuel_lvl

    # Getter 'fuel_consumption' parameter
    def get_fuel_consumption(self):
        return self._fuel_consumption

    # Setter 'fuel_consumption' parameter
    def set_fuel_consumption(self, fuel_consumption: float):
        if type(fuel_consumption) is not float:
            raise TypeError(f"Errore! Il tipo del parametro passato deve essere 'float'. Ricevuto {type(fuel_consumption)}")
        
        self._fuel_consumption = fuel_consumption
        self._updated_data["fuel_consumption"] = fuel_consumption
        self._data_to_send["collected_metrics"]["hybrid"]["fuel_consumption"] = fuel_consumption

    # Getter 'updated_data' parameter
    def get_updated_data(self):
        return deepcopy(self._updated_data)

    # Setter 'updated_data' parameter
    def set_updated_data(self, updated_data: dict):
        if type(updated_data) is not dict:
            raise TypeError(f"Errore! Il tipo del parametro passato deve essere 'dict'. Ricevuto {type(updated_data)}")

        self._updated_data.update(updated_data)
        self._data_to_send["collected_metrics"]["hybrid"].update(updated_data)

    # Simulazione Metriche - metodo che permette di generare ed aggiornare le metriche da comprendere successivamente nel
    # "pacchetto" dati da inviare, attraverso il protocollo di comunicazione MQTT, al sistema di Ingestion. L'aggiornamento
    # avviene in maniera elaborata per sembrare maggiormente verosimile
    def simulate(self, first_exec: bool, fermata_bus: int):
        # Logica:
            # 1. discostamento massimo all'interno di un intervallo: livello batteria, temperatura batteria
            # 2. diminuzione quantità prestabilita: livello carburante
            # 3. diminuzione rispecchiata: consumo carburante
        
        fermata_bus = super().simulate(first_exec, fermata_bus)

        # Dati prima simulazione hard-coded
        initial_battery_temp = 25.0
        # Dati precedente simulazione
        prec_bt_lvl = self.get_battery_lvl()
        prec_bt_temp = self.get_battery_temp()
        prec_fuel_lvl = self.get_fuel_lvl()
        # Limiti massimi di discostamento
        bt_lvl_span = 10.0
        bt_temp_span = 1.75
        fuel_lvl_span = 0.15

        if first_exec:
            # Set dati prima simulazione hard-coded
            self.set_battery_lvl(self._ranges["battery_lvl_up"])
            self.set_battery_temp(initial_battery_temp)
            self.set_fuel_lvl(self._ranges["hybrid_fuel_lvl_up"])
            self.set_fuel_consumption(self._ranges["hybrid_fuel_cons_low"])
        else:
            # Aggiornamento dati successivo alla prima simulazione, fornito di discostamento massimo

            # LIVELLO BATTERIA - discostamento massimo in un determinato intervallo, in questa maniera rispetto al valore 
            # precedente si mantiene un aggiornamento più sensato
            new_battery_level = self._generate_floating_point_or_integer_metric(up_limit=self._ranges["battery_lvl_up"], low_limit=self._ranges["battery_lvl_low"], past_metric=prec_bt_lvl, metric_span=bt_lvl_span, integer=False, initial=False, environment=False, digit_to_round=2)

            self.set_battery_lvl( new_battery_level )

            # TEMPERATURA BATTERIA - discostamento massimo in un determinato intervallo, in questa maniera rispetto al
            # valore precedente si mantiene un aggiornamento più sensato
            new_battery_temperature = self._generate_floating_point_or_integer_metric(up_limit=self._ranges["battery_temp_up"], low_limit=self._ranges["battery_temp_low"], past_metric=prec_bt_temp, metric_span=bt_temp_span, integer=False, initial=True, environment=False, initial_metric=initial_battery_temp, digit_to_round=2)

            self.set_battery_temp( new_battery_temperature )

            # LIVELLO CARBURANTE - diminuzione di una quantità predefinita, ai fini di simulare la durata di un pieno di
            # carburante in maniera verosimile
            new_fuel_level = self._generate_level_metric(past_level=prec_fuel_lvl, level_span=fuel_lvl_span, lower_level_bound=self._ranges["hybrid_fuel_lvl_low"], upper_level_bound=self._ranges["hybrid_fuel_lvl_up"])
            
            self.set_fuel_lvl(new_fuel_level)

            # CONSUMO CARBURANTE - diminuzione rispecchiata, per cui il consumo di carburante rispecchia il quantitativo
            # di carburante consumato rispetto al livello massimo del serbatoio
            new_fuel_cons = round( self._ranges["hybrid_fuel_lvl_up"] - new_fuel_level, 2)
            self.set_fuel_consumption(new_fuel_cons)

        return fermata_bus

    # Stampa a video - stampa a video delle metriche simulate da parte dell'oggetto AutobusIbrido (override)
    def show(self):
        super().show()

        print("Dati motorizzazione ibrida:")
        print(f"\tBatteria rimanente: {self.get_battery_lvl()} %")
        print(f"\tTemperatura batteria: {self.get_battery_temp()} °C")
        print(f"\tCarburante rimanente: {self.get_fuel_lvl()} l")
        print(f"\tConsumo carburante: {self.get_fuel_consumption()} l\n")
