import random
import socket
import sys
import time
from copy import deepcopy

import numpy as np
from autobus import Autobus
from paho.mqtt.enums import MQTTErrorCode


# Oggetto Autobus Elettrico - sottoclasse che identifica l'oggetto autobus smart di motorizzazione elettrica, 
# estende con le proprie specifiche legate alle batterie le funzioni di:
#   1. Simulazione metriche
#   2. Comunicazione "pacchetto" dati via protocollo MQTT (connessione e pubblicazione del "pacchetto" dati)
#   3. Stampa metriche
class AutobusElettrico(Autobus):
    # Pool di targhe da cui attingere. La decisione è quella di un attributo di classe per garantire l'assegnamento
    # unico ad ogni istanza, attraverso l'eliminazione di una targa dal pool a seguito dell'assegnamento
    pool_electric_license_plates = [
        "AC101BD",
        "BE202CF",
        "DG303EH",
        "FH404IK",
        "JL505MN",
        "KP606QR",
        "LS707ST",
        "MV808WX",
        "NZ909YA",
        "QZ321BX"
    ]

    def __init__(self, ranges: dict, timeout: float, host: str, port: int) -> None:
        super().__init__(ranges=ranges, timeout=timeout, host=host, port=port)

        # Assegnamento unico targa
        license_plate = random.choice(AutobusElettrico.pool_electric_license_plates)
        AutobusElettrico.pool_electric_license_plates.remove(license_plate)
        self.set_LP(license_plate)

        # Metriche specifiche per la motorizzazione elettrica
        self._battery_lvl = 0.0
        self._battery_temp = 0.0

        # Set up soglie per gestione livello batteria
        self._static_threshold = 20.0

        self._threshold_list = []
        for i in np.arange(self._ranges["battery_lvl_low"], self._static_threshold + 1, 1.0):
            self._threshold_list.append( float(i) )

        self._dynamic_threshold = random.choice(self._threshold_list)

        # "Pacchetto" dati specifico per la motorizzazione elettrica
        self._updated_data = {
            "battery_level": self._battery_lvl,
            "battery_temperature": self._battery_temp
        }

        # Aggiornamento "pacchetto" dati generale con l'aggiunta di quello specifico
        self._data_to_send["collected_metrics"]["electric"] = deepcopy(self._updated_data)
        # Sarebbe più adatto utilizzare il metodo copy con copia superficiale perché i valori nelle coppie
        # chiave - valore non sono mai liste o dizionari, sono sempre oggetti immutabili, però per avere un maggiore
        # grado di sicurezza ed essere coperti da eventuali modifiche si utilizza la copia profonda

        # Connessione al broker MQTT
        self.connect_to_mqtt_broker()

        # Coda di messaggi, prevalentemente predisposta per ritardo nel tempo di setup della connessione al broker MQTT
        self._msg_queue = []

    # Connessione Broker MQTT - setup della connessione verso il broker MQTT, impostazione di client_id e dell'effettiva 
    # connessione, con gestione di eventuali errori legati ad indirizzo errato o a broker non disponibile
    def connect_to_mqtt_broker(self):
        mqtt_client = self.get_mqtt_client()
        host = self.get_host()
        port = self.get_port()
        client_id = "autobus_" + self.get_LP()

        # Inizializzazione var per contenere return value della connessione al broker MQTT
        err = None

        # Impostazione client_id
        self.set_mqtt_client_id(client_id.encode())
        try:
            # Connessione verso il broker MQTT
            err = mqtt_client.connect(host=host, port=port, keepalive=60)
        except socket.gaierror:
            sys.stderr.write("Errore! Impossibile risolvere l'indirizzo fornito\n")
            sys.exit(-22)
        except ConnectionRefusedError:
            sys.stderr.write("Errore! Connessione rifiutata\n")
            sys.exit(-23)
        else:
            # Verifica buon fine connessione
            if err == MQTTErrorCode.MQTT_ERR_SUCCESS:
                # Utilizzo del metodo loop_start() che concede di non preoccuparsi di funzionalità utili come la 
                # riconnessione automatica al broker MQTT. Creazione di un thread separato per effettuare le operazioni
                # che seguono
                err_l = mqtt_client.loop_start()

                # Verifica buon fine start background thread
                if err_l != MQTTErrorCode.MQTT_ERR_SUCCESS:
                    print("Errore! Mancata esecuzione del background thread per la comunicazione\n")
                    sys.exit(-24)
            else:
                print("Errore! Connessione non avvenuta\n")
                sys.exit(-25)

    # Getter 'battery_lvl' parameter
    def get_battery_lvl(self):
        return self._battery_lvl

    # Setter 'battery_lvl' parameter
    def set_battery_lvl(self, battery_lvl: float):
        if type(battery_lvl) is not float:
            raise TypeError(f"Errore! Il tipo del parametro passato deve essere 'float'. Ricevuto {type(battery_lvl)}")
        
        self._battery_lvl = battery_lvl
        self._updated_data["battery_level"] = battery_lvl
        self._data_to_send["collected_metrics"]["electric"]["battery_level"] = battery_lvl

    # Getter 'battery_temp' parameter
    def get_battery_temp(self):
        return self._battery_temp

    # Setter 'battery_temp' parameter
    def set_battery_temp(self, battery_temp: float):
        if type(battery_temp) is not float:
            raise TypeError(f"Errore! Il tipo del parametro passato deve essere 'float'. Ricevuto {type(battery_temp)}")
        
        self._battery_temp = battery_temp
        self._updated_data["battery_temperature"] = battery_temp
        self._data_to_send["collected_metrics"]["electric"]["battery_temperature"] = battery_temp

    # Getter 'updated_data' parameter
    def get_updated_data(self):
        return deepcopy(self._updated_data)

    # Setter 'updated_data' parameter
    def set_updated_data(self, updated_data: dict):
        if type(updated_data) is not dict:
            raise TypeError(f"Errore! Il tipo del parametro passato deve essere 'dict'. Ricevuto {type(updated_data)}")

        self._updated_data.update(updated_data)
        self._data_to_send["collected_metrics"]["electric"].update(updated_data)

    # Getter 'threshold_list' parameter
    def get_threshold_list(self):
        return deepcopy(self._threshold_list)
    
    # Setter 'threshold_list' parameter
    def set_threshold_list(self, threshold_list: list):
        if type(threshold_list) is not list:
            raise TypeError(f"Errore! Il tipo del parametro passato deve essere 'list'. Ricevuto {type(threshold_list)}")
        
        self._threshold_list = deepcopy(threshold_list)

    # Getter 'static_threshold' parameter
    def get_static_threshold(self):
        return self._static_threshold

    # Setter 'static_threshold' parameter
    def set_static_threshold(self, static_threshold: float):
        if type(static_threshold) is not float:
            raise TypeError(f"Errore! Il tipo del parametro passato deve essere 'float'. Ricevuto {type(static_threshold)}")
        
        self._static_threshold = static_threshold

        # Modifica della lista da cui generare la soglia dinamica di rifornimento e reimpostazione di questa
        threshold_list = self.get_threshold_list()
        # Verifica che la nuova soglia statica sia maggiore dell'ultimo elemento della lista (soglia precedente)
        if static_threshold > threshold_list[-1]:
            # In questo caso si genera una nuova lista per estendere la precedente
            #   [a, b] && c > b --> [a, b, b+1, c]
            extend_list = []
            for i in np.arange(threshold_list[-1] + 1, static_threshold + 1, 1.0):
                extend_list.append(i)
            
            threshold_list.extend(extend_list)
        # Verifica che la nuova soglia sia al contrario minore dell'ultimo elemento della lista (soglia precedente), 
        # e un numero non negativo
        elif static_threshold < threshold_list[-1] and static_threshold >= 0.0:
            # In questo caso si "taglia" la vecchia lista all'indice dell'elemento già presente al suo interno
            #   [a, b] && c < b --> [a, c]
            upper_bound = threshold_list.index(static_threshold)
            threshold_list = threshold_list[0:upper_bound+1]
        
        self.set_threshold_list(threshold_list)

    # Getter 'dynamic_threshold' parameter
    def get_dynamic_threshold(self):
        return self._dynamic_threshold

    # Setter 'dynamic_threshold' parameter
    def set_dynamic_threshold(self, dynamic_threshold: float):
        if type(dynamic_threshold) is not float:
            raise TypeError(f"Errore! Il tipo del parametro passato deve essere 'float'. Ricevuto {type(dynamic_threshold)}")
        
        self._dynamic_threshold = dynamic_threshold

    # Getter 'msg_queue' parameter
    def get_msg_queue(self):
        return self._msg_queue

    # Simulazione Metriche - metodo che permette di generare ed aggiornare le metriche da comprendere successivamente nel
    # "pacchetto" dati da inviare, attraverso il protocollo di comunicazione MQTT, al sistema di Ingestion. L'aggiornamento
    # avviene in maniera elaborata per sembrare maggiormente verosimile
    def simulate(self, first_exec: bool, fermata_bus: int):
        # Logica:
            # 1. diminuzione quantità prestabilita: livello batteria
            # 2. discostamento massimo all'interno di un intervallo: temperatura batteria

        fermata_bus = super().simulate(first_exec, fermata_bus)

        # Dati prima simulazione hard-coded
        initial_battery_temp = 25.0
        # Dati precedente simulazione
        prec_bt_lvl = self.get_battery_lvl()
        prec_bt_temp = self.get_battery_temp()
        # Limiti massimi di discostamento
        bt_lvl_span = 0.1
        bt_temp_span = 1.75

        if first_exec:
            # Set dati prima simulazione hard-coded
            self.set_battery_lvl(self._ranges["battery_lvl_up"])
            self.set_battery_temp(initial_battery_temp)
        else:
            # Aggiornamento dati successivo alla prima simulazione, fornito di discostamento massimo

            # LIVELLO BATTERIA - diminuzione di una quantità predefinita, ai fini di simulare la durata di una carica
            # della batteria in maniera verosimile
            new_bt_lvl = 0
            
            # Verifica che la quantità di batteria rimasta sia superiore alla soglia dinamica decisa
            if prec_bt_lvl >= self.get_dynamic_threshold():
                # Diminuzione quantità predefinita
                bt_update = round(prec_bt_lvl - bt_lvl_span, 2)
                # Verifica che la diminuzione non abbia comportato un livello negativo
                if bt_update >= self._ranges["battery_lvl_low"]:
                    new_bt_lvl = bt_update
                else:
                    new_bt_lvl = self._ranges["battery_lvl_low"]
            # Se la quantità di batteria rimasta non supera la soglia allora si simula la ricarica della batteria
            # riportando il livello al massimo e si cambia la soglia, in modo da simulare una variabilità del momento
            # in cui la ricarica viene effettuata
            else:
                new_bt_lvl = self._ranges["battery_lvl_up"]
                self.set_dynamic_threshold(random.choice(self.get_threshold_list()))
            
            self.set_battery_lvl(new_bt_lvl)

            # TEMPERATURA BATTERIA - discostamento massimo in un determinato intervallo, in questa maniera rispetto al valore 
            # precedente si mantiene un aggiornamento più sensato
            new_bt_temp = round( random.random() * (self._ranges["battery_temp_up"] - self._ranges["battery_temp_low"]) + self._ranges["battery_temp_low"], 2 )

            # Verifica livello batteria generato randomicamente all'interno dell'intervallo di discostamento massimo dalla
            # precedente misura di livello batteria
            if new_bt_temp not in np.arange(prec_bt_temp - bt_temp_span, prec_bt_temp + bt_temp_span + 0.01, 0.01):

                # Verifica presenza dell'intervallo [prec_bt_temp - bt_temp_span, prec_bt_temp + bt_temp_span] all'interno
                # dell'intervallo generale
                if ( prec_bt_temp - bt_temp_span ) >= self._ranges["battery_temp_low"] and ( prec_bt_temp + bt_temp_span ) <= self._ranges["battery_temp_up"] and ( prec_bt_temp + bt_temp_span ) >= ( prec_bt_temp - bt_temp_span ):
                    self.set_battery_temp( round( random.random() * (( prec_bt_temp + bt_temp_span ) - ( prec_bt_temp - bt_temp_span )) + ( prec_bt_temp - bt_temp_span ), 2 ) )
                # Verifica uscita dall'intervallo generale dell'estremo inferiore
                elif ( prec_bt_temp - bt_temp_span ) < self._ranges["battery_temp_low"] and ( prec_bt_temp + bt_temp_span ) <= self._ranges["battery_temp_up"] and ( prec_bt_temp + bt_temp_span ) >= self._ranges["battery_temp_low"]:
                    self.set_battery_temp( round( random.random() * (( prec_bt_temp + bt_temp_span ) - self._ranges["battery_temp_low"]) + self._ranges["battery_temp_low"], 2 ) )
                # Uscita dall'intervallo generale dell'estremo superiore
                elif ( prec_bt_temp + bt_temp_span ) > self._ranges["battery_temp_up"] and ( prec_bt_temp - bt_temp_span ) >= self._ranges["battery_temp_low"] and self._ranges["battery_temp_up"] >= ( prec_bt_temp - bt_temp_span ):
                    self.set_battery_temp( round( random.random() * (self._ranges["battery_temp_up"] - ( prec_bt_temp - bt_temp_span )) + ( prec_bt_temp - bt_temp_span ), 2 ) )
                else:
                    # Alternativa prevista nel caso in cui venga utilizzato l'oggetto con chiamata unica al metodo e non
                    # all'interno di un ciclo come nell'utilizzo previsto, per cui nel caso in cui i metodi vengano utilizzati
                    # come una sorta di API. Per questo stesso motivo è stata aggiunta la condizione che riguarda i due estremi
                    # , ossia che l'estremo superiore utilizzato per l'aggiornamento sia effettivamente maggiore o uguale di
                    # quello inferiore utilizzato nell'aggiornamento
                    self.set_battery_temp(initial_battery_temp)

            else:
                self.set_battery_temp( new_bt_temp )

        return fermata_bus

    # Comunicazione - comunicazione verso il sistema di Ingestion, ovverosia trasmissione del "pacchetto" dati verso 
    # il broker MQTT, e successiva predisposizione di un bridge per la comunicazione al sistema di Ingestion, ossia
    # Kafka
    def communicate(self):
        mqtt_client = self.get_mqtt_client()
        mqtt_timeout = self.get_timeout()
        payload = self.get_formatted_data_to_send()
        msg_queue = self.get_msg_queue()
        
        # Verifica connessione client to broker
        if mqtt_client.is_connected():
            # Verifica messaggi in coda
            if len(msg_queue) > 0:
                # Per ogni messaggio in coda avviene la pubblicazione di quest'ultimo, in maniera antecedente al 
                # messaggio attuale
                for i in range(0, len(msg_queue)):
                    # Publish con QoS 1 per assicurare la consegna del messaggio
                    msginfo = mqtt_client.publish(topic="AVM/telemetry/autobus/electric", payload=msg_queue[i], qos=1)

                    # Attesa della pubblicazione del messaggio per assicurare una corretta gestione della QoS desiderata.
                    # QoS = 1 indica una qualità del servizio 'at_least_once'
                    before_wait = time.time()
                    msginfo.wait_for_publish(timeout=mqtt_timeout)
                    after_wait = time.time()
                    if after_wait - before_wait >= mqtt_timeout:
                        print(f"Uscita da wait_for_publish() a causa del timeout di {mqtt_timeout} s (messaggio in coda)")
                    else:
                        print(f"Uscita da wait_for_publish() con successo della pubblicazione sul broker del messaggio in coda")

                # Cancellazione dalla coda di tutti i messaggi precedentemente in attesa
                msg_queue.clear()

            # Publish con QoS 1 per assicurare la consegna del messaggio
            msginfo = mqtt_client.publish(topic="AVM/telemetry/autobus/electric", payload=payload, qos=1)

            # Attesa della pubblicazione del messaggio per assicurare una corretta gestione della QoS desiderata.
            # QoS = 1 indica una qualità del servizio 'at_least_once'
            before_wait = time.time()
            msginfo.wait_for_publish(timeout=mqtt_timeout)
            after_wait = time.time()
            if after_wait - before_wait >= mqtt_timeout:
                print(f"Uscita da wait_for_publish() a causa del timeout di {mqtt_timeout} s")
            else:
                print(f"Uscita da wait_for_publish() con successo della pubblicazione sul broker")
        else:
            # Aggiunta messaggio non inviato alla coda di messaggi in attesa
            msg_queue.append(payload)
            print("Messaggio in coda...")

        # Rimozione dello stop del loop per due motivi:
        #   1. Stoppando il loop se la disconnessione al broker avviene nel mentre che il thread del loop non è "vivo"
        #      allora si verificherà un errore, mentre non stoppando il thread questo è sempre alive, e di conseguenza
        #      gestisce correttamente la riconnessione
        #   2. Stoppando il thread porta un notevole overhead sia nello stop che nella ricreazione di quest'ultimo, per
        #      cui questo si traduce in un notevole ritardo da parte degli oggetti autobus che devono aspettare che il 
        #      thread venga stoppato e ricreato (circa 2.5 / 3 secondi)
        # mqtt_client.loop_stop()

    # Stampa a video - stampa a video delle metriche simulate da parte dell'oggetto AutobusElettrico (override)
    def show(self):
        super().show()

        print("Dati motorizzazione elettrica:")
        print(f"\tBatteria rimanente: {self.get_battery_lvl()} %")
        print(f"\tTemperatura batteria: {self.get_battery_temp()} °C\n")
