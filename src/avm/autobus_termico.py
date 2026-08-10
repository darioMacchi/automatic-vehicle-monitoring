import random
import socket
import sys
import time
from copy import deepcopy

import numpy as np
from paho.mqtt.enums import MQTTErrorCode

from avm.autobus import Autobus


# Oggetto Autobus Termico - sottoclasse che identifica l'oggetto autobus smart di motorizzazione termica, 
# estende con le proprie specifiche legate al carburante le funzioni di:
#   1. Simulazione metriche
#   2. Comunicazione "pacchetto" dati via protocollo MQTT (connessione e pubblicazione del "pacchetto" dati)
#   3. Stampa metriche
class AutobusTermico(Autobus):
    # Pool di targhe da cui attingere. La decisione è quella di un attributo di classe per garantire l'assegnamento
    # unico ad ogni istanza, attraverso l'eliminazione di una targa dal pool a seguito dell'assegnamento
    pool_termic_license_plates = [
        "RT123UV", 
        "HK890PL", 
        "SD456GH", 
        "YU321ZX", 
        "NM654TR", 
        "BV207AS", 
        "LK019OD", 
        "QP832WE", 
        "HJ475RT", 
        "ZX908YM"
    ]
    
    def __init__(self, ranges: dict, timeout: float, host: str, port: int) -> None:
        super().__init__(ranges=ranges, timeout=timeout, host=host, port=port)

        # Assegnamento unico targa
        license_plate = random.choice(AutobusTermico.pool_termic_license_plates)
        AutobusTermico.pool_termic_license_plates.remove(license_plate)
        self.set_LP(license_plate)

        # Metriche specifiche per la motorizzazione termica
        self._fuel_lvl = 0.0
        self._fuel_consumption = 0.0

        # Set up soglie per gestione livello carburante
        self._static_threshold = 75.0

        self._threshold_list = []
        for i in np.arange(self._ranges["termic_fuel_lvl_low"], self._static_threshold + 1, 1.0):
            self._threshold_list.append( float(i) )

        self._dynamic_threshold = random.choice(self._threshold_list)

        # "Pacchetto" dati specifico per la motorizzazione termica
        self._updated_data = {
            "fuel_level": self._fuel_lvl,
            "fuel_consumption": self._fuel_consumption
        }

        # Aggiornamento "pacchetto" dati generale con l'aggiunta di quello specifico
        self._data_to_send["collected_metrics"]["termic"] = deepcopy(self._updated_data)
        # Sarebbe più adatto utilizzare il metodo copy con copia superficiale perché i valori nelle coppie
        # chiave - valore non sono mai liste o dizionari, sono sempre oggetti immutabili, però per avere un maggiore
        # grado di sicurezza ed essere coperti da eventuali modifiche si utilizza la copia profonda

        # Connessione al broker MQTT
        self._connect_to_mqtt_broker()
        
        # Coda di messaggi, prevalentemente predisposta per ritardo nel tempo di setup della connessione al broker MQTT
        self._msg_queue = []

    # Connessione Broker MQTT - setup della connessione verso il broker MQTT, impostazione di client_id e dell'effettiva 
    # connessione, con gestione di eventuali errori legati ad indirizzo errato o a broker non disponibile
    def _connect_to_mqtt_broker(self):
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
            sys.exit(-14)
        except ConnectionRefusedError:
            sys.stderr.write("Errore! Connessione rifiutata\n")
            sys.exit(-15)
        else:
            # Verifica buon fine connessione
            if err == MQTTErrorCode.MQTT_ERR_SUCCESS:
                # Utilizzo del metodo loop_start() che concede di non preoccuparsi di funzionalità utili come la 
                # riconnessione automatica al broker MQTT. Creazione di un thread separato per effettuare le operazioni
                # che seguono
                err_l = mqtt_client.loop_start()

                # Verifica buon fine start background thread
                if err_l != MQTTErrorCode.MQTT_ERR_SUCCESS:
                    print("Errore! Mancata esecuzione del background thread per la comunicazione\n")
                    sys.exit(-16)
            else:
                print("Errore! Connessione non avvenuta\n")
                sys.exit(-17)

    # Getter 'fuel_lvl' parameter
    def get_fuel_lvl(self):
        return self._fuel_lvl

    # Setter 'fuel_lvl' parameter
    def set_fuel_lvl(self, fuel_lvl: float):
        if type(fuel_lvl) is not float:
            raise TypeError(f"Errore! Il tipo del parametro passato deve essere 'float'. Ricevuto {type(fuel_lvl)}")
        
        self._fuel_lvl = fuel_lvl
        self._updated_data["fuel_level"] = fuel_lvl
        self._data_to_send["collected_metrics"]["termic"]["fuel_level"] = fuel_lvl

    # Getter 'fuel_consumption' parameter
    def get_fuel_consumption(self):
        return self._fuel_consumption

    # Setter 'fuel_consumption' parameter
    def set_fuel_consumption(self, fuel_consumption: float):
        if type(fuel_consumption) is not float:
            raise TypeError(f"Errore! Il tipo del parametro passato deve essere 'float'. Ricevuto {type(fuel_consumption)}")
        
        self._fuel_consumption = fuel_consumption
        self._updated_data["fuel_consumption"] = fuel_consumption
        self._data_to_send["collected_metrics"]["termic"]["fuel_consumption"] = fuel_consumption

    # Getter 'updated_data' parameter
    def get_updated_data(self):
        return deepcopy(self._updated_data)

    # Setter 'updated_data' parameter
    def set_updated_data(self, updated_data: dict):
        if type(updated_data) is not dict:
            raise TypeError(f"Errore! Il tipo del parametro passato deve essere 'dict'. Ricevuto {type(updated_data)}")

        self._updated_data.update(updated_data)
        self._data_to_send["collected_metrics"]["termic"].update(updated_data)

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
            
            threshold_list.extend(extend_list.copy())
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
        return self._msg_queue.copy()
    
    # Clear 'msg_queue' parameter
    def clear_msg_queue(self):
        self._msg_queue.clear()

    # Append 'msg_queue' parameter
    def append_msg_queue(self, el: str | bytes):
        if type(el) is not str and type(el) is not bytes:
            raise TypeError(f"Errore! Il tipo del parametro passato deve essere 'str' | 'bytes'. Ricevuto {type(el)}")

        self._msg_queue.append(el)

    # Simulazione Metriche - metodo che permette di generare ed aggiornare le metriche da comprendere successivamente nel
    # "pacchetto" dati da inviare, attraverso il protocollo di comunicazione MQTT, al sistema di Ingestion. L'aggiornamento
    # avviene in maniera elaborata per sembrare maggiormente verosimile
    def simulate(self, first_exec: bool, fermata_bus: int):
        # Logica:
            # 1. diminuzione quantità prestabilita: livello carburante
            # 2. diminuzione rispecchiata: consumo carburante

        fermata_bus = super().simulate(first_exec, fermata_bus)

        # Dati precedente simulazione
        prec_fuel_lvl = self.get_fuel_lvl()
        # Limiti massimi di discostamento
        fuel_lvl_span = 0.2

        if first_exec:
            # Set dati prima simulazione hard-coded
            self.set_fuel_lvl(self._ranges["termic_fuel_lvl_up"])
            self.set_fuel_consumption(self._ranges["termic_fuel_cons_low"])
        else:
            # Aggiornamento dati successivo alla prima simulazione, fornito di discostamento massimo

            # LIVELLO CARBURANTE - diminuzione di una quantità predefinita, ai fini di simulare la durata di un pieno di
            # carburante in maniera verosimile
            new_fuel_lvl = 0
            
            # Verifica che la quantità di carburante rimasta sia superiore alla soglia dinamica decisa
            if prec_fuel_lvl >= self.get_dynamic_threshold():
                # Diminuzione quantità predefinita
                fuel_update = round( prec_fuel_lvl - fuel_lvl_span, 2 )
                # Verifica che la diminuzione non abbia comportato un livello negativo
                if fuel_update >= self._ranges["termic_fuel_lvl_low"]:
                    new_fuel_lvl = fuel_update
                else:
                    new_fuel_lvl = self._ranges["termic_fuel_lvl_low"]
            # Se la quantità di carburante rimasta non supera la soglia allora si simula il pieno di carburante riportando
            # il livello al massimo e si cambia la soglia, in modo da simulare una variabilità del momento in cui il
            # pieno viene effettuato
            else:
                new_fuel_lvl = self._ranges["termic_fuel_lvl_up"]
                self.set_dynamic_threshold(random.choice(self.get_threshold_list()))
            
            self.set_fuel_lvl(new_fuel_lvl)

            # CONSUMO CARBURANTE - diminuzione rispecchiata, per cui il consumo di carburante rispecchia il quantitativo
            # di carburante consumato rispetto al livello massimo del serbatoio
            new_fuel_cons = round( self._ranges["termic_fuel_lvl_up"] - new_fuel_lvl, 2)
            self.set_fuel_consumption(new_fuel_cons)
        
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
            # Verifica messaggi in coda. Per ogni messaggio in coda avviene la pubblicazione di quest'ultimo, in
            # maniera antecedente al messaggio attuale
            for msg in msg_queue:
                # Publish con QoS 1 per assicurare la consegna del messaggio in coda
                msginfo = mqtt_client.publish(topic="AVM/telemetry/autobus/termic", payload=msg, qos=1)

                # Attesa della pubblicazione del messaggio per assicurare una corretta gestione della QoS desiderata.
                # QoS = 1 indica una qualità del servizio 'at_least_once'
                before_wait = time.time()
                msginfo.wait_for_publish(timeout=mqtt_timeout)
                after_wait = time.time()
                if after_wait - before_wait >= mqtt_timeout:
                    print(f"Uscita da wait_for_publish() --> timeout di {mqtt_timeout} s scaduto (messaggio in coda)")
                else:
                    print(f"Uscita da wait_for_publish() con successo della pubblicazione sul broker del messaggio in coda")

            # Cancellazione dalla coda di tutti i messaggi precedentemente in attesa
            msg_queue.clear()
            self.clear_msg_queue()

            # Publish con QoS 1 per assicurare la consegna del messaggio corrente
            msginfo = mqtt_client.publish(topic="AVM/telemetry/autobus/termic", payload=payload, qos=1)

            # Attesa della pubblicazione del messaggio per assicurare una corretta gestione della QoS desiderata.
            # QoS = 1 indica una qualità del servizio 'at_least_once'
            before_wait = time.time()
            msginfo.wait_for_publish(timeout=mqtt_timeout)
            after_wait = time.time()
            if after_wait - before_wait >= mqtt_timeout:
                print(f"Uscita da wait_for_publish() --> timeout di {mqtt_timeout} s scaduto")
            else:
                print(f"Uscita da wait_for_publish() con successo della pubblicazione sul broker del messaggio corrente")
        else:
            # Aggiunta messaggio non inviato alla coda di messaggi in attesa
            self.append_msg_queue(el=payload)
            print("Connessione assente --> messaggio in coda...")
        
        # Rimozione dello stop del loop per due motivi:
        #   1. Stoppando il loop se la disconnessione al broker avviene nel mentre che il thread del loop non è "vivo"
        #      allora si verificherà un errore, mentre non stoppando il thread questo è sempre alive, e di conseguenza
        #      gestisce correttamente la riconnessione
        #   2. Stoppando il thread porta un notevole overhead sia nello stop che nella ricreazione di quest'ultimo, per
        #      cui questo si traduce in un notevole ritardo da parte degli oggetti autobus che devono aspettare che il 
        #      thread venga stoppato e ricreato (circa 2.5 / 3 secondi)
        # mqtt_client.loop_stop()

    # Stampa a video - stampa a video delle metriche simulate da parte dell'oggetto AutobusTermico (override)
    def show(self):
        super().show()

        print("Dati motorizzazione termica:")
        print(f"\tCarburante rimanente: {self.get_fuel_lvl()} l")
        print(f"\tConsumo carburante: {self.get_fuel_consumption()} l\n")
