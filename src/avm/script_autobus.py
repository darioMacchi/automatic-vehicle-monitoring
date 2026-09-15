import random
import signal
import sys
import time
from copy import deepcopy

from avm.autobus_elettrico import AutobusElettrico
from avm.autobus_ibrido import AutobusIbrido
from avm.autobus_termico import AutobusTermico


# Lista di autobus smart con motorizzazione termica
termic_bus_list = []
# Lista di autobus smart con motorizzazione ibrida
hybrid_bus_list = []
# Lista di autobus smart con motorizzazione elettrica
electric_bus_list = []


# Handler del segnale CTRL+C - metodo con cui si termina l'esecuzione di tutti gli Autobus con un messaggio, successivamente
# avviene la cessazione di tutte le connessioni e la terminazione dei thread di background in cui avviene la comunicazione
# con il broker RabbitMQ per il protocollo di comunicazione MQTT, e infine avviene la terminazione con codice uscita 0
# (funzionamento corretto) - firma del handler deve essere 'handler_name(sig_num, frame)'
def shutdown_all_autobus(sig_num: int, frame):
    sig_name = signal.Signals(sig_num).name

    # Utilizzo delle liste di autobus smart di tutte le motorizzazioni per accedere alle istanze create e agire su di esse
    # per una 'graceful disconnection'
    global termic_bus_list
    global hybrid_bus_list
    global electric_bus_list

    print("")

    # Disconnessione dal broker MQTT per tutti gli Autobus e terminazione di tutti i background thread
    for termic_bus in termic_bus_list:
        termic_bus.stop_autobus()
    
    for hybrid_bus in hybrid_bus_list:
        hybrid_bus.stop_autobus()

    for electric_bus in electric_bus_list:
        electric_bus.stop_autobus()

    print(f"Esecuzione interrotta dal segnale {sig_name}")
    print("Spegnimento motore...")
    sys.exit(0)


# Check CMD Line Arguments - verifica dei parametri passati da linea di comando, in particolare relativi al numero di
# autobus da costituire divisi per tipologia di motorizzazione. Viene operato un controllo sul tipo dei paramteri passati,
# che in quanto passati da linea di comando mi aspetto essere stringhe, e successivo controllo che il numero passato,
# oltre che valido, sia anche un numero non negativo, per l'ovvia ragione che non è possibile avere un numero negativo di
# autobus. Infine l'ultimo controllo operato è quello del non superamento della soglia massima di autobus data dal numero
# massimo di targhe disponibili per classe di motorizzazione.
# Altri parametri su cui viene eseguito il controllo sono host e porta del broker MQTT; per l'host viene controllato 
# solamente se l'indirizzo è una stringa non vuota, mentre per la porta si opera un controllo sulla validità del numero
# e se il numero di porta sia 1883 o 8883, porte standard per MQTT
def check_cmd_line_args(termic_autobus_num: str, hybrid_autobus_num: str, electric_autobus_num: str, host: str, port: str):
    if type(termic_autobus_num) is not str:
        raise TypeError(f"Errore! Il tipo del parametro 'termic_autobus_num' passato deve essere 'str'. Ricevuto {type(termic_autobus_num)}")

    if type(hybrid_autobus_num) is not str:
        raise TypeError(f"Errore! Il tipo del parametro 'hybrid_autobus_num' passato deve essere 'str'. Ricevuto {type(hybrid_autobus_num)}")

    if type(electric_autobus_num) is not str:
        raise TypeError(f"Errore! Il tipo del parametro 'electric_autobus_num' passato deve essere 'str'. Ricevuto {type(electric_autobus_num)}")
    
    if type(host) is not str:
        raise TypeError(f"Errore! Il tipo del parametro 'host' passato deve essere 'str'. Ricevuto {type(host)}")
    
    if type(port) is not str:
        raise TypeError(f"Errore! Il tipo del parametro 'port' passato deve essere 'str'. Ricevuto {type(port)}")

    termic_num = 0
    hybrid_num = 0
    electric_num = 0
    host_mqtt = ""
    port_mqtt = 0

    # Termic
    # Check numero valido
    try:
        termic_num = int(termic_autobus_num)
    except ValueError:
        sys.stderr.write("Errore! L'argomento $num_autobus_termici passato da linea di comando non è un numero valido\n")
        sys.exit(-2)

    # Check numero non negativo
    if termic_num < 0:
        sys.stderr.write("Errore! L'argomento $num_autobus_termici deve essere un numero maggiore o uguale a zero\n")
        sys.exit(-3)

    # Check numero non superiore al numero di targhe disponibili
    termic_num_max = len(AutobusTermico.pool_termic_license_plates)
    if termic_num > termic_num_max:
        sys.stderr.write("Errore! L'argomento $num_autobus_termici deve essere un numero non superiore a {}\n".format(termic_num_max))
        sys.exit(-4)

    # Hybrid
    # Check numero valido
    try:
        hybrid_num = int(hybrid_autobus_num)
    except ValueError:
        sys.stderr.write("Errore! L'argomento $num_autobus_ibridi passato da linea di comando non è un numero valido\n")
        sys.exit(-5)

    # Check numero non negativo
    if hybrid_num < 0:
        sys.stderr.write("Errore! L'argomento $num_autobus_ibridi deve essere un numero maggiore o uguale a zero\n")
        sys.exit(-6)

    # Check numero non superiore al numero di targhe disponibili
    hybrid_num_max = len(AutobusIbrido.pool_hybrid_license_plates)
    if hybrid_num > hybrid_num_max:
        sys.stderr.write("Errore! L'argomento $num_autobus_ibridi deve essere un numero non superiore a {}\n".format(hybrid_num_max))
        sys.exit(-7)

    # Electric
    # Check numero valido
    try:
        electric_num = int(electric_autobus_num)
    except ValueError:
        sys.stderr.write("Errore! L'argomento $num_autobus_elettrici passato da linea di comando non è un numero valido\n")
        sys.exit(-8)

    # Check numero non negativo
    if electric_num < 0:
        sys.stderr.write("Errore! L'argomento $num_autobus_elettrici deve essere un numero maggiore o uguale a zero\n")
        sys.exit(-9)

    # Check numero non superiore al numero di targhe disponibili
    electric_num_max = len(AutobusElettrico.pool_electric_license_plates)
    if electric_num > electric_num_max:
        sys.stderr.write("Errore! L'argomento $num_autobus_elettrici deve essere un numero non superiore a {}\n".format(electric_num_max))
        sys.exit(-10)

    # Host
    # Check stringa non vuota
    host_mqtt = host
    if host_mqtt == "":
        sys.stderr.write("Errore! L'argomento $host deve essere un indirizzo non nullo\n")
        sys.exit(-11)

    # Port
    # Check numero valido
    try:
        port_mqtt = int(port)
    except ValueError:
        sys.stderr.write("Errore! L'argomento $porta passato da linea di comando non è un numero valido\n")
        sys.exit(-12)

    # Check porta
    if port_mqtt != 1883 and port_mqtt != 8883:
        sys.stderr.write("Errore! L'argomento $porta deve essere una porta MQTT valida: 1883 oppure 8883 (connessioni SSL)\n")
        sys.exit(-13)

    return termic_num, hybrid_num, electric_num, host_mqtt, port_mqtt


# Events License Plates SAFE - verifica dell'uguaglianza tra le targhe degli eventi generati, per evitare situazioni 
# dove entrambi gli eventi avvengono su uno stesso autobus, perché il primo ad avvenire impedirebbe l'avvenimento
# dell'altro, dato che dopo un qualsiasi evento di questo genere l'autobus viene fermato
def events_license_plates_safe(lp_engine: str, lp_pbutton: str, time_engine: float, time_pbutton: float, lp_list: list):
    if type(lp_engine) is not str:
        raise TypeError(f"Errore! Il tipo del parametro 'lp_engine' passato deve essere 'str'. Ricevuto {type(lp_engine)}")

    if type(lp_pbutton) is not str:
        raise TypeError(f"Errore! Il tipo del parametro 'lp_pbutton' passato deve essere 'str'. Ricevuto {type(lp_pbutton)}")

    if type(time_engine) is not float:
            raise TypeError(f"Errore! Il tipo del parametro 'time_engine' passato deve essere 'float'. Ricevuto {type(time_engine)}")

    if type(time_pbutton) is not float:
            raise TypeError(f"Errore! Il tipo del parametro 'time_pbutton' passato deve essere 'float'. Ricevuto {type(time_pbutton)}")

    if type(lp_list) is not list:
        raise TypeError(f"Errore! Il tipo del parametro 'lp_list' passato deve essere 'list'. Ricevuto {type(lp_list)}")

    # Verifica medesime targhe per entrambi gli eventi generati
    if lp_engine == lp_pbutton:
        # Aggiornamento della targa dell'evento che è stato creato da meno tempo, quindi dando priorità all'evento
        # generato da più tempo, ossia l'evento più "vecchio"
        if time_engine < time_pbutton:
            # Rimozione targa evento più "vecchio"
            lp_list.remove(lp_engine)
            try:
                # Pick della nuova targa dell'evento più "giovane" nella lista modificata
                lp_pbutton = random.choice(lp_list)
            except IndexError:
                # Verifica lista vuota, random.choice() su una lista vuota fallisce e genera una eccezione IndexError,
                # perché non può scegliere nessun elemento, per verificare che sia effettivamente questo il caso viene
                # verificata l'assenza di elementi dalla lista, in quel caso la targa dei due eventi deve essere per
                # forza la stessa
                if len(lp_list) == 0:
                    lp_pbutton = lp_engine
        else:
            # Rimozione targa evento più "vecchio"
            lp_list.remove(lp_pbutton)
            try:
                # Pick della nuova targa dell'evento più "giovane" nella lista modificata
                lp_engine = random.choice(lp_list)
            except IndexError:
                # Verifica lista vuota, random.choice() su una lista vuota fallisce e genera una eccezione IndexError,
                # perché non può scegliere nessun elemento, per verificare che sia effettivamente questo il caso viene
                # verificata l'assenza di elementi dalla lista, in quel caso la targa dei due eventi deve essere per
                # forza la stessa
                if len(lp_list) == 0:
                    lp_engine = lp_pbutton

    return lp_engine, lp_pbutton


# Method main() - esecuzione del sistema di telemetria AVM relativo agli autobus smart, con controllo dei parametri 
# passati da linea di comando, setup dei range per le metriche, del formato dati e altri informazioni necessarie alla 
# corretta esecuzione, instanziazione degli autobus smart con diverse motorizzazioni e Ciclo azioni con le operazioni
# di gestione eventi, simulazione metriche, preparazione e invio "pacchetto" dati e stampa a video
def main():
    # Verifica corretta invocazione del programma
    if len(sys.argv) != 6:
        sys.stderr.write(f"Errore! Uso coretto del programma: python[3] {sys.argv[0]} $num_autobus_termici $num_autobus_ibridi $num_autobus_elettrici $host $porta\n")
        sys.stderr.write("\t$num_autobus_termici = 'num_autobus_termici'\n")
        sys.stderr.write("\t$num_autobus_ibridi = 'num_autobus_ibridi'\n")
        sys.stderr.write("\t$num_autobus_elettrici = 'num_autobus_elettrici'\n")
        sys.stderr.write("\t$host = 'host_MQTT_broker'\n")
        sys.stderr.write("\t$porta = '1883' | '8883'\n")
        sys.exit(-1)

    # Ranges intervallo misure
    ranges = {
        #       GPS
        "gps": {
            #   [°N]
            "latitude_low": 44.49321,
            "latitude_up": 44.83591,
            #   [°E]
            "longitude_low": 11.27662,
            "longitude_up": 11.61932
        },
        #       [km/h]
        "speed_low": 0.0,
        "speed_up": 100.0,
        #       [bar]
        "tyre_pressure_low": 1.0,
        "tyre_pressure_up": 4.5,
        #       Brake Status
        "brake_status": ["pessimo", "mediocre", "cattivo", "accettabile", "buono", "ottimo", "eccellente"],
        #       Engine Status
        "engine_status": ["pessimo", "mediocre", "cattivo", "accettabile", "buono", "ottimo", "eccellente"],
        #       [persone]
        "num_psg_low": 0,
        "num_psg_up": 75,
        "environmental": {
            #   [°C]
            "temp_low": -5.0,
            "temp_up": 30.0,
            #   [%]
            "hum_low": 0.0,
            "hum_up": 100.0
        },
        #       [%]
        "battery_lvl_low": 0.0,
        "battery_lvl_up": 100.0,
        #       [°C]
        "battery_temp_low": 5.0,
        "battery_temp_up": 55.0,
        #       [l]
        "termic_fuel_lvl_low": 0.0,
        "termic_fuel_lvl_up": 480.0,
        #       [l]
        "termic_fuel_cons_low": 0.0,
        "termic_fuel_cons_up": 480.0,
        #       [l]
        "hybrid_fuel_lvl_low": 0.0,
        "hybrid_fuel_lvl_up": 400.0,
        #       [l]
        "hybrid_fuel_cons_low": 0.0,
        "hybrid_fuel_cons_up": 400.0
    }

    # Formato dati
    format = "JSON"
    # Setup ritardo in secondi [s]
    delay_metrics = 5.0
    # Setup ritardo accensione motore
    delay_setup = 2.0

    # Setup timeout attesa pubblicazione messaggio broker MQTT
    # TODO
    # Ridimensionare (al momento è troppo alto perché per ogni messaggio aspettare quasi 5 secondi di timeout per la
    # pubblicazione del messaggio è tanto, soprattutto se ci sono tanti messaggi in coda [magari a seguito di una
    # perdita di connessione col broker MQTT])
    delay_mqtt = 4.90
    # Numero autobus:
    #   Termici
    #   Ibridi
    #   Elettrici
    termic_num = 0
    hybrid_num = 0
    electric_num = 0
    # Flag necessario a segnalare la prima esecuzione del Ciclo azioni
    first_exec = True

    # Contatore necessario a segnalare il numero di esecuzioni del Ciclo azioni, e di conseguenza la frequenza
    # delle fermate
    fermata_bus = 0
    # Lista necessaria a verificare la correttezza dei contatori fermata bus restituiti dai diversi autobus smart
    update_fermata_bus_list = []

    # Predisposizione parametri mu e sigma della distribuzione gaussiana da cui estrarre il numero che comporterà
    # l'avvenimento o meno dell'evento spia motore. I parametri sono stati scelti in modo che l'evento sia poco
    # probabile in un numero di esecuzioni alto, ossia la probabilità di estrarre un numero che comporta l'avvenimento
    # dell'evento è di circa 2.28%, quindi ogni 100 iterazioni avvengono circa 2-3 eventi spia motore
    # TODO
    # Ridimensionare mean e devstd
    mean_engine_event = 0
    # devstd_engine_event = 1/2
    devstd_engine_event = 0.8
    # Set up evento spia motore
    engine_event = {
        "type": "enginelight",
        "license_plate": "",
        "info_autobus": {
            "gps": None,
            "num_psg": None
        },
        "happened": False,
        "created_at": None
    }
    # Predisposizione parametri mu e sigma della distribuzione gaussiana da cui estrarre il numero che comporterà
    # l'avvenimento o meno dell'evento panic button. I parametri sono stati scelti in modo che l'evento sia poco
    # probabile in un numero di esecuzioni alto, ossia la probabilità di estrarre un numero che comporta l'avvenimento
    # dell'evento è di circa 1.00%, quindi ogni 100 iterazioni avviene circa 1 evento panic button
    # TODO
    # Ridimensionare mean e devstd
    mean_panic_button_event = 0
    # devstd_panic_button_event = 0.43
    devstd_panic_button_event = 0.8
    # Set up evento panic button
    panic_button_event = {
        "type": "panicbutton", 
        "license_plate": "",
        "info_autobus": {
            "gps": None,
            "num_psg": None
        },
        "happened": False,
        "created_at": None
    }

    # Inizializzazione dizionario dedito al mantenimento delle targhe in esecuzione e delle relativa lista e indice
    # all'interno di questa corrispondenti
    license_plates_in_exec = {}

    # Installazione handler del segnale CTRL+C
    signal.signal(signalnum=signal.SIGINT, handler=shutdown_all_autobus)

    # Verifica validità numero autobus e indirizzo broker MQTT
    termic_num, hybrid_num, electric_num, host, port = check_cmd_line_args(termic_autobus_num=sys.argv[1], hybrid_autobus_num=sys.argv[2], electric_autobus_num=sys.argv[3], host=sys.argv[4], port=sys.argv[5])

    # ISTANZIAZIONE OGGETTI AUTOBUS
    # Utilizzo della lista globale di autobus smart con motorizzazione termica
    global termic_bus_list
    for i in range(0, termic_num):
        termic_bus_list.append(AutobusTermico(ranges=ranges, timeout=delay_mqtt, host=host, port=port))

        # Aggiunta al dizionario di targhe in esecuzione dell'oggetto:
        #   targa : {
        #       lista,
        #       indice
        #   }
        license_plates_in_exec.update(
            {
                termic_bus_list[i].get_LP() : {
                    "list": termic_bus_list,
                    "index": i
                }
            }
        )

    # Utilizzo della lista globale di autobus smart con motorizzazione ibrida
    global hybrid_bus_list
    for i in range(0, hybrid_num):
        hybrid_bus_list.append(AutobusIbrido(ranges=ranges, timeout=delay_mqtt, host=host, port=port))

        # Aggiunta al dizionario di targhe in esecuzione dell'oggetto:
        #   targa : {
        #       lista,
        #       indice
        #   }
        license_plates_in_exec.update(
            {
                hybrid_bus_list[i].get_LP() : {
                    "list": hybrid_bus_list,
                    "index": i
                }
            }
        )

    # Utilizzo della lista globale di autobus smart con motorizzazione elettrica
    global electric_bus_list
    for i in range(0, electric_num):
        electric_bus_list.append(AutobusElettrico(ranges=ranges, timeout=delay_mqtt, host=host, port=port))

        # Aggiunta al dizionario di targhe in esecuzione dell'oggetto:
        #   targa : {
        #       lista,
        #       indice
        #   }
        license_plates_in_exec.update(
            {
                electric_bus_list[i].get_LP() : {
                    "list": electric_bus_list,
                    "index": i
                }
            }
        )

    # Pick della targa da associare all'evento spia motore
    engine_event["license_plate"] = random.choice( list(license_plates_in_exec.keys()) )
    # Assegnamento dell'istante di creazione dell'evento
    engine_event["created_at"] = time.time()

    # Rimozione temporanea della targa appena estratta per evitare che i due eventi siano associati alla stessa targa
    list_license_plates_in_exec = list( license_plates_in_exec.keys() )
    list_license_plates_in_exec.remove(engine_event["license_plate"])

    # Pick della targa da associare all'evento panic button
    panic_button_event["license_plate"] = random.choice( list_license_plates_in_exec )
    # Assegnamento dell'istante di creazione dell'evento
    panic_button_event["created_at"] = time.time()

    print("Accensione motore...\n")
    time.sleep(delay_setup)

    # Ciclo azioni
    while True:
        # Rimozione dalla lista dei valori delle precedenti esecuzioni del Ciclo azioni
        update_fermata_bus_list.clear()

        # Aggiornamento corrispondente al numero attuale di esecuzioni del Ciclo azioni
        fermata_bus += 1

        # GESTIONE EVENTI

        # Pick dalla distribuzione gaussiana, con parametri opportuni, del numero che determina l'avvenimento degli
        # eventi o meno
        happened_engine_event = random.gauss(mu=mean_engine_event, sigma=devstd_engine_event)
        happened_panic_button_event = random.gauss(mu=mean_panic_button_event, sigma=devstd_panic_button_event)

        # Verifica del numero associato all'evento spia motore estratto, solamente se maggiore uno allora l'evento
        # avviene
        if happened_engine_event > 1:
            engine_event["happened"] = True

        # Verifica del numero associato all'evento panic button estratto, solamente se maggiore uno allora l'evento
        # avviene
        if happened_panic_button_event > 1:
            panic_button_event["happened"] = True

        # Verifica avvenimento evento spia motore
        if engine_event["happened"]:
            # Nel caso in cui l'evento avvenga alla prima iterazione, per evitare di ottenere dati nulli viene effettuata
            # una simulazione dei dati con 'first_exec' a True, in modo da avere sicuramente i dati di partenza
            if first_exec:
               license_plates_in_exec[engine_event["license_plate"]]["list"][license_plates_in_exec[engine_event["license_plate"]]["index"]].simulate(first_exec, fermata_bus)

            # Assegnazione degli ultimi dati di posizione e numero passeggeri dell'autobus di interesse
            engine_event["info_autobus"]["gps"] = license_plates_in_exec[engine_event["license_plate"]]["list"][license_plates_in_exec[engine_event["license_plate"]]["index"]].get_gps()
            engine_event["info_autobus"]["num_psg"] = license_plates_in_exec[engine_event["license_plate"]]["list"][license_plates_in_exec[engine_event["license_plate"]]["index"]].get_num_psg()

            # Chiamata al metodo di gestione dell'evento dell'autobus selezionato
            license_plates_in_exec[engine_event["license_plate"]]["list"][license_plates_in_exec[engine_event["license_plate"]]["index"]].handle_critic_events(event_msg=deepcopy(engine_event))

            # Predispozione liste di targhe per motorizzazione
            termic_lp_list = [autobus.get_LP() for autobus in termic_bus_list]
            hybrid_lp_list = [autobus.get_LP() for autobus in hybrid_bus_list]
            electric_lp_list = [autobus.get_LP() for autobus in electric_bus_list]

            # Rimozione dell'autobus e della targa associata dagli oggetti che ne contengono occorrenze
            license_plates_in_exec[engine_event["license_plate"]]["list"].pop(license_plates_in_exec[engine_event["license_plate"]]["index"])
            license_plates_in_exec.pop(engine_event["license_plate"])

            # Aggiornamento della rispettiva lista, di cui dovrà essere calato il numero di elementi e di nuovo
            # scorsa per aggiornare gli indici presenti nel dizionario di targhe in esecuzione
            if engine_event["license_plate"] in termic_lp_list:
                termic_num -= 1
                for i in range(0, termic_num):
                    license_plates_in_exec.update(
                        {
                            termic_bus_list[i].get_LP() : {
                                "list": termic_bus_list,
                                "index": i
                            }
                        }
                    )

                termic_lp_list.remove(engine_event["license_plate"])
            elif engine_event["license_plate"] in hybrid_lp_list:
                hybrid_num -= 1

                for i in range(0, hybrid_num):
                    license_plates_in_exec.update(
                        {
                            hybrid_bus_list[i].get_LP() : {
                                "list": hybrid_bus_list,
                                "index": i
                            }
                        }
                    )

                hybrid_lp_list.remove(engine_event["license_plate"])
            else:
                electric_num -= 1

                for i in range(0, electric_num):
                    license_plates_in_exec.update(
                        {
                            electric_bus_list[i].get_LP() : {
                                "list": electric_bus_list,
                                "index": i
                            }
                        }
                    )

                electric_lp_list.remove(engine_event["license_plate"])

            # Reimpostazione dell'avvenimento dell'evento a False, ossia 'non avvenuto'
            engine_event["happened"] = False
            # Reimpostazione dei dati di posizione e numero passeggeri
            engine_event["info_autobus"]["gps"] = None
            engine_event["info_autobus"]["num_psg"] = None

            # Pick nuova targa associata all'evento
            try:
                engine_event["license_plate"] = random.choice( list(license_plates_in_exec.keys()) )
            except IndexError:
                # Verifica presenza autobus, random.choice() su una lista vuota fallisce e genera una eccezione
                # IndexError, perché non può scegliere nessun elemento, per verificare che sia effettivamente questo
                # il caso viene verificata l'assenza di autobus in esecuzione, in quel caso la targa diventa la stringa
                # vuota come placeholder, dato che a seguito degli eventi avvenuti l'esecuzione sarà interrotta
                if termic_num == 0 and hybrid_num == 0 and electric_num == 0:
                    engine_event["license_plate"] = ""
            # Assegnamento nuovo istante di creazione dell'evento
            engine_event["created_at"] = time.time()

        # Verifica uguaglianza targhe eventi
        engine_event["license_plate"], panic_button_event["license_plate"] = events_license_plates_safe(lp_engine=engine_event["license_plate"], lp_pbutton=panic_button_event["license_plate"], time_engine=engine_event["created_at"], time_pbutton=panic_button_event["created_at"], lp_list=list( license_plates_in_exec.keys() ))

        # Verifica avvenimento evento panic button
        if panic_button_event["happened"]:
            # Nel caso in cui l'evento avvenga alla prima iterazione, per evitare di ottenere dati nulli viene effettuata
            # una simulazione dei dati con 'first_exec' a True, in modo da avere sicuramente i dati di partenza
            if first_exec:
                license_plates_in_exec[panic_button_event["license_plate"]]["list"][license_plates_in_exec[panic_button_event["license_plate"]]["index"]].simulate(first_exec, fermata_bus)

            # Assegnazione degli ultimi dati di posizione e numero passeggeri dell'autobus di interesse
            panic_button_event["info_autobus"]["gps"] = license_plates_in_exec[panic_button_event["license_plate"]]["list"][license_plates_in_exec[panic_button_event["license_plate"]]["index"]].get_gps()
            panic_button_event["info_autobus"]["num_psg"] = license_plates_in_exec[panic_button_event["license_plate"]]["list"][license_plates_in_exec[panic_button_event["license_plate"]]["index"]].get_num_psg()

            # Chiamata al metodo di gestione dell'evento dell'autobus selezionato
            license_plates_in_exec[panic_button_event["license_plate"]]["list"][license_plates_in_exec[panic_button_event["license_plate"]]["index"]].handle_critic_events(event_msg=deepcopy(panic_button_event))

            # Predispozione liste di targhe per motorizzazione
            termic_lp_list = [autobus.get_LP() for autobus in termic_bus_list]
            hybrid_lp_list = [autobus.get_LP() for autobus in hybrid_bus_list]
            electric_lp_list = [autobus.get_LP() for autobus in electric_bus_list]

            # Rimozione dell'autobus e della targa associata dagli oggetti che ne contengono occorrenze
            license_plates_in_exec[panic_button_event["license_plate"]]["list"].pop(license_plates_in_exec[panic_button_event["license_plate"]]["index"])
            license_plates_in_exec.pop(panic_button_event["license_plate"])

            # Aggiornamento della rispettiva lista, di cui dovrà essere calato il numero di elementi e di nuovo
            # scorsa per aggiornare gli indici presenti nel dizionario di targhe in esecuzione
            if panic_button_event["license_plate"] in termic_lp_list:
                termic_num -= 1
                for i in range(0, termic_num):
                    license_plates_in_exec.update(
                        {
                            termic_bus_list[i].get_LP() : {
                                "list": termic_bus_list,
                                "index": i
                            }
                        }
                    )

                termic_lp_list.remove(panic_button_event["license_plate"])
            elif panic_button_event["license_plate"] in hybrid_lp_list:
                hybrid_num -= 1

                for i in range(0, hybrid_num):
                    license_plates_in_exec.update(
                        {
                            hybrid_bus_list[i].get_LP() : {
                                "list": hybrid_bus_list,
                                "index": i
                            }
                        }
                    )

                hybrid_lp_list.remove(panic_button_event["license_plate"])
            else:
                electric_num -= 1

                for i in range(0, electric_num):
                    license_plates_in_exec.update(
                        {
                            electric_bus_list[i].get_LP() : {
                                "list": electric_bus_list,
                                "index": i
                            }
                        }
                    )

                electric_lp_list.remove(panic_button_event["license_plate"])

            # Reimpostazione dell'avvenimento dell'evento a False, ossia 'non avvenuto'
            panic_button_event["happened"] = False
            # Reimpostazione dei dati di posizione e numero passeggeri
            panic_button_event["info_autobus"]["gps"] = None
            panic_button_event["info_autobus"]["num_psg"] = None

            # Pick nuova targa associata all'evento
            try:
                panic_button_event["license_plate"] = random.choice( list(license_plates_in_exec.keys()) )
            except IndexError:
                # Verifica presenza autobus, random.choice() su una lista vuota fallisce e genera una eccezione
                # IndexError, perché non può scegliere nessun elemento, per verificare che sia effettivamente questo
                # il caso viene verificata l'assenza di autobus in esecuzione, in quel caso la targa diventa la stringa
                # vuota come placeholder, dato che a seguito degli eventi avvenuti l'esecuzione sarà interrotta
                if termic_num == 0 and hybrid_num == 0 and electric_num == 0:
                    panic_button_event["license_plate"] = ""
            # Assegnamento nuovo istante di creazione dell'evento
            panic_button_event["created_at"] = time.time()

        # Verifica uguaglianza targhe eventi
        engine_event["license_plate"], panic_button_event["license_plate"] = events_license_plates_safe(lp_engine=engine_event["license_plate"], lp_pbutton=panic_button_event["license_plate"], time_engine=engine_event["created_at"], time_pbutton=panic_button_event["created_at"], lp_list=list( license_plates_in_exec.keys() ))

        # Verifica assenza autobus in esecuzione, in questo caso l'esecuzione viene interrotta
        if termic_num == 0 and hybrid_num == 0 and electric_num == 0:
            print(f"Esecuzione interrotta a causa di insufficienza di autobus")
            print("Spegnimento motore...")
            sys.exit(0)

        # SIMULAZIONE METRICHE
        # Termic
        for i in range(0, termic_num):
            update_fermata_bus_list.append( termic_bus_list[i].simulate(first_exec, fermata_bus) )
        # Hybrid
        for i in range(0, hybrid_num):
            update_fermata_bus_list.append( hybrid_bus_list[i].simulate(first_exec, fermata_bus) )
        # Electric
        for i in range(0, electric_num):
            update_fermata_bus_list.append( electric_bus_list[i].simulate(first_exec, fermata_bus) )

        # PREPARAZIONE "PACCHETTO" DATI
        # Termic
        for i in range(0, termic_num):
            termic_bus_list[i].format_data(data_type=format)
        # Hybrid
        for i in range(0, hybrid_num):
            hybrid_bus_list[i].format_data(data_type=format)
        # Electric
        for i in range(0, electric_num):
            electric_bus_list[i].format_data(data_type=format)

        # INVIO "PACCHETTO" DATI
        # Termic
        for i in range(0, termic_num):
            termic_bus_list[i].communicate()
        # Hybrid
        for i in range(0, hybrid_num):
            hybrid_bus_list[i].communicate()
        # Electric
        for i in range(0, electric_num):
            electric_bus_list[i].communicate()

        # STAMPA A VIDEO REPORT METRICHE
        # Termic
        for i in range(0, termic_num):
            termic_bus_list[i].show()
        # Hybrid
        for i in range(0, hybrid_num):
            hybrid_bus_list[i].show()
        # Electric
        for i in range(0, electric_num):
            electric_bus_list[i].show()

        # Inversione flag a segnalare che la prima esecuzione del Ciclo azioni è terminata 
        first_exec = False 

        # Verifica uguaglianza di tutti i contatori di fermate dei bus restituiti dalle funzioni simulate()
        for i in range(0, len(update_fermata_bus_list)-1):
            for j in range(i+1, len(update_fermata_bus_list)):
                if update_fermata_bus_list[i] != update_fermata_bus_list[j]:
                    sys.stderr.write("Errore! Uno dei contatori delle fermate bus è diverso dagli altri\n")
                    sys.exit(-26)
        update_fermata_bus = update_fermata_bus_list[0]

        # Aggiornamento al valore restituito dalle funzioni di simulazione metriche
        fermata_bus = update_fermata_bus

        time.sleep(delay_metrics)


if __name__ == "__main__":
    main()
