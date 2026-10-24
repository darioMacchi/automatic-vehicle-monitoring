import random
import signal
import sys
import time
from copy import deepcopy
from enum import IntEnum

from avm.autobus_elettrico import AutobusElettrico
from avm.autobus_ibrido import AutobusIbrido
from avm.autobus_termico import AutobusTermico


# Numero massimo di autobus smart con motorizzazione termica
TERMIC_MAX_NUM = len(AutobusTermico.pool_termic_license_plates)
# Numero massimo di autobus smart con motorizzazione ibrida
HYBRID_MAX_NUM = len(AutobusIbrido.pool_hybrid_license_plates)
# Numero massimo di autobus smart con motorizzazione elettrica
ELECTRIC_MAX_NUM = len(AutobusElettrico.pool_electric_license_plates)


# Lista di autobus smart con motorizzazione termica
termic_bus_list = []
# Lista di autobus smart con motorizzazione ibrida
hybrid_bus_list = []
# Lista di autobus smart con motorizzazione elettrica
electric_bus_list = []


# Lista di formato dati possibili per la comunicazione dati
format_list = ["JSON", "BINARY"]


# Tipo di dati enumerativo volto alla definizione dei tipi di motorizzazione
class Motore(IntEnum):
    TERMICO = 1
    IBRIDO = 2
    ELETTRICO = 3


# Stop buses - metodo necessario per evitare duplicazione dello scorrimento delle diverse liste di autobus, una per ogni tipo
# di motorizzazione, al fine di eseguire una 'graceful disconnection'. Permette di scorrere una lista e invocare sugli
# elementi che ne fanno parte il metodo stop_autobus
def stop_buses(buses_list: list):
    if type(buses_list) is not list:
        raise TypeError(f"Errore! Il tipo del parametro 'buses_list' passato deve essere 'list'. Ricevuto {type(buses_list)}")

    for bus in buses_list:
        bus.stop_autobus()


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
    stop_buses(buses_list=termic_bus_list)
    stop_buses(buses_list=hybrid_bus_list)
    stop_buses(buses_list=electric_bus_list)

    print(f"Esecuzione interrotta dal segnale {sig_name}")
    print("Spegnimento motore...")
    sys.exit(0)


# Calcolo numero massimo di autobus - funzione necessaria per calcolare il numero massimo di autobus a seconda del tipo di
# motorizzazione. Il calcolo avviene sulla base delle macro impostate per tipologia di alimentazione, inoltre è prevista
# l'impostazione di una stringa che contiene il parametro associato al numero massimo di autobus smart
def calculate_max_buses(engine: Motore):
    if type(engine) is not Motore:
        raise TypeError(f"Errore! Il tipo del parametro 'engine' passato deve essere 'Motore'. Ricevuto {type(engine)}")

    engine_string = ""
    max_number = 0

    match engine:
        case Motore.TERMICO:
            engine_string = "$num_autobus_termici"
            max_number = TERMIC_MAX_NUM
        case Motore.IBRIDO:
            engine_string = "$num_autobus_ibridi"
            max_number = HYBRID_MAX_NUM
        case Motore.ELETTRICO:
            engine_string = "$num_autobus_elettrici"
            max_number = ELECTRIC_MAX_NUM

    return engine_string, max_number


# Check dimensione lista - funzione utile alla verifica della coerenza della dimensione di una lista, operata attraverso
# il calcolo dinamico della lunghezza della lista
def check_list_dimension(list_to_check: list, dimension: int):
    if type(list_to_check) is not list:
        raise TypeError(f"Errore! Il tipo del parametro 'list_to_check' passato deve essere 'list'. Ricevuto {type(list_to_check)}")

    if type(dimension) is not int:
        raise TypeError(f"Errore! Il tipo del parametro 'dimension' passato deve essere 'int'. Ricevuto {type(dimension)}")

    flag = True
    if len(list_to_check) != dimension:
        flag = False

    return flag


# Check numero autobus - funzione utile alla verifica del numero di autobus passato da linea di comando e non solo. La 
# verifica prevede il controllo del tipo del numero, la non negatività, e il non superamento del numero massimo di autobus
# smart disponibili
def check_bus_number(engine: Motore, bus_number: int, input_check: bool):
    if type(engine) is not Motore:
        raise TypeError(f"Errore! Il tipo del parametro 'engine' passato deve essere 'Motore'. Ricevuto {type(engine)}")
    
    if type(input_check) is not bool:
        raise TypeError(f"Errore! Il tipo del parametro 'input_check' passato deve essere 'bool'. Ricevuto {type(input_check)}")

    bus_number_checked = 0
    engine_string, max_number = calculate_max_buses(engine=engine)

    # Check numero valido
    try:
        bus_number_checked = int(bus_number)
    except ValueError:
        to_str = "non è un numero valido"
        if input_check:
            sys.stderr.write(f"Errore! L'argomento {engine_string} passato da linea di comando {to_str}\n")
            sys.exit(-2)
        else:
            raise ValueError(f"Errore! Il numero di autobus fornito {to_str}")

    # Check numero non negativo
    if bus_number_checked < 0:
        to_str = "deve essere un numero maggiore o uguale a zero"
        if input_check:
            sys.stderr.write(f"Errore! L'argomento {engine_string} {to_str}\n")
            sys.exit(-3)
        else:
            raise ValueError(f"Errore! Il numero di autobus fornito {to_str}")

    # Check numero non superiore al numero di targhe disponibili
    if bus_number_checked > max_number:
        to_str = f"deve essere un numero non superiore a {max_number}"
        if input_check:
            sys.stderr.write(f"Errore! L'argomento {engine_string} {to_str}\n")
            sys.exit(-4)
        else:
            raise ValueError(f"Errore! Il numero di autobus fornito {to_str}")

    return bus_number_checked


# Check host MQTT - funzione utile alla verifica della coerenza dell'host MQTT passato da linea di comando e non solo. La 
# verifica prevede il controllo della presenza di una stringa non vuota
def check_mqtt_host(host: str, input_check: bool):
    if type(host) is not str:
        raise TypeError(f"Errore! Il tipo del parametro 'host' passato deve essere 'str'. Ricevuto {type(host)}")
    
    if type(input_check) is not bool:
        raise TypeError(f"Errore! Il tipo del parametro 'input_check' passato deve essere 'bool'. Ricevuto {type(input_check)}")

    # Check stringa non vuota
    if host == "":
        to_str = "deve essere un indirizzo non nullo"
        if input_check:
            sys.stderr.write(f"Errore! L'argomento $host {to_str}\n")
            sys.exit(-5)
        else:
            raise ValueError(f"Errore! L'host fornito {to_str}")


# Check porta MQTT - funzione utile alla verifica della coerenza della porta MQTT passata da linea di comando e non solo. La 
# verifica prevede il controllo del tipo del numero di porta, e che il numero di porta rappresenti effettivamente una delle
# porte utilizzate dal protocollo MQTT utilizzato
def check_mqtt_port(port: int, input_check: bool):
    if type(input_check) is not bool:
        raise TypeError(f"Errore! Il tipo del parametro 'input_check' passato deve essere 'bool'. Ricevuto {type(input_check)}")

    mqtt_port = None

    # Check numero valido
    try:
        mqtt_port = int(port)
    except ValueError:
        to_str = "non è un numero valido"
        if input_check:
            sys.stderr.write(f"Errore! L'argomento $porta passato da linea di comando {to_str}\n")
            sys.exit(-6)
        else:
            raise ValueError(f"Errore! Il numero di porta fornito {to_str}")

    # Check porta
    if mqtt_port != 1883 and mqtt_port != 8883:
        to_str = "deve essere una porta MQTT valida: 1883 oppure 8883 (connessioni SSL)"
        if input_check:
            sys.stderr.write(f"Errore! L'argomento $porta {to_str}\n")
            sys.exit(-7)
        else:
            raise ValueError(f"Errore! Il numero di porta fornito {to_str}")

    return mqtt_port


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
    input_check = True

    # Termic check
    termic_num = check_bus_number(engine=Motore.TERMICO, bus_number=termic_autobus_num, input_check=input_check)

    # Hybrid check
    hybrid_num = check_bus_number(engine=Motore.IBRIDO, bus_number=hybrid_autobus_num, input_check=input_check)

    # Electric check
    electric_num = check_bus_number(engine=Motore.ELETTRICO, bus_number=electric_autobus_num, input_check=input_check)

    # Host check
    host_mqtt = host
    check_mqtt_host(host=host_mqtt, input_check=input_check)

    # Port check
    port_mqtt = check_mqtt_port(port=port, input_check=input_check)

    return termic_num, hybrid_num, electric_num, host_mqtt, port_mqtt


# Inizializzazione autobus - funzione necessaria all'inizializzazione degli autobus, opera verifiche sui parametri forniti
# al fine di assicurare la presenza delle chiavi di dizionario necessarie, e che timeout, host e porta MQTT siano validi
def initialize_buses(bus_number: int, buses_list: list, license_plates_running: dict, metric_ranges: dict, mqtt_timeout: float, mqtt_host: str, mqtt_port: int, engine: Motore):
    if type(bus_number) is not int:
        raise TypeError(f"Errore! Il tipo del parametro 'bus_number' passato deve essere 'int'. Ricevuto {type(bus_number)}")
    
    if type(license_plates_running) is not dict:
        raise TypeError(f"Errore! Il tipo del parametro 'license_plates_running' passato deve essere 'dict'. Ricevuto {type(license_plates_running)}")

    if type(buses_list) is not list:
        raise TypeError(f"Errore! Il tipo del parametro 'buses_list' passato deve essere 'list'. Ricevuto {type(buses_list)}")

    if type(metric_ranges) is not dict:
        raise TypeError(f"Errore! Il tipo del parametro 'metric_ranges' passato deve essere 'dict'. Ricevuto {type(metric_ranges)}")

    if type(mqtt_timeout) is not float:
        raise TypeError(f"Errore! Il tipo del parametro 'mqtt_timeout' passato deve essere 'float'. Ricevuto {type(mqtt_timeout)}")

    if type(mqtt_host) is not str:
        raise TypeError(f"Errore! Il tipo del parametro 'mqtt_host' passato deve essere 'str'. Ricevuto {type(mqtt_host)}")

    if type(mqtt_port) is not int:
        raise TypeError(f"Errore! Il tipo del parametro 'mqtt_port' passato deve essere 'int'. Ricevuto {type(mqtt_port)}")

    if type(engine) is not Motore:
        raise TypeError(f"Errore! Il tipo del parametro 'engine' passato deve essere 'Motore'. Ricevuto {type(engine)}")

    # Verifica numero di autobus smart
    bus_number = check_bus_number(engine=engine, bus_number=bus_number, input_check=False)

    # Impostazione dizionari chiavi necessarie al corretto funzionamento degli autobus smart
    required_superficial_keys = {
        "gps",
        "speed_low", "speed_up",
        "tyre_pressure_low", "tyre_pressure_up",
        "brake_status",
        "engine_status",
        "num_psg_low", "num_psg_up",
        "environmental",
        "battery_lvl_low", "battery_lvl_up",
        "battery_temp_low", "battery_temp_up",
        "termic_fuel_lvl_low", "termic_fuel_lvl_up",
        "termic_fuel_cons_low", "termic_fuel_cons_up",
        "hybrid_fuel_lvl_low", "hybrid_fuel_lvl_up",
        "hybrid_fuel_cons_low", "hybrid_fuel_cons_up"
    }
    required_gps_keys = {
        "latitude_low", "latitude_up",
        "longitude_low", "longitude_up"
    }
    required_environmental_keys = {
        "temp_low", "temp_up",
        "hum_low", "hum_up"
    }

    # Verifica presenza del set completo di chiavi all'interno del dizionario contenente i range delle metriche prodotte
    # dagli autobus smart
    if ( not required_superficial_keys.issubset(metric_ranges) or not required_gps_keys.issubset(metric_ranges["gps"]) or not required_environmental_keys.issubset(metric_ranges["environmental"]) ):
        raise KeyError("Errore! Il dizionario contenente i range delle metriche fornito non possiede tutte le chiavi necessarie")

    # Verifica timeout MQTT non negativo
    if mqtt_timeout < 0:
        raise ValueError("Errore! Il timeout MQTT fornito deve essere un numero maggiore o uguale a zero")

    # Verifica host MQTT
    check_mqtt_host(host=mqtt_host, input_check=False)

    # Verifica porta MQTT
    mqtt_port = check_mqtt_port(port=mqtt_port, input_check=False)

    # Inizializzazione autobus sulla base della tipologia di motorizzazione fornita
    for i in range(0, bus_number):
        try:
            match engine:
                case Motore.TERMICO:
                    buses_list.append(AutobusTermico(ranges=metric_ranges, timeout=mqtt_timeout, host=mqtt_host, port=mqtt_port))
                case Motore.IBRIDO:
                    buses_list.append(AutobusIbrido(ranges=metric_ranges, timeout=mqtt_timeout, host=mqtt_host, port=mqtt_port))
                case Motore.ELETTRICO:
                    buses_list.append(AutobusElettrico(ranges=metric_ranges, timeout=mqtt_timeout, host=mqtt_host, port=mqtt_port))
        except ValueError as e:
            sys.stderr.write(f"{str(e)}\n")
            sys.exit(-12)

        # Aggiunta al dizionario di targhe in esecuzione dell'oggetto:
        #   targa : {
        #       lista,
        #       indice,
        #       tipo_motore
        #   }
        license_plates_running.update(
            {
                buses_list[i].get_LP() : {
                    "list": buses_list,
                    "index": i,
                    "engine_type": engine
                }
            }
        )


# Scelta nuova targa - funzione necessaria alla scelta di una nuova targa da assegnare algli eventi riguardanti gli 
# autobus smart, nella circostanza in cui due eventi riguardino uno stesso autobus. Cerca di rimuovere dalla lista contenente
# tutte le targhe in esecuzione in un determinato istante la targa che riguarda l'evento creato da più tempo, e dalla nuova
# lista viene estratta una nuova targa. Nel caso in cui la lista sia vuota a seguito della rimozione significa che non sono
# presenti due o più targhe, ma una solamente, in quel caso gli eventi riguardano obbligatoriamente lo stesso autobus
def pick_new_license_plate(license_plates_list: list, older_event_license_plate: str):
    if type(license_plates_list) is not list:
        raise TypeError(f"Errore! Il tipo del parametro 'license_plates_list' passato deve essere 'list'. Ricevuto {type(license_plates_list)}")
    
    if type(older_event_license_plate) is not str:
        raise TypeError(f"Errore! Il tipo del parametro 'older_event_license_plate' passato deve essere 'str'. Ricevuto {type(older_event_license_plate)}")

    new_license_plate = ""

    try:
        # Rimozione targa evento più "vecchio"
        license_plates_list.remove(older_event_license_plate)
        # Pick della nuova targa dell'evento più "giovane" nella lista modificata
        new_license_plate = random.choice(license_plates_list)
    except IndexError:
        # Verifica lista vuota, random.choice() su una lista vuota fallisce e genera una eccezione IndexError,
        # perché non può scegliere nessun elemento, per verificare che sia effettivamente questo il caso viene
        # verificata l'assenza di elementi dalla lista, in quel caso la targa dei due eventi deve essere per
        # forza la stessa
        if len(license_plates_list) == 0:
            new_license_plate = older_event_license_plate
    except ValueError as e:
        sys.stderr.write(f"{str(e)}\n")
        sys.exit(-13)
    finally:
        return new_license_plate


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
            lp_pbutton = pick_new_license_plate(license_plates_list=lp_list, older_event_license_plate=lp_engine)
        else:
            lp_engine = pick_new_license_plate(license_plates_list=lp_list, older_event_license_plate=lp_pbutton)

    return lp_engine, lp_pbutton


# Gestione evento - funzione dedita alla gestione di ogni evento nel momento in cui accade, si occupa di verificare la
# coerenza dei parametri forniti, dato la stretta connessione tra i parametri 'license_plates_running', 'event_list',
# 'event_index', 'engine'. Successivamente simula un'esecuzione dell'autobus smart nel momento in cui l'evento avvenga
# nella prima esecuzione dell'emulazione della marcia dell'autobus, per evitare di avere un evento nel sistema registrato
# con metriche inizializzate a 0. Dopo di ché si occupa di invocare il metodo di gestione evento dell'autobus, predisposto
# appositamente, e aggiornare le liste e i dizionari che contengono l'autobus su cui è avvenuto l'evento, a seguito del fermo
# alla marcia del bus
def handle_event(event: dict, license_plates_running: dict, simulate: bool, stop_frequency: int, num_buses: int, event_list: list, event_index: int, engine: Motore):
    if type(event) is not dict:
        raise TypeError(f"Errore! Il tipo del parametro 'event' passato deve essere 'dict'. Ricevuto {type(event)}")

    if type(license_plates_running) is not dict:
        raise TypeError(f"Errore! Il tipo del parametro 'license_plates_running' passato deve essere 'dict'. Ricevuto {type(license_plates_running)}")

    if type(simulate) is not bool:
        raise TypeError(f"Errore! Il tipo del parametro 'simulate' passato deve essere 'bool'. Ricevuto {type(simulate)}")

    if type(stop_frequency) is not int:
        raise TypeError(f"Errore! Il tipo del parametro 'stop_frequency' passato deve essere 'int'. Ricevuto {type(stop_frequency)}")

    if type(num_buses) is not int:
        raise TypeError(f"Errore! Il tipo del parametro 'num_buses' passato deve essere 'int'. Ricevuto {type(num_buses)}")

    if type(event_list) is not list:
        raise TypeError(f"Errore! Il tipo del parametro 'event_list' passato deve essere 'list'. Ricevuto {type(event_list)}")

    if type(event_index) is not int:
        raise TypeError(f"Errore! Il tipo del parametro 'event_index' passato deve essere 'int'. Ricevuto {type(event_index)}")

    if type(engine) is not Motore:
        raise TypeError(f"Errore! Il tipo del parametro 'engine' passato deve essere 'Motore'. Ricevuto {type(engine)}")

    # Verifica frequenza fermata fornita non negativa
    if stop_frequency < 0:
        raise ValueError("Errore! La frequenza di fermate fornita deve essere un numero maggiore o uguale a zero")

    # Verifica numero di autobus smart
    num_buses = check_bus_number(engine=engine, bus_number=num_buses, input_check=False)

    # Verifica presenza chiave 'license_plate' all'interno dell'evento
    if "license_plate" not in event:
        raise KeyError("L'evento fornito deve contenere la chiave 'license_plate'")

    # Verifica presenza della targa riportata nell'evento accaduto all'interno del dizionario di targhe in esecuzione
    if event["license_plate"] not in license_plates_running:
        raise ValueError("Errore! L'evento fornito contiene una targa che identifica un autobus assente dal pool di autobus in esecuzione")

    # Verifica coerenza parametro 'engine' fornito rispetto alla tipologia di motorizzazione presente all'interno del 
    # dizionario di targhe in esecuzione relativamente alla targa appropriata
    if engine != license_plates_running[event["license_plate"]]["engine_type"]:
        raise ValueError("Errore! La tipologia di motorizzazione fornita non corrisponde a quella dell'autobus coinvolto nell'evento")

    # Verifica coerenza parametro 'event_list' fornito rispetto alla lista presente all'interno del dizionario di targhe
    # in esecuzione, in particolare attraverso il predicato 'is' la verifica è operata rispetto alla uguaglianza dell'oggetto
    # stesso, e non del solo contenuto
    if event_list is not license_plates_running[event["license_plate"]]["list"]:
        raise ValueError("Errore! La lista contenente l'autobus fornita non corrisponde a quella che contiene l'autobus coinvolto nell'evento")

    # Verifica coerenza parametro 'event_index' fornito rispetto all'indice presente all'interno del dizionario di targhe
    # in esecuzione, inoltre verifica operata anche in merito al valore positivo dell'indice o al più 0, e alla dimensione
    # che non deve superare la lunghezza della lista, per evitare 'index out of bound'
    if event_index != license_plates_running[event["license_plate"]]["index"] or event_index < 0 or event_index >= len(event_list):
        raise ValueError("Errore! L'indice fornito non corrisponde a quello che identifica nell'appropriata lista l'autobus coinvolto nell'evento")

    # Verifica coerenza tra la targa ottenuta attraverso il metodo 'get_LP' su lista e indice forniti in ingresso, e la targa
    # riportata dall'evento accaduto
    if event_list[event_index].get_LP() != event["license_plate"]:
        raise ValueError("Errore! La targa dell'autobus identificato attraverso 'event_list' e 'event_index' non corrisponde alla targa contenuta nell'evento")

    # Verifica coerenza dimensioni lista riportate dal parametro 'num_buses'
    if not check_list_dimension(list_to_check=event_list, dimension=num_buses):
        raise ValueError("Errore! Il numero di autobus fornito differisce dal numero di bus contenuto nella lista di autobus associata all'evento")

    # Nel caso in cui l'evento avvenga alla prima iterazione, per evitare di ottenere dati nulli viene effettuata
    # una simulazione dei dati con 'first_exec' a True, in modo da avere sicuramente i dati di partenza
    if simulate:
        event_list[event_index].simulate(simulate, stop_frequency)

    # Assegnazione degli ultimi dati di posizione e numero passeggeri dell'autobus di interesse
    event["info_autobus"]["gps"] = event_list[event_index].get_gps()
    event["info_autobus"]["num_psg"] = event_list[event_index].get_num_psg()

    # Chiamata al metodo di gestione dell'evento dell'autobus selezionato
    event_list[event_index].handle_critic_events(event_msg=deepcopy(event))

    # Rimozione dell'autobus e della targa associata dagli oggetti che ne contengono occorrenze
    event_list.pop(event_index)

    # Aggiornamento della rispettiva lista, di cui dovrà essere calato il numero di elementi e di nuovo
    # scorsa per aggiornare gli indici presenti nel dizionario di targhe in esecuzione
    num_buses -= 1
    for i in range(0, num_buses):
        license_plates_running.update(
            {
                event_list[i].get_LP() : {
                    "list": event_list,
                    "index": i,
                    "engine_type": engine
                }
            }
        )

    license_plates_running.pop(event["license_plate"])

    # Reimpostazione dell'avvenimento dell'evento a False, ossia 'non avvenuto'
    event["happened"] = False
    # Reimpostazione dei dati di posizione e numero passeggeri
    event["info_autobus"]["gps"] = None
    event["info_autobus"]["num_psg"] = None

    # Pick nuova targa associata all'evento
    try:
        event["license_plate"] = random.choice( list(license_plates_running.keys()) )
    except IndexError:
        # Verifica presenza autobus, random.choice() su una lista vuota fallisce e genera una eccezione
        # IndexError, perché non può scegliere nessun elemento, per verificare che sia effettivamente questo
        # il caso viene verificata l'assenza di autobus in esecuzione, in quel caso la targa diventa la stringa
        # vuota come placeholder, dato che a seguito degli eventi avvenuti l'esecuzione sarà interrotta
        if len( list(license_plates_running.keys()) ) == 0:
            event["license_plate"] = ""
    # Assegnamento nuovo istante di creazione dell'evento
    event["created_at"] = time.time()

    return num_buses


# Simulazione metriche - funzione dedicata all'emulazione della funzionalità di produzione delle metriche da parte
# dell'autobus smart, vengono operate verifiche sui parametri forniti, al fine di assicurare la corretta esecuzione
def simulate_metrics(bus_number: int, buses_list: list, update_stop_frequency_list: list, init: bool, stop_frequency: int, engine: Motore):
    if type(bus_number) is not int:
        raise TypeError(f"Errore! Il tipo del parametro 'bus_number' passato deve essere 'int'. Ricevuto {type(bus_number)}")

    if type(buses_list) is not list:
        raise TypeError(f"Errore! Il tipo del parametro 'buses_list' passato deve essere 'list'. Ricevuto {type(buses_list)}")

    if type(update_stop_frequency_list) is not list:
        raise TypeError(f"Errore! Il tipo del parametro 'update_stop_frequency_list' passato deve essere 'list'. Ricevuto {type(update_stop_frequency_list)}")

    if type(init) is not bool:
        raise TypeError(f"Errore! Il tipo del parametro 'init' passato deve essere 'bool'. Ricevuto {type(init)}")

    if type(stop_frequency) is not int:
        raise TypeError(f"Errore! Il tipo del parametro 'stop_frequency' passato deve essere 'int'. Ricevuto {type(stop_frequency)}")

    if type(engine) is not Motore:
        raise TypeError(f"Errore! Il tipo del parametro 'engine' passato deve essere 'Motore'. Ricevuto {type(engine)}")

    # Verifica numero di autobus smart
    bus_number = check_bus_number(engine=engine, bus_number=bus_number, input_check=False)

    # Verifica frequenza fermata fornita non negativa
    if stop_frequency < 0:
        raise ValueError("Errore! La frequenza di fermate fornita deve essere un numero maggiore o uguale a zero")

    # Verifica coerenza dimensioni lista riportate dal parametro 'bus_number'
    if not check_list_dimension(list_to_check=buses_list, dimension=bus_number):
        raise ValueError("Errore! Il numero di autobus fornito differisce dal numero di bus contenuto nella lista fornita")

    for i in range(0, bus_number):
        update_stop_frequency_list.append( buses_list[i].simulate(init, stop_frequency) )


# Formattazione metriche - funzione dedicata all'emulazione della funzionalità di formattazione delle metriche da parte
# dell'autobus smart, vengono operate verifiche sui parametri forniti, al fine di assicurare la corretta esecuzione
def format_data_packet(bus_number: int, buses_list: list, format_type: str, engine: Motore):
    if type(bus_number) is not int:
        raise TypeError(f"Errore! Il tipo del parametro 'bus_number' passato deve essere 'int'. Ricevuto {type(bus_number)}")

    if type(buses_list) is not list:
        raise TypeError(f"Errore! Il tipo del parametro 'buses_list' passato deve essere 'list'. Ricevuto {type(buses_list)}")

    if type(format_type) is not str:
        raise TypeError(f"Errore! Il tipo del parametro 'format_type' passato deve essere 'str'. Ricevuto {type(format_type)}")
    
    if type(engine) is not Motore:
        raise TypeError(f"Errore! Il tipo del parametro 'engine' passato deve essere 'Motore'. Ricevuto {type(engine)}")

    global format_list

    # Verifica numero di autobus smart
    bus_number = check_bus_number(engine=engine, bus_number=bus_number, input_check=False)

    # Verifica coerenza dimensioni lista riportate dal parametro 'bus_number'
    if not check_list_dimension(list_to_check=buses_list, dimension=bus_number):
        raise ValueError("Errore! Il numero di autobus fornito differisce dal numero di bus contenuto nella lista fornita")

    # Verifica del parametro di formattazione dati fornito sia presente nella lista dei formati disponibili
    format_type_upper = format_type.upper()
    if format_type_upper not in format_list:
        raise ValueError("Errore! Il tipo di formato dati fornito non è uno dei seguenti: 'JSON' | 'BINARY'")

    for i in range(0, bus_number):
        buses_list[i].format_data(data_type=format_type)


# Comunicazione metriche - funzione dedicata all'emulazione della funzionalità di comunicazione delle metriche da parte
# dell'autobus smart, vengono operate verifiche sui parametri forniti, al fine di assicurare la corretta esecuzione
def communicate_data(bus_number: int, buses_list: list, engine: Motore):
    if type(bus_number) is not int:
        raise TypeError(f"Errore! Il tipo del parametro 'bus_number' passato deve essere 'int'. Ricevuto {type(bus_number)}")
    
    if type(buses_list) is not list:
        raise TypeError(f"Errore! Il tipo del parametro 'buses_list' passato deve essere 'list'. Ricevuto {type(buses_list)}")

    if type(engine) is not Motore:
        raise TypeError(f"Errore! Il tipo del parametro 'engine' passato deve essere 'Motore'. Ricevuto {type(engine)}")

    # Verifica numero di autobus smart
    bus_number = check_bus_number(engine=engine, bus_number=bus_number, input_check=False)

    # Verifica coerenza dimensioni lista riportate dal parametro 'bus_number'
    if not check_list_dimension(list_to_check=buses_list, dimension=bus_number):
        raise ValueError("Errore! Il numero di autobus fornito differisce dal numero di bus contenuto nella lista fornita")

    for i in range(0, bus_number):
        buses_list[i].communicate()


# Visualizzazione metriche - funzione dedicata alla funzionalità di stampa a video delle metriche da parte, vengono operate 
# verifiche sui parametri forniti, al fine di assicurare la corretta esecuzione
def show_data(bus_number: int, buses_list: list, engine: Motore):
    if type(bus_number) is not int:
        raise TypeError(f"Errore! Il tipo del parametro 'bus_number' passato deve essere 'int'. Ricevuto {type(bus_number)}")
        
    if type(buses_list) is not list:
        raise TypeError(f"Errore! Il tipo del parametro 'buses_list' passato deve essere 'list'. Ricevuto {type(buses_list)}")

    if type(engine) is not Motore:
        raise TypeError(f"Errore! Il tipo del parametro 'engine' passato deve essere 'Motore'. Ricevuto {type(engine)}")

    # Verifica numero di autobus smart
    bus_number = check_bus_number(engine=engine, bus_number=bus_number, input_check=False)

    # Verifica coerenza dimensioni lista riportate dal parametro 'bus_number'
    if not check_list_dimension(list_to_check=buses_list, dimension=bus_number):
        raise ValueError("Errore! Il numero di autobus fornito differisce dal numero di bus contenuto nella lista fornita")

    for i in range(0, bus_number):
        buses_list[i].show()


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

    # Setup timeout attesa pubblicazione ogni messaggio broker MQTT
    delay_mqtt = 2.00
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
    mean_engine_event = 0
    devstd_engine_event = 1/2
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
    mean_panic_button_event = 0
    devstd_panic_button_event = 0.43
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

    # Verifica assenza autobus in esecuzione, in questo caso l'esecuzione viene interrotta
    if termic_num == 0 and hybrid_num == 0 and electric_num == 0:
        print(f"Esecuzione interrotta a causa di insufficienza di autobus")
        print("Spegnimento motore...")
        sys.exit(0)

    # ISTANZIAZIONE OGGETTI AUTOBUS
    # Utilizzo della lista globale di autobus smart con motorizzazione termica, ibrida, elettrica
    global termic_bus_list
    global hybrid_bus_list
    global electric_bus_list

    # Inizializzazione autobus smart
    initialize_buses(bus_number=termic_num, buses_list=termic_bus_list, license_plates_running=license_plates_in_exec, metric_ranges=ranges, mqtt_timeout=delay_mqtt, mqtt_host=host, mqtt_port=port, engine=Motore.TERMICO)
    initialize_buses(bus_number=hybrid_num, buses_list=hybrid_bus_list, license_plates_running=license_plates_in_exec, metric_ranges=ranges, mqtt_timeout=delay_mqtt, mqtt_host=host, mqtt_port=port, engine=Motore.IBRIDO)
    initialize_buses(bus_number=electric_num, buses_list=electric_bus_list, license_plates_running=license_plates_in_exec, metric_ranges=ranges, mqtt_timeout=delay_mqtt, mqtt_host=host, mqtt_port=port, engine=Motore.ELETTRICO)

    # Pick della targa da associare all'evento spia motore
    engine_event["license_plate"] = random.choice( list(license_plates_in_exec.keys()) )
    # Assegnamento dell'istante di creazione dell'evento
    engine_event["created_at"] = time.time()

    # Rimozione temporanea della targa appena estratta per evitare che i due eventi siano associati alla stessa targa
    list_license_plates_in_exec = list( license_plates_in_exec.keys() )
    list_license_plates_in_exec.remove(engine_event["license_plate"])

    try:
        # Pick della targa da associare all'evento panic button
        panic_button_event["license_plate"] = random.choice( list_license_plates_in_exec )
    except IndexError:
        if len(list_license_plates_in_exec) == 0:
            panic_button_event["license_plate"] = engine_event["license_plate"]
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
            # Gestione evento sulla base della tipologia di motorizzazione dell'autobus smart su cui avviene l'evento stesso
            match license_plates_in_exec[engine_event["license_plate"]]["engine_type"]:
                case Motore.TERMICO:
                    termic_num = handle_event(event=engine_event, license_plates_running=license_plates_in_exec, simulate=first_exec, stop_frequency=fermata_bus, num_buses=termic_num, event_list=license_plates_in_exec[engine_event["license_plate"]]["list"], event_index=license_plates_in_exec[engine_event["license_plate"]]["index"], engine=license_plates_in_exec[engine_event["license_plate"]]["engine_type"])
                case Motore.IBRIDO:
                    hybrid_num = handle_event(event=engine_event, license_plates_running=license_plates_in_exec, simulate=first_exec, stop_frequency=fermata_bus, num_buses=hybrid_num, event_list=license_plates_in_exec[engine_event["license_plate"]]["list"], event_index=license_plates_in_exec[engine_event["license_plate"]]["index"], engine=license_plates_in_exec[engine_event["license_plate"]]["engine_type"])
                case Motore.ELETTRICO:
                    electric_num = handle_event(event=engine_event, license_plates_running=license_plates_in_exec, simulate=first_exec, stop_frequency=fermata_bus, num_buses=electric_num, event_list=license_plates_in_exec[engine_event["license_plate"]]["list"], event_index=license_plates_in_exec[engine_event["license_plate"]]["index"], engine=license_plates_in_exec[engine_event["license_plate"]]["engine_type"])

        # Verifica assenza autobus in esecuzione, in questo caso l'esecuzione viene interrotta
        if termic_num == 0 and hybrid_num == 0 and electric_num == 0:
            print(f"Esecuzione interrotta a causa di insufficienza di autobus")
            print("Spegnimento motore...")
            sys.exit(0)

        # Verifica uguaglianza targhe eventi
        engine_event["license_plate"], panic_button_event["license_plate"] = events_license_plates_safe(lp_engine=engine_event["license_plate"], lp_pbutton=panic_button_event["license_plate"], time_engine=engine_event["created_at"], time_pbutton=panic_button_event["created_at"], lp_list=list( license_plates_in_exec.keys() ))

        # Verifica avvenimento evento panic button
        if panic_button_event["happened"]:
            # Gestione evento sulla base della tipologia di motorizzazione dell'autobus smart su cui avviene l'evento stesso
            match license_plates_in_exec[panic_button_event["license_plate"]]["engine_type"]:
                case Motore.TERMICO:
                    termic_num = handle_event(event=panic_button_event, license_plates_running=license_plates_in_exec, simulate=first_exec, stop_frequency=fermata_bus, num_buses=termic_num, event_list=license_plates_in_exec[panic_button_event["license_plate"]]["list"], event_index=license_plates_in_exec[panic_button_event["license_plate"]]["index"], engine=license_plates_in_exec[panic_button_event["license_plate"]]["engine_type"])
                case Motore.IBRIDO:
                    hybrid_num = handle_event(event=panic_button_event, license_plates_running=license_plates_in_exec, simulate=first_exec, stop_frequency=fermata_bus, num_buses=hybrid_num, event_list=license_plates_in_exec[panic_button_event["license_plate"]]["list"], event_index=license_plates_in_exec[panic_button_event["license_plate"]]["index"], engine=license_plates_in_exec[panic_button_event["license_plate"]]["engine_type"])
                case Motore.ELETTRICO:
                    electric_num = handle_event(event=panic_button_event, license_plates_running=license_plates_in_exec, simulate=first_exec, stop_frequency=fermata_bus, num_buses=electric_num, event_list=license_plates_in_exec[panic_button_event["license_plate"]]["list"], event_index=license_plates_in_exec[panic_button_event["license_plate"]]["index"], engine=license_plates_in_exec[panic_button_event["license_plate"]]["engine_type"])

        # Verifica assenza autobus in esecuzione, in questo caso l'esecuzione viene interrotta
        if termic_num == 0 and hybrid_num == 0 and electric_num == 0:
            print(f"Esecuzione interrotta a causa di insufficienza di autobus")
            print("Spegnimento motore...")
            sys.exit(0)

        # Verifica uguaglianza targhe eventi
        engine_event["license_plate"], panic_button_event["license_plate"] = events_license_plates_safe(lp_engine=engine_event["license_plate"], lp_pbutton=panic_button_event["license_plate"], time_engine=engine_event["created_at"], time_pbutton=panic_button_event["created_at"], lp_list=list( license_plates_in_exec.keys() ))

        # SIMULAZIONE METRICHE
        # Termic
        simulate_metrics(bus_number=termic_num, buses_list=termic_bus_list, update_stop_frequency_list=update_fermata_bus_list, init=first_exec, stop_frequency=fermata_bus, engine=Motore.TERMICO)
        # Hybrid
        simulate_metrics(bus_number=hybrid_num, buses_list=hybrid_bus_list, update_stop_frequency_list=update_fermata_bus_list, init=first_exec, stop_frequency=fermata_bus, engine=Motore.IBRIDO)
        # Electric
        simulate_metrics(bus_number=electric_num, buses_list=electric_bus_list, update_stop_frequency_list=update_fermata_bus_list, init=first_exec, stop_frequency=fermata_bus, engine=Motore.ELETTRICO)

        # PREPARAZIONE "PACCHETTO" DATI
        # Termic
        format_data_packet(bus_number=termic_num, buses_list=termic_bus_list, format_type=format, engine=Motore.TERMICO)
        # Hybrid
        format_data_packet(bus_number=hybrid_num, buses_list=hybrid_bus_list, format_type=format, engine=Motore.IBRIDO)
        # Electric
        format_data_packet(bus_number=electric_num, buses_list=electric_bus_list, format_type=format, engine=Motore.ELETTRICO)

        # INVIO "PACCHETTO" DATI
        # Termic
        communicate_data(bus_number=termic_num, buses_list=termic_bus_list, engine=Motore.TERMICO)
        # Hybrid
        communicate_data(bus_number=hybrid_num, buses_list=hybrid_bus_list, engine=Motore.IBRIDO)
        # Electric
        communicate_data(bus_number=electric_num, buses_list=electric_bus_list, engine=Motore.ELETTRICO)

        # STAMPA A VIDEO REPORT METRICHE
        # Termic
        show_data(bus_number=termic_num, buses_list=termic_bus_list, engine=Motore.TERMICO)
        # Hybrid
        show_data(bus_number=hybrid_num, buses_list=hybrid_bus_list, engine=Motore.IBRIDO)
        # Electric
        show_data(bus_number=electric_num, buses_list=electric_bus_list, engine=Motore.ELETTRICO)

        # Inversione flag a segnalare che la prima esecuzione del Ciclo azioni è terminata 
        first_exec = False 

        # Verifica uguaglianza di tutti i contatori di fermate dei bus restituiti dalle funzioni simulate()
        for i in range(0, len(update_fermata_bus_list)-1):
            for j in range(i+1, len(update_fermata_bus_list)):
                if update_fermata_bus_list[i] != update_fermata_bus_list[j]:
                    sys.stderr.write("Errore! Uno dei contatori delle fermate bus è diverso dagli altri\n")
                    sys.exit(-14)
        update_fermata_bus = update_fermata_bus_list[0]

        # Aggiornamento al valore restituito dalle funzioni di simulazione metriche
        fermata_bus = update_fermata_bus

        time.sleep(delay_metrics)


if __name__ == "__main__":
    main()
