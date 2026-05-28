import signal
import sys
import time

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
# oltre che valido, sia anche un numero non negativo, per l'ovvia ragione che non è possibile avere un numero negativo
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

# Method main() - esecuzione del sistema di telemetria AVM relativo agli autobus smart, con controllo dei parametri 
# passati da linea di comando, setup dei range per le metriche, del formato dati e altri informazioni necessarie alla 
# corretta esecuzione, instanziazione degli autobus smart con diverse motorizzazioni e Ciclo azioni con le operazioni
# di simulazione metriche, preparazione e invio "pacchetto" dati e stampa a video
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
    # TODO
    # Dimensionamento tra 1 e 5 secondi - DA DECIDERE
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

    # Installazione handler del segnale CTRL+C
    signal.signal(signalnum=signal.SIGINT, handler=shutdown_all_autobus)

    # Verifica validità numero autobus e indirizzo broker MQTT
    termic_num, hybrid_num, electric_num, host, port = check_cmd_line_args(termic_autobus_num=sys.argv[1], hybrid_autobus_num=sys.argv[2], electric_autobus_num=sys.argv[3], host=sys.argv[4], port=sys.argv[5])

    # Istanziazione oggetti Autobus
    # Utilizzo della lista globale di autobus smart con motorizzazione termica
    global termic_bus_list
    for _ in range(0, termic_num):
        termic_bus_list.append(AutobusTermico(ranges=ranges, timeout=delay_mqtt, host=host, port=port))

    # Utilizzo della lista globale di autobus smart con motorizzazione ibrida
    global hybrid_bus_list
    for _ in range(0, hybrid_num):
        hybrid_bus_list.append(AutobusIbrido(ranges=ranges, timeout=delay_mqtt, host=host, port=port))

    # Utilizzo della lista globale di autobus smart con motorizzazione elettrica
    global electric_bus_list
    for _ in range(0, electric_num):
        electric_bus_list.append(AutobusElettrico(ranges=ranges, timeout=delay_mqtt, host=host, port=port))

    print("Accensione motore...\n")
    time.sleep(delay_setup)

    # Ciclo azioni
    while True:
        # Rimozione dalla lista dei valori delle precedenti esecuzioni del Ciclo azioni
        update_fermata_bus_list.clear()

        # Aggiornamento corrispondente al numero attuale di esecuzioni del Ciclo azioni
        fermata_bus += 1

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
