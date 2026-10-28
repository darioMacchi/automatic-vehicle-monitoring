# Check host MQTT - funzione utile alla verifica della coerenza dell'host MQTT passato da linea di comando e non solo. La 
# verifica prevede il controllo della presenza di una stringa non vuota
def check_mqtt_host(host: str, input_check: bool) -> None:
    if type(host) is not str:
        raise TypeError(f"Errore! Il tipo del parametro 'host' passato deve essere 'str'. Ricevuto {type(host)}")
    
    if type(input_check) is not bool:
        raise TypeError(f"Errore! Il tipo del parametro 'input_check' passato deve essere 'bool'. Ricevuto {type(input_check)}")

    # Check stringa non vuota
    error_to_str = None
    if host == "":
        error_to_str = "deve essere un indirizzo non nullo"
        if input_check:
            error_to_str = "Errore! L'argomento $host " + error_to_str
        else:
            error_to_str = "Errore! L'host fornito " + error_to_str

        raise ValueError(error_to_str)


# Check porta MQTT - funzione utile alla verifica della coerenza della porta MQTT passata da linea di comando e non solo. La 
# verifica prevede il controllo del tipo del numero di porta, e che il numero di porta rappresenti effettivamente una delle
# porte utilizzate dal protocollo MQTT utilizzato
def check_mqtt_port(port: int, input_check: bool) -> int:
    if type(input_check) is not bool:
        raise TypeError(f"Errore! Il tipo del parametro 'input_check' passato deve essere 'bool'. Ricevuto {type(input_check)}")

    mqtt_port = None
    error_to_str = None

    # Check numero valido
    try:
        mqtt_port = int(port)
    except ValueError as error:
        error_to_str = "non è un numero valido"
        if input_check:
            error_to_str = "Errore! L'argomento $porta passato da linea di comando " + error_to_str
        else:
            error_to_str = "Errore! Il numero di porta fornito " + error_to_str

        raise ValueError(error_to_str) from error

    # Check porta
    if mqtt_port != 1883 and mqtt_port != 8883:
        error_to_str = "deve essere una porta MQTT valida: 1883 oppure 8883 (connessioni SSL)"
        if input_check:
            error_to_str = "Errore! L'argomento $porta " + error_to_str
        else:
            error_to_str = "Errore! Il numero di porta fornito " + error_to_str

        raise ValueError(error_to_str)

    return mqtt_port


# Check brokers Kafka - funzione utile alla verifica della coerenza dei brokers Kafka passati da linea di comando. La 
# verifica prevede la rimozione di eventuali spazi vuoti presenti nella stringa di connessione, il controllo della presenza
# del carattere ':' a indicare la separazione tra host e porta, successivamnte viene verificato che l'host non sia vuoto, e
# inoltre che la porta sia di tipo intero e che ricada nell'insieme di porte che rappresentano i nodi del cluster
def check_brokers(brokers: list[str]) -> list[str]:
    if type(brokers) is not list:
        raise TypeError(f"Errore! Il tipo del parametro 'brokers' passato deve essere 'list'. Ricevuto {type(brokers)}")
        
    # Inizializzazione parametri di ritorno
    kafka_brokers = []

    # Brokers Kafka
    for broker in brokers:
        # Rimozione eventuali blank spaces
        broker = broker.replace(" ", "")

        try:
            # Controllo formato stringa broker strettamente del tipo 'host:porta'
            host, port = broker.split(":")
        except Exception as error:
            raise SyntaxError("Errore! Gli argomenti $host_kafka-* e $port_kafka-* passati da linea di comando devono essere nel formato '$host_kafka-*:port_kafka-*'") from error

        # Host Kafka
        # Check stringa non vuota
        if host == "":
            raise ValueError("Errore! L'argomento $host_kafka-* deve essere un indirizzo non nullo")

        # Port Kafka
        # Check numero valido
        try:
            port = int(port)
        except ValueError as error:
            raise ValueError("Errore! L'argomento $porta_kafka-* passato da linea di comando non è un numero valido") from error

        # Check porta
        if port != 9092 and port != 9094 and port != 9096:
            raise ValueError("Errore! L'argomento $porta_kafka-* deve essere una porta Kafka valida: 9092 | 9094 | 9096")
        
        kafka_brokers.append(broker)

    return kafka_brokers
