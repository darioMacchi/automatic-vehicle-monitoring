import sys

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import (KafkaError, NoBrokersAvailable,
                          TopicAlreadyExistsError)


# Metodo dedito alla verifica della correttezza dei comandi passati da linea di comando, vengono esseguite verifiche
# sul tipo del parametro passato da linea di comando, ulteriori verifiche su stringhe non nulle, sulla validità dei
# numeri di porta passati e sull'appartenenza al range permesso. Inoltre viene eseguita una verifica sul formato dei
# parametri passati da linea di comando come indirizzi dei bootstrap server (broker) di Kafka, ossia viene verificato che
# il formato sia  strettamente del tipo 'host:porta'
def check_cmd_line_args(topics: list[str], servers: list[str]):
    if type(topics) is not list:
        raise TypeError(f"Errore! Il tipo del parametro 'topics' passato deve essere 'list'. Ricevuto {type(topics)}")
    
    if type(servers) is not list:
        raise TypeError(f"Errore! Il tipo del parametro 'servers' passato deve essere 'list'. Ricevuto {type(servers)}")
    
    topic_to_ret = []
    servers_to_ret = []

    # Topics
    for topic in topics:
        # Verifica stringa non nulla
        if topic != "":
            topic_to_ret.append(topic)
    
    # Servers
    for server in servers:
        # Rimozione eventuali blank spaces
        server = server.replace(" ", "")

        try:
            # Controllo formato stringa broker strettamente del tipo 'host:porta'
            broker, porta = server.split(":")
        except Exception:
            sys.stderr.write("Errore! Il formato degli elementi contenuti nel parametro 'servers' deve essere $broker:porta\n")
            sys.exit(-1)
        
        # Verifica stringa non nulla
        if broker != "":
            try:
                # Verifica validità del numero di porta passato (è effettivamente un numero o meno)
                porta = int(porta)
            except ValueError:
                sys.stderr.write("Errore! L'argomento $porta passato da linea di comando non è un numero valido\n")
                sys.exit(-2)
            else:
                # Verifica range porta
                if porta < 1 or porta > 65535:
                    sys.stderr.write("Errore! L'argomento $porta passato da linea di comando non si trova all'interno del range [1, 65535]\n")
                    sys.exit(-3)
            
            # Aggiunta della stringa broker all'interno della lista di bootstrap server da restitituire
            servers_to_ret.append(server)
    
    return topic_to_ret, servers_to_ret

# Metodo dedito all'istanziazione dell'oggetto Admin necessario per configurare correttamente i topic con i parametri
# desiderati
def connect_admin(bootstrap_servers: list[str], client_id="AVM_telemetry_admin"):
    # Gestione errore di connessione ad un broker non disponibile alla connessione
    try:
        # Instanziazione Kafka admin con bootstrap server a cui deve avvenire la connessione, e client_id
        admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers, client_id=client_id)
    except NoBrokersAvailable:
        sys.stderr.write("Errore! Nessun broker disponibile per la connessione\n")
        sys.exit(-4)
    else:
        return admin

# Metodo dedito alla creazione dei topic con i parametri desiderati, ossia per fare in modo che il topic sia gestito
# tra più broker Kafka, con un certo grado di partizione, replica e repliche in-sync (ISR). Verifica se il topic è già
# presente nel cluster, altrimenti si incarica della creazione
def ensure_topics(admin: KafkaAdminClient, topics: list[str], partitions=2, replication=3):
    try:
        # Acquisizione topic presenti nel cluster
        topics_created = admin.list_topics()
    except KafkaError:
        sys.stderr.write("Errore! Impossibile ottenere il listato dei topic presenti nel cluster\n")
        sys.exit(-5)
    
    print(f"Topics already in the cluster:\n{topics_created}\n")

    # Per ogni topic presente all'interno della lista di topic da creare passata alla funzione, se non è già presente
    # nel cluster avviene la creazione
    for topic_name in topics:
        # Verifica topic assente all'interno del cluster
        if topic_name not in topics_created:
            # Creazione topic attraverso l'oggetto NewTopic specificando:
            #   name: nome del topic
            #   num_partitions: numero di partizioni assegnate al topic
            #   replication_factor: grado di replicazione del topic, ossia su quanti broker deve essere replicato
            #   topic_configs: specifica del numero minimo di repliche che deve essere in-sync, ossia allineate
            #                  con il leader rispetto alle partizioni del topic
            topic_to_create = NewTopic(
                name=topic_name,
                num_partitions=partitions,
                replication_factor=replication,
                topic_configs={
                    "min.insync.replicas":"2"
                }
            )

            try:
                # Creazione del topic all'interno del cluster
                admin.create_topics(new_topics=[topic_to_create])
                print(f"Topic {topic_name} created")
            except TopicAlreadyExistsError:
                sys.stderr.write("Errore! Cercato di creare un topic che esiste già nel cluster\n")
                sys.exit(-6)
            except KafkaError:
                sys.stderr.write("Errore!\n")
                sys.exit(-7)
        else:
            # Topic già presente nel cluster, per cui non viene fatta nessun'altra azione
            print(f"Topic {topic_name} already in the cluster")

# Funzione main che consente di passare diversi argomenti da CLI al programma, controllare la correttezza di questi, 
# successivamente si occupa di creare l'oggetto Admin e di creare il topic passato da CLI all'interno del cluster Kafka
def main():
    if len(sys.argv) < 3 or len(sys.argv) > 5:
        sys.stderr.write(f"Errore! Uso coretto del programma: python[3] {sys.argv[0]} $nome_topic $broker:porta [$broker:porta ...]\n")
        sys.stderr.write("\t$nome_topic = 'topic_da_creare'\n")
        sys.stderr.write("\t$broker:porta\n")
        sys.stderr.write("\t\t$broker = 'indirizzo_broker_Kafka'\n")
        sys.stderr.write("\t\t$porta = ['9092', '9093', ...]\n")
        sys.stderr.write("\t[$broker:porta ...] = 'eventuali_altri_broker'\n")
        sys.exit(-1)

    # Verifica correttezza degli argomenti passati da linea di comando
    topics, servers = check_cmd_line_args([sys.argv[1]], sys.argv[2:])

    # Lista di bootstrap servers necessari alla connessione al cluster dell'oggetto Admin
    bootstrap_servers = servers.copy()

    # Creazione e connessione al cluster Kafka dell'oggetto Admin
    admin = connect_admin(bootstrap_servers=bootstrap_servers)

    print("Admin started...\n")

    try:
        # Creazione del topic
        ensure_topics(admin=admin, topics=topics)
    finally:
        # Assicurazione di chiusura dell'oggetto Admin
        admin.close()


if __name__ == "__main__":
    main()
