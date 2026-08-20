# 🚗 Automatic Vehicle Monitoring (AVM)
Progetto universitario per la realizzazione di un sistema di telemetria dedicato al raccoglimento dati da veicoli, in particolare autobus. Lo stack prevede la simulazione dei veicoli, e la comunicazione attraverso protocollo MQTT ad un sistema che comprende: Ingestion, Processing, Storage, Visualization.

## 📌 Overview
Questo progetto implementa un sistema di vehicle monitoring che consente di:
  - 📍 Tracciare la posizione dei veicoli.
  - 📊 Analizzare dati di utilizzo e performance.
  - ⚙️ Monitorare parametri tecnici: velocità, pressione degli pneumatici, stato del motore, stato dell'impianto frenante, informazioni sui consumi, dati ambientali.
  - 🧠 Preparare i dati per applicazioni di analytics o machine learning.

Il sistema si ispira ai moderni approcci di fleet management e telemetria, comunemente utilizzati nel trasporto e nella mobilità intelligente.

## 🏗️ Architettura
Il progetto è strutturato in diversi moduli:
  - Data Collection / Ingestion → acquisizione dati dai sensori.
  - Processing Layer → aggregazione e analisi dei dati.
  - Storage → database.
  - Visualization → esposizione dei dati tramite dashboard.

```
Vehicle → Data Collector / Ingestion → Processing → Storage → Dashboard
```

## ⚙️ Tecnologie utilizzate
  - Linguaggio →  Python
  - Data Collection / Ingestion → Apache Kafka.
  - Processing Layer → Apache Flink.
  - Storage →  MongoDB.
  - Visualization → Grafana.
  - Comunicazione / Messaging → MQTT.

## 📁 Struttura del progetto

```text
.
├── .gitignore
├── config/
├── examples/
│   └── processing/
├── executables/
├── src/
│   └── avm/
├── tests/
├── requirements.txt
└── setup.py
```

### Descrizione

- `.gitignore` → file e directory che Git deve ignorare.
- `config/` → file di configurazione dei broker Kafka.
- `examples/` → esempi e script di integrazione (MQTT, Kafka, Flink, MongoDB).
- `examples/processing/` → esempi di job Flink e utilità di processing.
- `executables/` → script di avvio per i broker Kafka.
- `src/avm/` → codice del pacchetto AVM (simulazione autobus, bridge MQTT↔Kafka, bridge Ingestion↔Storage, processing on Edge, live dashboarding, script).
- `tests/` → test e script di verifica.
- `requirements.txt` → dipendenze del progetto.
- `setup.py` → metadati del pacchetto.

## 🚀 Installazione (Editable Mode)

Si consiglia di utilizzare un ambiente virtuale Python.

Esempio di installazione:

```bash
python -m venv vAVMenv
source vAVMenv/bin/activate   # Linux/macOS

# Oppure su Windows
vAVMenv\Scripts\activate

pip install -e .
```

L'opzione `pip install -e .` installa il pacchetto in modalità **editable**, consentendo di applicare modifiche al codice sorgente locale che saranno immediatamente visibili senza dover reinstallare il pacchetto.

## ⚙️ Configurazione dell'ambiente

Il progetto utilizza un file `.env` nella root della repository per gestire le variabili di configurazione necessarie all'esecuzione degli esempi.

Prima di eseguire gli script, è necessario creare il proprio file `.env` e configurare le variabili richieste.

### 📄 Creazione del file `.env`

Creare il file `.env` inserendo i valori corretti per il proprio ambiente.

Il file deve contenere almeno le seguenti variabili:

```env
FLINK_CONNECTOR_KAFKA_JAR=/absolute/path/to/flink-sql-connector-kafka-3.3.0-1.20.jar
MONGODB_URI=mongodb://username:password@host:port/database
```

### 🔧 Variabili di configurazione

| Variabile | Descrizione |
|---|---|
| `FLINK_CONNECTOR_KAFKA_JAR` | Percorso assoluto del connettore Kafka per Apache Flink (`.jar`). |
| `MONGODB_URI` | Connection string utilizzata per la connessione al database MongoDB. |

### 📦 Connettore Kafka per Flink

La variabile `FLINK_CONNECTOR_KAFKA_JAR` deve contenere il percorso assoluto del file `.jar` presente sulla propria macchina.

È possibile scaricare il connettore Kafka ufficiale da Maven Central:

🔗 <https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.3.0-1.20/flink-sql-connector-kafka-3.3.0-1.20.jar>

Ad esempio:

```env
FLINK_CONNECTOR_KAFKA_JAR=/home/user/flink/flink-sql-connector-kafka-3.3.0-1.20.jar
```

Sostituire il percorso con la posizione assoluta del file `.jar` sulla propria macchina.

### 🍃 MongoDB

La variabile `MONGODB_URI` deve contenere la connection string del database MongoDB utilizzato dal progetto.

Ad esempio, per un'istanza MongoDB locale:

```env
MONGODB_URI=mongodb://username:password@localhost:27017/AVMDb
```

Nel caso di MongoDB Atlas, utilizzare la connection string fornita da Atlas.

### 🔒 Sicurezza

Il file `.env` può contenere informazioni sensibili, come credenziali di accesso a MongoDB. **Non deve quindi essere committato nella repository.**

Assicurarsi che il file `.gitignore` contenga:

```gitignore
.env
```
