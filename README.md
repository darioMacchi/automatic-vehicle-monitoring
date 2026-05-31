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

- `config/` → file di configurazione dei broker Kafka.
- `examples/` → esempi e script di integrazione (MQTT, Kafka, Flink).
- `examples/processing/` → esempi di job Flink e utilità di processing.
- `executables/` → script di avvio per i broker Kafka.
- `src/avm/` → codice del pacchetto AVM (simulazione autobus, bridge MQTT↔Kafka, script).
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

## 🔧 Sostituzione del percorso del file `.jar`

Alcuni esempi contengono un percorso fittizio per il connettore Kafka di Flink. È necessario sostituirlo con la reale locazione assoluta del file `.jar` presente sulla propria macchina.

### 📄 File da aggiornare

- `kafka_json_format.py` (riga 103)
- `kafka_json_format_writer.py` (riga 78)
- `processing_on_edge.py` (riga 174)
- `processing_experimenting.py` (riga 204)

### ✏️ Riga di esempio da sostituire

```python
env.add_jars("file:///absolute-path/to/flink-sql-connector-kafka-3.3.0-1.20.jar")
```

### ✅ Esempio di configurazione corretta

```python
env.add_jars("file:///home/user/flink/flink-sql-connector-kafka-3.3.0-1.20.jar")
```

Sostituire il percorso con la reale locazione assoluta del file `.jar` presente sul proprio sistema.

### 📦 Download del connettore Kafka per Flink

È possibile scaricare il JAR ufficiale da Maven Central:

🔗 <https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.3.0-1.20/flink-sql-connector-kafka-3.3.0-1.20.jar>

Per maggiori informazioni:

- Maven Central: <https://repo.maven.apache.org/>
- Apache Flink: <https://flink.apache.org/>
