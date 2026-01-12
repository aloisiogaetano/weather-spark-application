# Weather Spark Application
## 📌 Overview

Questo progetto implementa una pipeline di data processing utilizzando Apache Spark per l’analisi di dati meteorologici storici.
L’obiettivo è dimostrare la capacità di:

progettare trasformazioni Spark robuste

gestire dati reali e potenzialmente sporchi

strutturare un progetto scalabile e testabile

Il progetto risponde a tre task analitici distinti, ognuno implementato come job indipendente ma orchestrato tramite un unico entrypoint (main.py).

## 🧱 Struttura del Progetto
```
weather-spark-app/
│
├── docker/
│   └── Dockerfile.dockerfile
│
├── src/
│   ├── main.py
│   ├── jobs/
│   │   ├── clear_days.py
│   │   ├── nation_stats.py
│   │   └── seasonal_range.py
│   │
│   ├── transformations/
│   │   ├── wide_to_long.py
│   │   ├── join_metrics.py
│   │   ├── convert_to_local.py
│   │   ├── add_season.py
│   │   ├── clear_days_logic.py
│   │   └── aggregate_*.py
│   │
│   └── utils/
│       └── spark_session.py
│
├── tests/
│   ├── conftest.py
│   ├── test_clear_days_logic.py
│   ├── test_nation_stats.py
│   └── test_seasonal_range.py
│
├── data/
│   └── raw/
│
├── requirements.txt
├── docker-compose.yml
└── README.md
```

### ⚙️ Scelte Implementative (Design Decisions)
Tecnologie

Apache Spark 3.5.1 (PySpark 3.5.1)
Scelto per la sua capacità di gestire grandi volumi di dati, API dichiarative e ampia diffusione in ambienti enterprise.

Docker
Garantisce riproducibilità dell’ambiente, isolamento delle dipendenze e portabilità.

PyTest
Per test unitari e di integrazione sulle trasformazioni Spark.

### 🧠 Gestione dei Dati
Dataset Wide → Long

Tutti i dataset meteorologici (temperature, pressure, humidity) sono forniti in formato wide, con una colonna per ogni città.

👉 È stata introdotta una transformation comune **reshape_wide_to_long** per:

- normalizzare i dati

- semplificare join e aggregazioni

- rendere il codice riutilizzabile e testabile

👉 Dati Sporchi e Assunzioni:

- Valori nulli gestiti tramite filtri espliciti o ignorati durante le aggregazioni (comportamento standard Spark).

- Le conversioni datetime sono centralizzate e mappate con i rispettivi timezone.

- Città non presenti in tutti i dataset: le join sono effettuate in modo conservativo (inner/left join a seconda del contesto).

### 📊 Task 1 – Analisi del Tempo Sereno in Primavera
Obiettivo

Per ogni anno, individuare le città che hanno avuto almeno 15 giorni al mese (marzo, aprile, maggio) con tempo sereno.

Scelte Chiave

Un “giorno sereno” è definito come un giorno con ≥ 18 ore di "sky is clear".

Le descrizioni meteo sono aggregate:

ora → giorno

giorno → mese

mese → anno

I mesi primaverili sono valutati in AND (tutti devono soddisfare il criterio).

API Spark Utilizzate

- groupBy

- count

- filter

- funzioni datetime

🌍 Task 2 – Statistiche Meteorologiche per Nazione
Obiettivo

Calcolare per ogni nazione, mese e anno:

media

deviazione standard

minimo

massimo
di temperatura, pressione e umidità.

Scelte Chiave

Tutti i dataset vengono:

normalizzati (wide → long)

rinominati semanticamente (temperature, pressure, humidity)

joinati su (datetime, city)

Join con city_attributes.csv per ottenere la nazione

Conversione degli orari da UTC a fuso locale tramite mapping città → timezone

Motivazione

Separare:

logica di reshaping

logica di join

logica di aggregazione

rende il codice:

testabile

riutilizzabile

facilmente estendibile

🌡️ Task 3 – Escursione Termica Stagionale (Top 3 Città)
Obiettivo

Per ogni nazione, individuare nel 2017 le 3 città con la maggiore differenza tra:

temperatura media periodo caldo (giugno–settembre)

temperatura media periodo freddo (gennaio–aprile)

considerando solo la fascia oraria locale 12:00–15:00.

Scelte Chiave

Introduzione esplicita del concetto di stagione (add_season)

I mesi fuori dai periodi definiti vengono esclusi

Le medie stagionali sono calcolate aggregando tutti i mesi del periodo

Ranking per nazione tramite Window + row_number

Test

Verifica assegnazione stagioni

Verifica calcolo differenza termica

Verifica ranking top 3 per nazione

🧪 Testing

I test usano una SparkSession reale

Nessun mock di Spark → comportamento realistico

Le transformation sono testate isolatamente

È presente conftest.py per inizializzare Spark una sola volta

Esecuzione test:

docker-compose run --rm spark-app pytest

🚀 Come Avviare il Progetto
Prerequisiti

Docker ≥ 20.x

Docker Compose

Build dell’ambiente
docker-compose build

Esecuzione Task
Task 1
docker-compose run --rm spark-app \
  python src/main.py task1 data/raw/weather_description.csv

Task 2
docker-compose run --rm spark-app \
  python src/main.py task2 \
  data/raw/temperature.csv \
  data/raw/pressure.csv \
  data/raw/humidity.csv \
  data/raw/city_attributes.csv

Task 3
docker-compose run --rm spark-app \
  python src/main.py task3

🐳 Perché Docker

Docker è stato scelto per:

eliminare dipendenze locali (Java, Spark, Python)

garantire coerenza tra ambiente di sviluppo e test

semplificare la valutazione da parte del reviewer

Un singolo docker-compose.yml è sufficiente per:

eseguire i job

lanciare i test

estendere il progetto

☸️ Integrazione con Kubernetes (Possibile Evoluzione)

In un contesto produttivo:

Il container Spark può essere eseguito su Spark on Kubernetes

I job possono diventare:

SparkApplication (Spark Operator)

job schedulati (Airflow / Argo)

I dataset possono risiedere su:

S3 / GCS / ADLS

Delta Lake

🔮 Possibili Sviluppi Futuri

Introduzione di Delta Lake

Validazione schema con Great Expectations

Metriche e logging strutturato

CI/CD con GitHub Actions

Parametrizzazione completa via config file


Supporto multi-year e multi-timezone dinamico





