# ETL Pipeline - Vacunación & Decesos COVID-19 Ecuador

> Proyecto final del curso **ETL (G51)** — Facultad de Ingeniería y Ciencias Básicas  
> Universidad Autónoma de Occidente · Programa: Ingeniería de Datos e Inteligencia Artificial

---

## Descripción general

Pipeline ETL completo que integra datos de **vacunación COVID-19 en Ecuador** (fuente: DWH SQLite propio) con datos de **decesos COVID-19** (fuente: Google BigQuery público). El pipeline está orquestado con **Apache Airflow**, transmite métricas en tiempo real mediante **Apache Kafka**, y expone un dashboard reactivo construido con **Plotly Dash**.

---

## Arquitectura del sistema

```

<img width="1013" height="859" alt="image" src="https://github.com/user-attachments/assets/fa75ab4c-503e-4e2a-99d3-10d44ffc82a2" />

```

---

## Fuentes de datos

| # | Fuente | Tecnología | Contenido |
|---|--------|-----------|-----------|
| 1 | DWH propio (segunda entrega) | SQLite | `fact_vacunacion`, `dim_fecha`, `dim_canton`, `dim_provincia`, `dim_region`, `dim_indice_uhc` |
| 2 | Google BigQuery público | BigQuery API | `bigquery-public-data.covid19_jhu_csse.deaths` — filtrado por Ecuador |

---

## Stack tecnológico

| Capa | Herramienta |
|------|-------------|
| Orquestación | Apache Airflow |
| Entorno de ejecución | Google Colab |
| Almacenamiento | SQLite (DWH) |
| Fuente externa | Google BigQuery |
| Calidad de datos | Great Expectations |
| Streaming | Apache Kafka (modo KRaft, sin Zookeeper) |
| Cliente Kafka Python | `confluent-kafka` |
| Visualización RT | Plotly Dash + Plotly Graph Objects |
| Túnel público | Cloudflare Tunnel (`cloudflared`) |
| Control de versiones | Git / GitHub |

---

## Estructura del repositorio

```
.
├── ETL_final_delivery.ipynb   # Notebook principal: DAG, producer, consumer y lanzamiento del dashboard
├── dashboard_rt.py            # App Dash con consumidor Kafka integrado (dashboard en tiempo real)
├── README.md                  # Este archivo
├── .gitignore
└── docs/
    └── technical_report.docx  # Documento técnico (arquitectura, pipeline, insights)
```

---

## Diseño del DAG de Airflow

El DAG `etl_final_delivery` se ejecuta con `schedule_interval="@daily"` y contiene **6 tareas**:

```
[extract_db] ──┐
               ├──► [transform] ──► [quality_checks] ──► [load] ──► [kafka_producer]
[extract_bq] ──┘
```

| Tarea | Descripción |
|-------|-------------|
| `extract_db` | Lee todas las tablas del DWH SQLite de la segunda entrega y las exporta como CSV temporales |
| `extract_bigquery` | Consulta la tabla pública de muertes COVID-19 en BigQuery filtrando por Ecuador |
| `transform` | Limpia, valida y convierte los datos de ambas fuentes; genera CSVs transformados |
| `quality_checks` | Ejecuta expectativas con Great Expectations sobre todas las tablas transformadas |
| `load` | Crea el esquema del DWH final en SQLite y carga dimensiones + tablas de hechos |
| `kafka_producer` | Lee `fact_vacunacion` y `fact_decesos` del DWH y los publica en Kafka con delay de 1 ms/msg |

---

## Modelo dimensional

```

<img width="575" height="698" alt="image" src="https://github.com/user-attachments/assets/94f46535-1b67-4a29-baea-aeeccf04a821" />


```

---

## Streaming con Kafka

- **Broker**: Kafka 3.7.0 en modo **KRaft** (sin Zookeeper).
- **Tópicos**: `eventos-vacunacion` y `eventos-decesos`.
- **Producer** (task `kafka_producer` del DAG): itera sobre todas las filas de ambas tablas de hechos y las publica con un delay de 1 ms entre mensajes, simulando un flujo continuo.
- **Consumer** (integrado en `dashboard_rt.py`): corre en un hilo daemon, suscribiéndose a ambos tópicos y almacenando los mensajes en listas en memoria protegidas por `threading.Lock`.

---

## Dashboard en tiempo real

Archivo: `dashboard_rt.py` · Puerto: `8050`

El dashboard se refresca cada **5 segundos** y muestra:

- **KPIs**: eventos recibidos (vacunación / decesos), dosis totales acumuladas, muertes acumuladas, tasa de mensajes (msg/s).
- **Gráfico de barras**: dosis acumuladas por tipo (primera, segunda, única, refuerzo).
- **Gráfico de área**: decesos acumulados en el tiempo.
- **Gráfico de barras horizontal**: top 10 provincias por dosis totales.
- **Gráfico de líneas**: mensajes acumulados de ambos tópicos con tasa en tiempo real.
- **Tabla**: últimos 10 registros recibidos (5 de cada tópico).

---

## Instrucciones de configuración

### Prerrequisitos

- Cuenta de Google con acceso a Google Colab y Google Drive
- Proyecto en Google Cloud Platform con BigQuery habilitado
- El DWH SQLite de la segunda entrega guardado en Google Drive (ruta configurada en el DAG)

### Pasos

1. **Abrir el notebook** `ETL_final_delivery.ipynb` en Google Colab.
2. **Montar Google Drive** (primera celda).
3. **Instalar dependencias**: Airflow, Dash, Kafka (celdas de instalación).
4. **Autenticar BigQuery** con `google.colab.auth.authenticate_user()`.
5. **Ejecutar el DAG**: iniciar Airflow con el webserver y el scheduler, acceder a la UI via el túnel Cloudflare generado, y activar el DAG `etl_final_delivery`.
6. **Lanzar el dashboard**: ejecutar la celda de lanzamiento de Dash y acceder a la URL del proxy Colab en el puerto 8050.

### Variables de entorno requeridas

| Variable | Descripción |
|----------|-------------|
| `GCP_PROJECT_ID` | ID del proyecto de Google Cloud (almacenar en Colab Secrets como `project_gcp`) |
| `AIRFLOW_HOME` | `/content/airflow` (configurado automáticamente en el notebook) |

---

## Calidad de datos

Se validan **todas las tablas** del modelo con Great Expectations antes de la carga:

- No nulos en PKs y campos obligatorios.
- Unicidad en identificadores (`canton_id`, `id`, `anio`).
- Rangos válidos: meses 1–12, días 1–31, `uhc_score` 0–100, dosis y muertes ≥ 0.
- Conteo mínimo de filas > 0 en cada tabla.

Si alguna expectativa falla, la tarea `quality_checks` lanza una excepción y **detiene el pipeline**, evitando cargar datos corruptos.

---

## .gitignore recomendado

```gitignore
# Datos temporales del pipeline
*.csv
*.db

# Credenciales y secretos
*.json
service_account*.json

# Entornos virtuales
venv/
.env

# Artefactos de Jupyter
.ipynb_checkpoints/
__pycache__/

# Logs de Airflow
logs/
*.log

# Kafka binarios
kafka_2.13-*/
cloudflared
```

---

## Generación de valor

| Proceso | Mecanismo | Valor generado |
|---------|-----------|---------------|
| **Operacional** | Stream Kafka en tiempo real | Monitoreo continuo de avance de vacunación y decesos; permite reacción inmediata ante anomalías |
| **Analítico** | Modelo dimensional en SQLite + Airflow DAG | Análisis histórico consolidado; dashboards estáticos para toma de decisiones estratégicas en salud pública |
