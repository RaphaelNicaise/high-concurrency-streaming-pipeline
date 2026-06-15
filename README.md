# high-concurrency-streaming-pipeline

# Plataforma de Telemetría y Streaming en Tiempo Real

## El Objetivo
Diseñar, desplegar y orquestar una arquitectura de datos end-to-end capaz de procesar telemetría en tiempo real y alta concurrencia para un ecosistema de catálogos digitales y ticketing (TapDrink). 

El sistema debe ser capaz de absorber picos masivos de tráfico (simulando eventos de venta de entradas), procesar métricas de negocio en vivo (vistas vs. compras) y asegurar la integridad de los datos mediante validaciones automatizadas, implementando un paradigma ELT con almacenamiento inmutable.

## La Arquitectura (Qué vamos a hacer)
El proyecto se divide en 6 componentes interconectados, diseñados para correr en contenedores de forma aislada:

1.  **Generador de Carga (Chaos Simulator):** Un script en Python que inyecta miles de eventos por segundo (JSONs) simulando navegación de usuarios, carritos abandonados, compras de tickets y eventos malformados para estresar el sistema.
2.  **Capa de Ingesta y Rate Limiting:** Una API (FastAPI) que recibe el tráfico y utiliza **Redis** (Pub/Sub o Streams) como un buffer/amortiguador para evitar que la base de datos colapse durante los picos de tráfico.
3.  **Data Lake y Almacenamiento Crudo (Landing / Bronze):** Despliegue de **MinIO** (almacenamiento de objetos compatible con AWS S3) para guardar los eventos JSON originales de forma inmutable, protegiendo los datos frente a fallos de procesamiento.
4.  **Procesamiento de Streaming (Hot Path):** Un clúster de **Apache Spark (PySpark)** estructurado para consumir la cola de Redis, agregar métricas en ventanas de 5 segundos, filtrar el fraude y separar los datos corruptos.
5.  **Almacenamiento Indexado:** **PostgreSQL** configurado con particionamiento lógico para recibir los datos procesados en tiempo real y servir dashboards de analítica.
6.  **Orquestación y Batch (Cold Path):** **Prefect / Apache Airflow** orquestando tareas nocturnas programadas que toman el histórico crudo de MinIO, lo validan, lo transforman a formato Parquet (Silver) y compactan las tablas finales (Gold).

## Conceptos Core Trabajados
* **Desacoplamiento de Arquitectura:** Uso de Redis como capa de mensajería para separar la ingesta del procesamiento.
* **Paradigma ELT y Arquitectura de Medallón:** Almacenamiento inmutable de la fuente original (Capa Bronze), transformación a formatos columnares eficientes (Capa Silver) y consolidación de métricas de negocio (Capa Gold).
* **Idempotencia y Reprocesamiento (Backfilling):** Garantizar que fallas en los nodos no generen duplicados y permitir la reconstrucción del historial completo de PostgreSQL leyendo desde el Data Lake.
* **Data Quality (Calidad de Datos):** Implementación de reglas (Data Contracts/Great Expectations) para capturar y aislar *bad data* sin detener el flujo de streaming.
* **Windowing (Agrupación por Ventanas de Tiempo):** Agrupación de micro-lotes en Spark para calcular métricas de conversión en vivo.
* **Infraestructura como Código (IaC) y Contenerización:** Todo el ecosistema paquetizado con **Docker / Docker Compose** para replicar el entorno de producción localmente o en un VPS.

## Roadmap de Ejecución (Paso a paso)

### Fase 1: Simulación e Ingesta
- [ ] Desarrollar el script generador de telemetría (faker/Python).
- [ ] Levantar contenedor de Redis.
- [ ] Crear el endpoint de FastAPI que empuja los eventos a la cola de Redis.

### Fase 2: Data Lake Crudo (Paradigma ELT)
- [ ] Levantar contenedor de MinIO.
- [ ] Implementar un proceso (consumer) que lea eventos crudos de Redis y los persista inmutables en un bucket `bronze` de MinIO.

### Fase 3: Procesamiento en Tiempo Real
- [ ] Configurar el contenedor de Apache Spark.
- [ ] Escribir el job de PySpark que lea los streams en caliente.
- [ ] Implementar la lógica de limpieza y agregación de ventanas temporales.

### Fase 4: Almacenamiento y Modelo Analítico
- [ ] Levantar contenedor de PostgreSQL.
- [ ] Diseñar el esquema de tablas (Modelo de Estrella o Tablas Anchas indexadas).
- [ ] Conectar la salida de Spark para escribir en las tablas correspondientes.

### Fase 5: Orquestación y Resiliencia
- [ ] Integrar Prefect / Airflow mediante Docker.
- [ ] Crear el DAG nocturno que tome los datos de la capa `bronze` en MinIO, los convierta a Parquet (`silver`) y actualice las métricas consolidadas.
- [ ] Implementar lógica de reintentos (*retries*) frente a caídas de infraestructura.

### Fase 6: CI/CD y Despliegue
- [ ] Configurar GitHub Actions.
- [ ] Empaquetar las imágenes finales de Python/FastAPI.
- [ ] Hacer un MakeFile (?)

---

## Cómo Levantar el Proyecto

### Requisitos Previos
- [Docker](https://docs.docker.com/get-docker/) (v24+ recomendado)
- [Docker Compose](https://docs.docker.com/compose/install/) (v2+ — viene incluido con Docker Desktop)
- Al menos **8 GB de RAM** disponibles para los contenedores (Spark es el más demandante)

### 1. Clonar el repositorio
```bash
git clone https://github.com/RaphaelNicaise/high-concurrency-streaming-pipeline.git
cd high-concurrency-streaming-pipeline
```

### 2. Configurar variables de entorno
El archivo `.env` ya viene con valores por defecto listos para desarrollo local. Revisalo y ajustá las credenciales si lo necesitás:
```bash
cat .env
```

### 3. Levantar todos los servicios
```bash
# Levanta la infra completa (Redis, MinIO, Spark, PostgreSQL, Prefect)
docker compose up -d
```
> **Nota:** La primera ejecución descarga las imágenes (~3-4 GB). Los servicios arrancan en orden gracias a los healthchecks configurados.

### 4. Verificar que todo esté corriendo
```bash
docker compose ps
```
Todos los servicios deberían mostrar estado `running (healthy)`.

### 5. Lanzar el test de carga (opcional)
El Chaos Simulator está bajo un profile separado para no arrancar por defecto:
```bash
docker compose --profile load-test up -d chaos-simulator
```

### Interfaces Web

| Servicio | URL | Credenciales |
|----------|-----|-------------|
| MinIO Console | [http://localhost:9001](http://localhost:9001) | `tapdrink` / `tapdrink_minio_secret` |
| Spark Master UI | [http://localhost:8080](http://localhost:8080) | — |
| Prefect Dashboard | [http://localhost:4200](http://localhost:4200) | — |
| Ingest API (FastAPI docs) | [http://localhost:8000/docs](http://localhost:8000/docs) | — |

### Detener y limpiar
```bash
# Detener todos los contenedores
docker compose --profile load-test down

# Detener y borrar volúmenes (⚠️ elimina todos los datos persistidos)
docker compose --profile load-test down -v
```