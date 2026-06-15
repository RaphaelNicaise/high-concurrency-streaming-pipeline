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

```bash
docker compose --profile load-test up -d chaos-simulator
```

para borrar toda la información persistente (volúmenes) y empezar de cero:
```bash
docker compose down -v
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
