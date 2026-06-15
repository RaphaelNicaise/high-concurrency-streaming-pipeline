# 📊 TapDrink Chaos Dashboard (Frontend)

Este módulo contiene la interfaz gráfica de usuario (UI) desarrollada en **React + Vite** y empaquetada con **Docker**. Su objetivo principal es actuar como la cabina de control y el centro de observabilidad de todo nuestro pipeline de datos en alta concurrencia.

Para optimizar el rendimiento y evitar la complejidad innecesaria de WebSockets bajo estrés, el dashboard implementa una estrategia de **Short Polling HTTP asíncrono**, desacoplando la renderización visual de la carga de procesamiento masivo de Spark.

---

## 🏗️ Arquitectura de la Interfaz

La aplicación se estructura en tres capas visuales y operativas bien definidas:

+------------------------------------------------------------------------+
|                      [ Capa 1: Panel de Control ]                      |
|           (Interruptor General / Selector de Modo de Estrés)            |
+------------------------------------------------------------------------+
│
▼  (Peticiones POST / HTTP)
+------------------------------------------------------------------------+
|                [ Capa 2: React Flow Pipeline Diagram ]                 |
|       (Visualización estática animada del mapa de componentes)          |
+------------------------------------------------------------------------+
▲
│  (Short Polling GET / cada 2s)
+------------------------------------------------------------------------+
|                     [ Capa 3: Live Metrics (Charts) ]                  |
|          (Gráficos de rendimiento en tiempo real: Ev/seg, Errores)     |
+------------------------------------------------------------------------+


### 1. Panel de Control (Interacción con el Simulador)
El usuario puede enviar comandos directos al backend a través de componentes interactivos (Toggles y Botones):
* **Interruptor General (ON/OFF):** Activa o pausa por completo el `chaos-simulator` de Python.
* **Modo Ráfaga (Flash Sale Mode):** Al presionarlo, envía una señal para que el simulador multiplique por 10 o 20 la inyección de eventos, simulando un pico masivo de venta de tickets para Tap Pass.
* **Inyector de Corrupción:** Fuerza al simulador a subir temporalmente la tasa de datos corruptos (campos nulos, JSONs rotos) del 5% al 40% para ver cómo reacciona la capa de Data Quality en tiempo real.

### 2. Mapa Interactivo del Pipeline (React Flow)
Utilizando la librería **React Flow**, se renderiza un grafo estático dirigido que representa la infraestructura de contenedores del proyecto:
`[Simulator] ──> [FastAPI] ──> [Redis] ──> [Spark Streaming] ──> [PostgreSQL / MinIO]`

* **Animación Dinámica:** Las líneas de conexión (*edges*) cuentan con una animación de partículas en movimiento. Si el simulador está apagado, la animación se detiene; si entra en modo ráfaga, la velocidad de las partículas aumenta.
* **Indicadores de Salud:** Cada nodo del grafo cambia de color según el estado del servicio reportado por los chequeos de salud de FastAPI (`Healthy` = Verde, `Unhealthy` = Rojo).

### 3. Métricas en Tiempo Real (Recharts / Tremor)
Gráficos estadísticos que se actualizan automáticamente cada **2 segundos** mediante un temporizador asíncrono (`setInterval`):
* **Gráfico de Líneas (Throughput):** Muestra los eventos por segundo ingresando a la API frente a los eventos procesados por Spark.
* **Gráfico de Barras (Métricas de Conversión):** Métricas analíticas de negocio puras derivadas de TapDrink (Vistas de productos vs. Adiciones al carrito vs. Compras completadas).
* **Contador de Errores (Dead Letter Queue):** Un KPI que destaca cuántos eventos con datos corruptos fueron interceptados y aislados por el pipeline.

---

## 🔄 Flujo de Datos Técnico (Detrás de Escena)

Para mantener el sistema liviano y escalable, el flujo de información sigue un ciclo estrictamente síncrono por debajo de la interfaz visual:

1. **El Cambio de Estado (Acción del Usuario):**
   * El usuario activa el "Modo Ráfaga" en el Dashboard.
   * El frontend realiza una petición asíncrona: `POST http://localhost:8000/api/simulator/config` enviando el payload `{ "mode": "burst" }`.
   * FastAPI recibe el golpe y actualiza instantáneamente una clave en la memoria de **Redis**: `SET simulator:config "burst"`.
   * El script de Python (`chaos_simulator.py`), que consulta periódicamente esa clave de Redis, detecta el cambio e incrementa drásticamente la creación de hilos asíncronos (`aiohttp`).

2. **La Actualización de Pantalla (Métricas en Vivo):**
   * Apache Spark procesa las ráfagas en ventanas de tiempo y guarda los contadores globales agregados directamente en claves hash de **Redis** (ej. `metrics:throughput_current`).
   * Cada 2 segundos, el componente de React ejecuta de fondo: `GET http://localhost:8000/api/metrics`.
   * FastAPI responde en menos de 2 milisegundos leyendo los datos pre-calculados directamente desde la memoria de Redis, sin tocar el disco duro ni saturar a PostgreSQL.
   * El estado de React se actualiza y los gráficos se redibujan suavemente ante los ojos del usuario.

---

## 🛠️ Stack Tecnológico del Frontend
* **Core:** React 18+ (Functional Components & Hooks).
* **Build Tool:** Vite (para compilación e inicio ultra veloz).
* **Gestión de Gráficos:** Recharts / Tremor (diseñado para layouts de analítica).
* **Diagramación:** React Flow (Nodos y conexiones personalizadas).
* **Estilos:** Tailwind CSS.
* **Contenerización:** Dockerfile multi-stage basado en Node.js para desarrollo y Nginx Alpine para servir los estáticos en el VPS de producción.
