# RutaSur — TP7 Programación Avanzada 2026

Base de implementación educativa orientada a eventos basada en el PDF de 13 páginas. **No se ha probado de punta a punta con Docker en este entorno**: seguir pruebas manuales y corregir defectos antes de entregar.

## Prerrequisitos

- Docker Desktop con Docker Compose v2 y 6 GB+ de RAM disponibles.
- Opcional: Node.js 22 para verificaciones de sintaxis.
- Clonar/extraer esta carpeta y trabajar en su raíz.

## Puesta en marcha

```powershell
docker compose up --build -d
docker compose ps
docker compose logs -f simulator
```

Abrir http://localhost:3000 y http://localhost:3000/demo. Una vez arrancado, el simulador intenta generar 20 envíos/min.

## Verificar por pasos

1. **Infraestructura**: `docker compose ps`; verificar Kafka, Mongo y los seis procesos de aplicación más la web. Kafka: `docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list`.
2. **Envío feliz**: en `/demo` pulsar `feliz`; abrir el detalle/tracking con el ID devuelto. Confirmar cinco estados y `ENTREGADO`.
3. **Idempotencia y secuencia**: revisar logs con `docker compose logs proyector`; el simulador inyecta 2% de duplicados y 2% fuera de orden. Validar historial sin entradas extra y `ultimaSecuencia` no decreciente.
4. **DLQ**: pulsar `invalido`. Consultar el topic: `docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic envios.estados.dlq --from-beginning --max-messages 1`.
5. **Entrega fallida**: pulsar `ausente`. Debe aparecer una notificación y una acción `REPROGRAMAR` al siguiente día hábil y franja contraria.
6. **Tercer fallo**: pulsar `tercer-fallo`. Debe aparecer `DEVOLVER` en reprogramación.
7. **SLA**: pulsar `demorado`. Esperar aprox. 1 minuto real (factor 120) y comprobar `SIN_SALIDA` de prioridad ALTA. Para las otras reglas preparar datos con vencimientos adecuados.
8. **Aislamiento**: `docker compose stop notificaciones`; crear evento fallido, comprobar el resto; `docker compose start notificaciones` y observar recuperación.
9. **Analítica**: visitar `/analitica` y filtrar Z1/Z2/Z3. Recontar una semilla fija si se usa esta funcionalidad para evaluación.
10. **Replay**: ejecutar `docker compose run --rm --no-deps analitica node src/replay.js` con broker y Mongo accesibles. El grupo es nuevo cada ejecución y rellena `analitica_tiempos` con históricos.
11. **Latencia**: medir tiempo desde publicación de `envios.estados` hasta su aparición en pantalla (objetivo <2 segundos).

## Organización de archivos

- `docker-compose.yml`: servicios, infraestructura y redes.
- `Dockerfile`: contenedor común Node.js.
- `package.json`: dependencias de KafkaJS, MongoDB, Zod.
- `.env.example`: referencia de variables; los valores de Compose están definidos en el YAML.
- `.gitignore`: archivos que no se versionan.
- `src/core.js`: productor, consumidor, esquema, DLQ, DB e índices.
- `src/topics.js`: creación y retención de los seis topics.
- `src/simulator.js`: generador reproducible y escenarios.
- `src/service.js`: ejecutable multipropósito para cinco procesos separados, uno por `ROLE`.
- `src/replay.js`: nuevo grupo consumidor para el cuarto indicador.
- `web/package.json`, `web/Dockerfile`, `web/.dockerignore`: frontend y su contenedor.
- `web/app/lib.js`: consultas MongoDB.
- `web/app/layout.js`, `web/app/style.css`: layout y estilo.
- `web/app/page.js`: torre de control.
- `web/app/envios/[id]/page.js`: detalle supervisor.
- `web/app/tracking/[id]/page.js`: tracking público.
- `web/app/analitica/page.js`: tabla de indicadores.
- `web/app/demo/page.js`, `web/app/demo/ui.js`: interfaz y botones para escenarios.
- `web/app/api/stream/route.js`: MongoDB change streams → SSE.
- `web/app/api/scenarios/route.js`: control de simulación local.
- `web/app/components/Live.js`: cliente SSE.

## Decisiones a documentar ante el profesor

- Servicios se comunican en la ruta de negocio mediante Kafka. El endpoint del simulador es excepcional, **solo para control de la demo**. La consigna estricta dice que Next.js solo lee de MongoDB; si se aplica sin excepciones, se debe reemplazar ese mecanismo por una ejecución de escenarios desde CLI, fuera de la interfaz.
- Interpretación propuesta: `ZONA_INACCESIBLE` resulta en `DEVOLVER`; el PDF no define su reacción explícita. Se puede cambiar tras consultar al docente.
- `SIN_INGRESO` y `SIN_SALIDA` dependen de ventanas sobre reloj simulado. El valor de aceleración predeterminado es `120` (1 hora simulada = 30 segundos).
- Esta base no incluye un estado formal `DEVUELTO`, porque no aparece en la lista de estados requerida; la devolución se expresa como acción de reprogramación.

## Limitaciones técnicas pendientes para considerar una entrega completa

- **Publicación atómica**: la tabla `salidas_pendientes` evita normalmente emisiones repetidas, pero puede perder una emisión si el proceso cae después de marcarla y antes de publicar; sustituir por patrón transactional outbox con publicador/reintentos y eventId determinístico.
- **SLA**: falta restaurar completamente anclaje de reloj simulado y evitar regresiones al procesar eventos antiguos; en reinicios, el monitor debe rehidratar el reloj y la emisión pendiente.
- **Analítica**: el cumplimiento SLA todavía no está calculado correctamente con las promesas; hace falta correlacionarlas y calcular explícitamente entregas en plazo / entregas totales.
- **Reprocesamiento**: el cuarto indicador está en colección aparte; falta manejar de manera robusta cambios de estado y timestamps fuera de orden y detectar el momento en que el consumidor alcanzó el final del histórico.
- **Seguridad**: la ruta de demo no tiene autenticación de usuario y NO debe publicarse en internet. Autenticación está expresamente fuera de alcance del TP. Restringir acceso local, validar entradas y añadir control de acceso si se despliega.
- **Control de latencia**: no existe test automatizado que certifique menos de 2 segundos.
- **Simulador**: usar `SIM_SEED` garantiza el PRNG de escenarios pero IDs aleatorios y tiempo de creación cambian, por lo que aún no garantiza exactamente la misma corrida. Falta homogeneizar timestamp e IDs determinísticos.
- **Reintentos**: el sistema registra acciones de reprogramación, pero todavía no genera automáticamente estados de un segundo intento a partir de ellas.
- **Métricas**: falta una tasa completa de entrega en primer intento por envío, por ejemplo contabilizando todos los resultados finales en una colección auxiliar.

## OWASP 2025

OWASP 2025 es una referencia de riesgos de seguridad, no una lista de requisitos explícitos del PDF. Uso propuesto:
- A01 Acceso roto: no exponer `/demo` ni detalles internos sin protección.
- A02 Configuración insegura: variables de entorno y red local.
- A03 Cadena de suministro: lockfiles, verificación de dependencias y escaneo.
- A05 Inyección: validar IDs y eventos con Zod.
- A08 Integridad: eventId, secuencia, idempotencia y control de schema.
- A09 Logging y alertas: logs con eventId, offset y partición.
- A10 Excepciones: DLQ para eventos inválidos y errores de operación visibles.

Referencia: https://top10.owasp.org/2025/0x00_2025-Introduction/
