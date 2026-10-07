# Guion del video demo (5–8 min)

| Tiempo | Qué mostrar | Qué decir |
|---|---|---|
| 0:00–0:45 | README (diagrama) | El caso del club en 2 minutos: 3 fuentes, 3 preguntas de negocio. |
| 0:45–1:30 | Terminal: `docker compose ps` | Todo el entorno levanta con un comando: Airflow, MinIO, SFTP, MySQL, mock de la API. |
| 1:30–3:30 | Airflow → `deporte_ingesta_diaria` → Graph | TaskGroups, mapping por competición y por archivo, sensor en *reschedule*, trigger rules. Disparar el DAG y abrir el log de `extraer_partidos` (tramos de 10 días) y de `procesar_archivo_gps` (filas a cuarentena). |
| 3:30–4:00 | Consola MinIO (localhost:9001) | `deporte-raw`: `api/`, `gps/crudo/`, `mysql/`, `cuarentena/`. |
| 4:00–5:00 | Airflow → `deporte_transformacion_dbt` | Cosmos: cada modelo + sus tests como tareas. Mostrar una tarea de test en verde. |
| 5:00–6:00 | Snowsight | `select * from MARTS.MART_RIESGO_LESION order by acwr desc;` y `FCT_RENDIMIENTO_PARTIDO`. Una consulta de `analyses/consultas_negocio.sql`. |
| 6:00–7:30 | GitHub | Un Pull Request con los 4 checks de CI en verde, la Branch Protection Rule de `main` y el historial de commits. |
| 7:30–8:00 | — | Cierre: qué aprendimos y qué haríamos en producción (Airflow gestionado, alertas a Slack, OpenMetadata). |

Tip: para mostrar el manejo de errores, agregar `MOCK_API_TOKEN=otro-token`
al `.env`, ejecutar `docker compose up -d mock-api` y disparar el DAG: la tarea
`extraer_partidos` falla **sin reintentar** con el mensaje "La API rechazó el
token". Quitar la línea y repetir `docker compose up -d mock-api` para volver.
