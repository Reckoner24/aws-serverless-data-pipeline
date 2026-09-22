# Galactic Services Data Pipeline — Documentación

## Visión general

Pipeline de datos que extrae información de personajes de Star Wars desde la API pública SWAPI,
la almacena en S3 en formato raw (bronze) y la transforma en un CSV limpio (silver).

```mermaid
flowchart LR
    SWAPI["swapi.info/api/people"]
    Lambda["Lambda<br>handler-ingest_swapi"]
    Raw["S3 raw/<br>bronze_crew_data_TS.json"]
    Glue["Glue Job<br>transform_crew_data"]
    Processed["S3 processed/<br>silver_crew_data_TS.csv"]
    SWAPI -->|"HTTP GET"| Lambda
    Lambda -->|"PutObject JSON"| Raw
    Raw -->|"lee más reciente"| Glue
    Glue -->|"escribe CSV"| Processed
```

## Archivo 1 — `src/lambda/ingest_swapi.py`

### ¿Qué hace?

Esta Lambda es el **punto de entrada del pipeline**. Se ejecuta a demanda y se encarga de:
1. Llamar a la API de SWAPI para obtener los personajes
2. Guardar la respuesta como JSON en S3 en la carpeta `raw/`

### Flujo interno

```mermaid
flowchart TD
    A([lambda_handler se invoca]) --> B[fetch_swapi_data<br>GET swapi.info/api/people]
    B --> C{¿Respuesta OK?}
    C -->|Sí| D[Genera timestamp UTC<br>'20260504_172246']
    C -->|Error| Z([Retorna error])
    D --> E[Construye nombre de archivo<br>raw/bronze_PREFIX_TIMESTAMP.json]
    E --> F[s3.put_object<br>Sube JSON al bucket]
    F --> G["/Opcional/ start_job_run<br>actualmente comentado"]
    G --> H([Retorna statusCode 200<br>message + s3_key])
```

### Variables de entorno requeridas

| Variable | Descripción | Ejemplo |
|---|---|---|
| `S3_BUCKET` | Nombre del bucket destino | `mi-bucket-datos` |
| `RAW_PREFIX` | Prefijo para el nombre del archivo | `crew_data` |

### Resultado en S3

```
s3://mi-bucket-datos/
└── raw/
    └── bronze_crew_data_20260504_172246.json   ← lista completa de personajes
```

---

## Archivo 2 — `src/glue/transform_crew_data.py`

### ¿Qué hace?

Este Glue Job es el **motor de transformación**. Toma el JSON más reciente de `raw/`,
aplica 6 transformaciones y guarda el resultado como un CSV limpio en `processed/`.

### Parámetros del Job

| Parámetro | Descripción | Ejemplo |
|---|---|---|
| `--JOB_NAME` | Lo inyecta Glue automáticamente | `transform_crew_data` |
| `--S3_BUCKET` | Bucket con los prefijos `raw/` y `processed/` | `mi-bucket-datos` |

### Flujo interno

```mermaid
flowchart TD
    A([Inicio del Job<br>PySpark + GlueContext]) --> B[list_objects_v2 en S3 raw/<br>filtrar .json → max LastModified]
    B --> C[spark.read.json<br>lee el archivo más reciente]
    C --> T1[1. Seleccionar 8 campos<br>name, height, mass, hair_color,<br>skin_color, eye_color, birth_year, gender]
    T1 --> T2[2. normalized_birth_year<br>'19BBY' → 2000-19 = 1981<br>Sin BBY → None]
    T2 --> T3[3. mass_lb<br>mass kg × 2.20462<br>'unknown' → None]
    T3 --> T4[4. gender_id<br>male→M / female→F / otro→N]
    T4 --> T5[5. Filtro de masa<br>Elimina filas donde mass > 1000 kg]
    T5 --> T6[6. Filtro de calidad<br>Elimina filas con 3+ campos<br>vacíos / unknown / n/a]
    T6 --> W[coalesce 1 → escribe CSV<br>en S3 tmp/ como carpeta]
    W --> R[boto3 localiza el part-file<br>y lo copia a processed/<br>silver_crew_data_TS.csv]
    R --> D[Elimina carpeta tmp/]
    D --> E([job.commit])
```

### Funciones auxiliares

| Función | Propósito |
|---|---|
| `parse_birth_year(by)` | Convierte "19BBY" → "1981", retorna None si no aplica |
| `to_mass_lb(mass_str)` | Convierte kg a libras, maneja "unknown" |
| `to_gender_id(g)` | Mapea género a M/F/N |
| `mass_numeric(mass_str)` | Convierte string de masa a número para filtrar |
| `is_blank(val)` | Detecta valores vacíos, "unknown", "n/a", "null" |

### Resultado en S3

```
s3://mi-bucket-datos/
└── processed/
    └── silver_crew_data_20260504_172246.csv   ← mismo timestamp que el bronze
```

---

## Propuestas de Automatización Diaria

### Opción A — Lambda puro (encadenamiento directo)

La Lambda llama directamente al Glue Job al terminar su trabajo.

```mermaid
flowchart LR
    EB["EventBridge<br>cron 0 6 * * ? *<br>diario 6am UTC"]
    Lambda["Lambda<br>1. GET SWAPI<br>2. PUT S3 raw/<br>3. start_job_run"]
    Glue["Glue Job<br>1. Lee raw/<br>2. Transforma<br>3. CSV → silver/"]

    EB -->|"dispara"| Lambda
    Lambda -->|"fire & forget"| Glue
```

**Implementación:** Solo descomentar esta línea en la Lambda:

```python
boto3.client("glue").start_job_run(JobName="transform_crew_data")
```

Y agregar permiso `glue:StartJobRun` al rol IAM de la Lambda.

**Ventajas:**
- Simple de implementar — solo una línea de código
- Menos servicios que configurar
- Sin costo adicional de orquestación

**Desventajas:**
- La Lambda no espera a que termine el Glue Job (dispara y olvida)
- Si el Glue Job falla, la Lambda no se entera — no hay reintento automático
- Difícil de monitorear el pipeline completo como una unidad

---

### Opción B — AWS Step Functions (orquestación)

Step Functions actúa como director de orquesta: coordina la Lambda y el Glue Job,
espera a que cada paso termine antes de continuar y maneja errores automáticamente.

```mermaid
flowchart TD
    EB["EventBridge<br>cron 0 6 * * ? *"]
    SF["AWS Step Functions<br>State Machine"]
    L["Step 1: Lambda<br>handler-ingest_swapi"]
    G["Step 2: Glue Job<br>transform_crew_data<br>.sync — espera que termine"]
    OK(["END — Pipeline exitoso"])
    ERR(["END — Fallo notificado"])

    EB -->|"dispara"| SF
    SF --> L
    L -->|"éxito"| G
    L -->|"error"| ERR
    G -->|"SUCCEEDED"| OK
    G -->|"FAILED"| ERR
```

**Implementación:** Se define la State Machine en JSON en la consola de AWS:

```json
{
  "StartAt": "IngestData",
  "States": {
    "IngestData": {
      "Type": "Task",
      "Resource": "arn:aws:states:::lambda:invoke",
      "Parameters": { "FunctionName": "handler-ingest_swapi" },
      "Next": "TransformData",
      "Catch": [{ "ErrorEquals": ["States.ALL"], "Next": "JobFailed" }]
    },
    "TransformData": {
      "Type": "Task",
      "Resource": "arn:aws:states:::glue:startJobRun.sync",
      "Parameters": { "JobName": "transform_crew_data" },
      "End": true,
      "Catch": [{ "ErrorEquals": ["States.ALL"], "Next": "JobFailed" }]
    },
    "JobFailed": {
      "Type": "Fail",
      "Error": "PipelineError",
      "Cause": "Un paso del pipeline falló"
    }
  }
}
```

**Ventajas:**
- Espera a que cada paso termine antes de avanzar (`.sync`)
- Reintento automático configurable por paso
- Historial visual de cada ejecución en la consola de AWS
- Fácil agregar pasos futuros (notificaciones, validaciones, etc.)
- Manejo de errores centralizado

**Desventajas:**
- Más configuración inicial
- Costo adicional por transición de estados (~$0.025 por 1,000 transiciones)
- Requiere un rol IAM adicional para Step Functions

---

## Comparativa

| Criterio | Lambda puro | Step Functions |
|---|---|---|
| Complejidad de setup | Baja | Media |
| Costo | Solo Lambda + Glue | Lambda + Glue + Step Functions |
| Manejo de errores | Manual | Automático |
| Visibilidad del pipeline | Baja | Alta (consola visual) |
| Espera entre pasos | No (fire & forget) | Sí (sincronizado) |
| Escalabilidad futura | Limitada | Alta |
| **Recomendación** | Demos / proyectos simples | Producción |

---

## Estructura del bucket S3

```
s3://mi-bucket-datos/
├── raw/
│   └── bronze_crew_data_20260504_172246.json   ← salida de Lambda
├── processed/
│   └── silver_crew_data_20260504_172246.csv    ← salida de Glue
└── scripts/
    └── transform_crew_data.py       ← script usado por Glue
```

## Implementación con IaC:
Se puede utilizar CloudFormation y crear una plantilla con todo lo que se necesita...