# Preparación de Datos: Pipeline KDD COVID-19 Ciudad de México

**Unidad II — Preparación de los Datos**
**Caso de estudio: Datos Abiertos COVID-19 SSA México**
**Autor:** Fortino Romero Mantilla
**Institución:** Universidad Tecnológica de Puebla
**Fecha:** Junio 2026

---

## Introducción

El presente documento describe el proceso completo de preparación de datos llevado a cabo sobre los registros epidemiológicos de COVID-19 publicados por la Dirección General de Epidemiología de la Secretaría de Salud de México (SSA). El caso de estudio se centra en la Ciudad de México (CDMX, entidad federativa clave 09) y abarca los años 2020 a 2026.

El objetivo del pipeline es transformar siete archivos CSV crudos, con un total de aproximadamente 20.9 millones de registros a nivel nacional, en un conjunto de datos limpio, estandarizado y enriquecido, listo para análisis estadístico y minería de datos. El resultado final es un archivo CSV de 7,840,211 registros correspondientes a la CDMX, con todas las variables codificadas sustituidas por sus descripciones del catálogo oficial.

El pipeline está implementado en Python como una serie de Jupyter Notebooks numerados secuencialmente, respaldados por una base de datos MongoDB ejecutada en un contenedor Docker y el formato columnar Parquet para el procesamiento analítico.

---

## Entorno de Trabajo

### Decisión de Arquitectura: Docker en lugar de MongoDB Atlas

Durante el desarrollo inicial se evaluó el uso de MongoDB Atlas (servicio cloud de MongoDB) como almacén de datos. Sin embargo, el volumen de registros nacionales (~20.9 millones de documentos) superó el límite de almacenamiento de la capa gratuita de Atlas, lo que provocó que la plataforma bloqueara la carga. Ante esta restricción, se optó por desplegar MongoDB localmente dentro de un contenedor Docker con un volumen persistente en disco, lo que elimina límites artificiales de almacenamiento y mantiene los datos bajo control local durante el desarrollo académico.

### Docker y MongoDB

MongoDB 8.0 se ejecuta mediante Docker Compose. La configuración define un servicio único con autenticación por archivo de secreto, un volumen nombrado para persistencia y exposición únicamente en la interfaz local (`127.0.0.1:27017`) para evitar exposición en red.

```yaml
services:
  mongodb:
    image: mongo:8.0
    container_name: mongodb-covid
    restart: unless-stopped
    environment:
      MONGO_INITDB_ROOT_USERNAME: admin
      MONGO_INITDB_ROOT_PASSWORD_FILE: /run/secrets/mongodb-root-password
    secrets:
      - mongodb-root-password
    volumes:
      - mongodb_data:/data/db
    ports:
      - "127.0.0.1:27017:27017"

volumes:
  mongodb_data:

secrets:
  mongodb-root-password:
    file: ./mongodb_root_password.txt
```

La conexión desde Python se gestiona a través del módulo `src/database.py`, que lee las credenciales desde variables de entorno definidas en un archivo `.env`:

```python
def get_database():
    client = MongoClient(os.getenv("MONGO_URI"))
    return client[os.getenv("DB_NAME")]
```

Esto permite cambiar el destino de la base de datos sin modificar el código de los notebooks.

### uv — Gestión de Dependencias

El proyecto utiliza `uv` como gestor de entornos y paquetes de Python. `uv` es una herramienta moderna escrita en Rust que reemplaza a `pip` + `venv`, con instalación de dependencias significativamente más rápida y resolución de versiones determinista. El proyecto se declara como paquete instalable mediante `pyproject.toml`, lo que permite importar el módulo `src` desde cualquier notebook sin manipulación manual del `sys.path`.

```toml
[project]
name = "covid19"
version = "0.1.0"
description = "Covid19 Knowledge Discovery from Data"
requires-python = ">=3.12"
dependencies = [
    "polars>=1.41.0",
    "pymongo>=4.17.0",
    "pandas>=3.0.3",
    "pyarrow>=24.0.0",
    "matplotlib>=3.10.9",
    "requests>=2.34.2",
    "openpyxl>=3.1.5",
    "ipykernel>=7.2.0",
    "dotenv>=0.9.9",
]
```

### Polars en lugar de Pandas

Para el procesamiento analítico de datos se utiliza **Polars** en lugar de Pandas. La decisión se basa en tres factores:

**Rendimiento:** Polars está implementado en Rust y utiliza procesamiento paralelo de manera nativa. Sobre los ~7.8 millones de registros del dataset CDMX, las operaciones de transformación son sensiblemente más rápidas que con Pandas.

**API Lazy:** Polars ofrece un modo de evaluación diferida (`scan_parquet`, `LazyFrame`) que permite construir pipelines de transformación completos antes de ejecutarlos. Esto permite al motor optimizar el plan de ejecución y evitar cargar columnas innecesarias en memoria, fundamental cuando se trabaja con archivos Parquet de más de 60 MB.

**Compatibilidad con Parquet:** La integración de Polars con Apache Arrow y el formato Parquet es directa y eficiente, incluyendo escritura en streaming (`sink_parquet`, `sink_csv`) sin necesidad de cargar el DataFrame completo en RAM.

Pandas se utiliza exclusivamente donde Polars no tiene soporte nativo: lectura de archivos Excel (catálogos) y la ingesta por chunks desde ZIP durante la carga inicial.

### Estructura del Repositorio

```
covid19/
├── data/
│   ├── DATA_RAW_2020.zip ... DATA_RAW_2026.zip   # Archivos crudos SSA
│   ├── diccionario_datos_abiertos/                # Catálogos oficiales Excel
│   ├── polars/                                    # Archivos Parquet intermedios
│   │   ├── mexico_city_covid_data.parquet
│   │   ├── mexico_city_covid_data_complete.parquet
│   │   ├── mexico_city_covid_data_dates.parquet
│   │   └── mexico_res_nac_um.parquet
│   └── mexico_city_covid_final_report.csv         # Producto final
├── notebooks/
│   ├── 01_download.ipynb
│   ├── 02_ingestion.ipynb
│   ├── 03_discovery.ipynb
│   ├── 04_clean.ipynb
│   ├── 05_mapping.ipynb
│   ├── 06_statistics.ipynb
│   ├── 07_death.ipynb
│   ├── 08_mortality_rate.ipynb
│   └── 09_csv.ipynb
├── src/
│   ├── __init__.py
│   └── database.py
├── docker-compose.yml
├── pyproject.toml
└── .env.example
```

---

## Unidad II: Preparación de los Datos

### A. Esquema del Data Warehouse

El data warehouse de este proyecto adopta una arquitectura en capas (medallion architecture simplificada) implementada sobre MongoDB y Parquet:

**Capa Raw — Colecciones MongoDB por año**

Cada archivo anual se ingesta en su propia colección sin transformación, preservando el esquema original de la SSA:

| Colección | Registros | Esquema |
|---|---|---|
| raw_2020_data | 3,868,396 | 41 campos (incluye RESULTADO_LAB, CLASIFICACION_FINAL) |
| raw_2021_data | 8,830,345 | 41 campos |
| raw_2022_data | 6,451,944 | 41 campos |
| raw_2023_data | 1,222,219 | 41 campos |
| raw_2024_data | 177,618 | 43 campos (nuevas variables PCR e Influenza) |
| raw_2025_data | 157,142 | 43 campos |
| raw_2026_data | 217,324 | 43 campos |

**Capa Unificada — covid_unified_data**

Colección maestra con todos los registros nacionales estandarizados a un esquema común de 44 campos. Incluye el campo calculado `YEAR_ORIGIN` para trazabilidad. Total: ~20.7 millones de documentos.

**Capa CDMX — mexico_city_covid_data**

Esta colección se construyó en dos etapas que reflejan una decisión de integración de datos tomada durante el proyecto.

**Etapa 1 — Filtro inicial por residencia y nacimiento:** Se creó la colección filtrando únicamente los registros donde `ENTIDAD_RES = "09"` (paciente residente de CDMX) o `ENTIDAD_NAC = "09"` (paciente nacido en CDMX). Resultado: 7,299,605 documentos.

**Etapa 2 — Integración de ENTIDAD_UM:** Durante la exploración se identificó que 540,606 registros adicionales corresponden a pacientes atendidos en una unidad médica de la CDMX (`ENTIDAD_UM = "09"`) aunque no residan ni hayan nacido en la ciudad. Como parte del requisito de integración de datos del proyecto, estos registros se incorporaron a la colección. Resultado final: 7,840,211 documentos.

**Capa Analítica — Archivos Parquet**

| Archivo | Descripción | Columnas |
|---|---|---|
| mexico_city_covid_data.parquet | EDAD e HIPERTENSION — colección pre-integración (RES+NAC, 7,299,605 registros) | 2 |
| mexico_res_nac_um.parquet | EDAD e HIPERTENSION — colección post-integración (RES+NAC+UM, 7,840,211 registros) | 2 |
| mexico_city_covid_data_complete.parquet | Todos los campos, fechas como String — exportación completa para análisis | 42 |
| mexico_city_covid_data_dates.parquet | Todos los campos + columnas calculadas de días — estado final para CSV | 45 |

**Producto Final — mexico_city_covid_final_report.csv**

CSV con 7,840,211 registros y todos los códigos de catálogo reemplazados por sus descripciones. Tamaño: 2,936.5 MB (factor 46.8x respecto al Parquet equivalente).

**Esquema del documento final (45 columnas)**

| Columna | Tipo | Descripción |
|---|---|---|
| ASMA | String | Diagnóstico: SI / NO / NO APLICA |
| CARDIOVASCULAR | String | Enfermedad cardiovascular |
| CLASIFICACION_FINAL_COVID | String | Clasificación diagnóstica COVID-19 |
| CLASIFICACION_FINAL_FLU | String | Clasificación diagnóstica Influenza |
| DIABETES | String | Diagnóstico diabetes |
| EDAD | Int64 | Edad en años |
| EMBARAZO | String | Condición de embarazo |
| ENTIDAD_NAC | String | Nombre entidad de nacimiento |
| ENTIDAD_RES | String | Nombre entidad de residencia |
| ENTIDAD_UM | String | Nombre entidad de la unidad médica |
| EPOC | String | EPOC |
| FECHA_DEF | Date | Fecha de defunción (null = vivo) |
| FECHA_INGRESO | Date | Fecha de ingreso al sistema |
| FECHA_SINTOMAS | Date | Fecha de inicio de síntomas |
| HABLA_LENGUA_INDIG | String | Habla lengua indígena |
| HIPERTENSION | String | Diagnóstico hipertensión |
| INDIGENA | String | Se identifica como indígena |
| INMUSUPR | String | Inmunosupresión |
| INTUBADO | String | Requirió intubación |
| MIGRANTE | String | Condición migrante |
| MUNICIPIO | String | Nombre del municipio de residencia |
| NACIONALIDAD | String | MEXICANA / EXTRANJERA |
| NEUMONIA | String | Diagnóstico neumonía |
| OBESIDAD | String | Diagnóstico obesidad |
| ORIGEN | String | USMER / FUERA DE USMER |
| OTRA_COM | String | Otra comorbilidad |
| OTRO_CASO | String | Contacto con otro caso |
| PAIS_NACIONALIDAD | String | País de nacionalidad |
| PAIS_ORIGEN | String | País de origen |
| RENAL_CRONICA | String | Enfermedad renal crónica |
| RESULTADO_ANTIGENO | String | Resultado prueba antígeno |
| RESULTADO_PCR | String | Resultado prueba PCR |
| RESULTADO_PCR_COINFECCION | String | Coinfección identificada por PCR |
| SECTOR | String | Sector de la unidad médica (IMSS, SSA, etc.) |
| SEXO | String | MUJER / HOMBRE |
| TABAQUISMO | String | Diagnóstico tabaquismo |
| TIPO_PACIENTE | String | AMBULATORIO / HOSPITALIZADO |
| TOMA_MUESTRA_ANTIGENO | String | Se tomó muestra de antígeno |
| TOMA_MUESTRA_LAB | String | Se tomó muestra de laboratorio |
| UCI | String | Requirió UCI |
| YEAR_ORIGIN | Int64 | Año del archivo fuente |
| DIAS_SINTOMAS | Int64 | Días entre síntomas e ingreso |
| DIAS_ATENCION | Int64 | Días entre ingreso y defunción |
| DIAS_ENFERMO | Int64 | Días entre síntomas y defunción |

---

### B. Tipos y Fuentes de Datos

**Fuente principal**

Los datos provienen del portal de Datos Abiertos de la Secretaría de Salud de México (SSA), específicamente del Sistema de Vigilancia Epidemiológica de Enfermedades Respiratorias Virales (SISVER). La SSA publica actualizaciones diarias; para este proyecto se descargaron los cortes históricos anuales y el corte vigente a mayo de 2026.

Los archivos se distribuyen como archivos ZIP que contienen un único CSV por año. A partir de 2024, el esquema cambió para incluir vigilancia de Influenza además de COVID-19.

**Fuente secundaria — Catálogos oficiales**

La SSA publica un archivo Excel (`240708 Catalogos.xlsx`) con 13 catálogos que definen el significado de los valores codificados. Se descarga programáticamente junto con el archivo `240708 Descriptores_.xlsx` que describe el formato de cada variable.

**Fuente de referencia — INEGI Censo 2020**

Para el cálculo de tasas de mortalidad se utilizan las cifras de población de la CDMX del Censo de Población y Vivienda 2020 del INEGI:

- Población total CDMX: 9,209,944
- Población masculina: 4,404,927
- Población femenina: 4,805,017

**Tipos de datos en el sistema**

| Tipo | Columnas | Observación |
|---|---|---|
| String (código) | La mayoría de campos clínicos y geográficos | Valores como "1", "2", "97", "99" según catálogo |
| String (texto libre) | PAIS_NACIONALIDAD, PAIS_ORIGEN | Sin codificación |
| Int64 | EDAD, YEAR_ORIGIN, DIAS_* | EDAD originalmente String en algunas colecciones |
| Date | FECHA_DEF, FECHA_INGRESO, FECHA_SINTOMAS | Formato AAAA-MM-DD; FECHA_DEF usa "9999-99-99" para vivos |
| Binary | _id (MongoDB ObjectId) | Se elimina en el CSV final |

---

### C. Técnicas de Limpieza de Datos

**1. Normalización de nombres de archivo**

Los archivos descargados tienen nombres inconsistentes entre años (ej. `COVID19MEXICO2020.zip` vs `datos_abiertos_influenza_covid19_30.12.2025.zip`). Se implementó una función que extrae el año mediante expresión regular y renombra todos los archivos al formato uniforme `DATA_RAW_{YEAR}.zip`.

**2. Estandarización de esquema entre años**

A partir de julio de 2024, la SSA modificó el esquema eliminando `RESULTADO_LAB` y `CLASIFICACION_FINAL`, e introduciendo `RESULTADO_PCR`, `RESULTADO_PCR_COINFECCION`, `CLASIFICACION_FINAL_COVID` y `CLASIFICACION_FINAL_FLU`. Para unificar todos los registros en un esquema común, la función `standardize_document()` aplica las siguientes reglas:

- Para años 2020-2023: mapea `RESULTADO_LAB → RESULTADO_PCR`, asigna `RESULTADO_PCR_COINFECCION = "997"` (no aplica), mapea `CLASIFICACION_FINAL → CLASIFICACION_FINAL_COVID`, asigna `CLASIFICACION_FINAL_FLU = "6"` (caso sospechoso).
- Para años 2024-2026: conserva los campos nuevos directamente.

Adicionalmente, se añade el campo `YEAR_ORIGIN` con el año del archivo fuente para mantener trazabilidad.

**3. Deduplicación por ID_REGISTRO**

Cada registro epidemiológico tiene un identificador único `ID_REGISTRO`. Se creó un índice único sobre este campo en la colección `covid_unified_data` y se utilizó la operación `UpdateOne` con `upsert=True` en lugar de `insert_many`. Esto garantiza que si un mismo registro aparece en múltiples cortes anuales (situación posible en actualizaciones), prevalece la versión más reciente sin duplicar el documento.

**4. Corrección de tipo en columna EDAD**

La columna `EDAD` se ingesta originalmente como String desde los CSV. Tras la creación de la colección `mexico_city_covid_data`, se ejecutó una actualización masiva con un pipeline de agregación MongoDB para convertir el campo a entero:

```python
pipeline = [{"$set": {"EDAD": {"$convert": {"input": "$EDAD", "to": "int", "onError": None, "onNull": None}}}}]
db.mexico_city_covid_data.update_many({}, pipeline)
# Documentos actualizados: 540,606
```

Los 540,606 documentos corresponden a los registros incorporados desde `ENTIDAD_UM = "09"` que mantenían EDAD como String.

**5. Tratamiento de fechas y valores nulos**

La columna `FECHA_DEF` utiliza el valor `"9999-99-99"` para indicar que el paciente no falleció (está vivo al momento del registro). Durante la conversión a formato Parquet, este valor se sustituye por `null` y las tres columnas de fecha se convierten al tipo `Date` nativo de Polars:

```python
processed_df = df_lazy.with_columns([
    pl.col(col).replace("9999-99-99", None).str.to_date("%Y-%m-%d", strict=False)
    for col in ["FECHA_DEF", "FECHA_INGRESO", "FECHA_SINTOMAS"]
])
```

**6. Eliminación de campos innecesarios**

Como parte de los requisitos del proyecto, se eliminaron los campos `ID_REGISTRO` y `FECHA_ACTUALIZACION` de la colección `mexico_city_covid_data` mediante:

```python
db.mexico_city_covid_data.update_many({}, {"$unset": {"ID_REGISTRO": 1, "FECHA_ACTUALIZACION": 1}})
# Documentos modificados: 7,840,210
```

`FECHA_ACTUALIZACION` representa la fecha en que la SSA actualizó el registro, no un dato clínico, por lo que no aporta valor analítico. `ID_REGISTRO` se elimina del documento MongoDB (su función como clave de deduplicación ya fue cumplida).

**7. Enriquecimiento geográfico con ENTIDAD_UM**

La colección inicial `mexico_city_covid_data` se construyó filtrando únicamente por `ENTIDAD_RES = "09"` (residencia) o `ENTIDAD_NAC = "09"` (nacimiento), con 7,299,605 registros. Posteriormente se identificaron 540,606 registros adicionales donde la unidad médica de atención está en CDMX (`ENTIDAD_UM = "09"`) aunque el paciente no resida ni haya nacido en la ciudad. Estos registros se incorporaron a la colección, ampliando el total a 7,840,211.

**8. Generación de variables derivadas de tiempo clínico**

A partir de las fechas convertidas se calcularon tres nuevas columnas que cuantifican la progresión clínica de los pacientes fallecidos:

| Variable | Cálculo | Significado |
|---|---|---|
| DIAS_SINTOMAS | FECHA_INGRESO − FECHA_SINTOMAS | Días desde síntomas hasta atención médica |
| DIAS_ATENCION | FECHA_DEF − FECHA_INGRESO | Días desde ingreso hasta defunción |
| DIAS_ENFERMO | FECHA_DEF − FECHA_SINTOMAS | Días totales de enfermedad hasta defunción |

Estas columnas son `null` para pacientes que no fallecieron. Las estadísticas descriptivas sobre los 94,087 pacientes fallecidos muestran:

| Estadístico | DIAS_SINTOMAS | DIAS_ATENCION | DIAS_ENFERMO |
|---|---|---|---|
| Media | 2.4 días | 9.2 días | 14.5 días |
| Mediana | 1 día | 6 días | 12 días |
| Percentil 75 | 3 días | 12 días | 19 días |
| Máximo | 371 días | 430 días | 439 días |

**9. Mapeo de valores de catálogo**

Los campos codificados con valores numéricos se reemplazan por sus descripciones textuales del catálogo oficial SSA. Se aplican 15 catálogos diferentes:

- **Catálogo SI_NO**: 20 columnas médicas (ASMA, DIABETES, HIPERTENSION, etc.) → "SI", "NO", "NO APLICA", "SE IGNORA", "NO ESPECIFICADO"
- **Catálogo SEXO**: "1" → "MUJER", "2" → "HOMBRE"
- **Catálogo ORIGEN**: "1" → "USMER", "2" → "FUERA DE USMER"
- **Catálogo TIPO_PACIENTE**: "1" → "AMBULATORIO", "2" → "HOSPITALIZADO"
- **Catálogo de ENTIDADES**: claves de 2 dígitos → nombres de estados (ej. "09" → "CIUDAD DE MÉXICO")
- **Catálogo MUNICIPIOS**: clave entidad + clave municipio → nombre (ej. "09_007" → "IZTAPALAPA")
- **Catálogo SECTOR**: sectores de salud (IMSS, SSA, ISSSTE, PEMEX, etc.)
- **Catálogo RESULTADO_PCR**: 35 valores para virus respiratorios identificados
- **Catálogo CLASIFICACION_FINAL_COVID / FLU**: 7 y 5 categorías diagnósticas respectivamente

La implementación utiliza el método `.replace()` de Polars en modo lazy, permitiendo procesar los 7.8 millones de registros sin cargar el DataFrame completo en memoria.

---

### D. Parámetros de Configuración del Data Warehouse

**MongoDB**

| Parámetro | Valor |
|---|---|
| Versión | 8.0 |
| Imagen Docker | mongo:8.0 |
| Puerto | 127.0.0.1:27017 (solo loopback) |
| Base de datos | covid |
| Autenticación | admin + contraseña por archivo de secreto |
| Volumen | mongodb_data (volumen nombrado Docker) |
| restart policy | unless-stopped |
| Índice principal | ID_REGISTRO (unique) en covid_unified_data |
| Índice secundario | ENTIDAD_RES en colecciones raw |
| Tamaño de lote de ingesta | 50,000 documentos por operación bulk_write |
| Tamaño de lote de exportación | 100,000 documentos por escritura Parquet |

**Parquet**

| Parámetro | Valor |
|---|---|
| Librería | PyArrow + Polars |
| Escritura | ParquetWriter con chunks de 100,000 filas (streaming) |
| Codificación fechas | Date nativo (días desde epoch) |
| Codificación numérica | Int64 para EDAD, YEAR_ORIGIN, DIAS_* |
| Codificación texto | String (UTF-8) |

**Variables de entorno (.env)**

| Variable | Descripción |
|---|---|
| MONGO_URI | URI de conexión MongoDB (incluye usuario y contraseña) |
| DB_NAME | Nombre de la base de datos (covid) |

---

### E. Repositorio con el Conjunto de Datos Preprocesados

El repositorio incluye todos los artefactos del pipeline en distintos estados de procesamiento:

**Datos crudos** (`data/DATA_RAW_*.zip`): Siete archivos ZIP descargados directamente del portal SSA, preservados sin modificación como referencia.

**Catálogos oficiales** (`data/diccionario_datos_abiertos/`): Archivos Excel con los catálogos y descriptores oficiales de la SSA, descargados programáticamente.

**Datos analíticos intermedios** (`data/polars/*.parquet`): Cuatro archivos Parquet en distintas etapas del pipeline, utilizados como caché entre notebooks para evitar re-procesar la base de datos completa en cada análisis.

**Producto final** (`data/mexico_city_covid_final_report.csv`): CSV con 7,840,211 registros de la CDMX, todas las variables etiquetadas con sus descripciones del catálogo oficial, listo para su uso en herramientas de análisis o minería de datos.

El repositorio Git documenta la evolución del pipeline a través de sus commits, desde la descarga inicial hasta el CSV final.

---

## Documentación Técnica del Pipeline

### Notebook 01 — Descarga de Datos

**Propósito:** Descargar los siete archivos de datos abiertos COVID-19 desde el portal de la SSA y validar su integridad.

**Decisiones técnicas:**

Los URLs de los archivos históricos (2020-2024) siguen el patrón `/historicos/{año}/COVID19MEXICO{año}.zip`. Sin embargo, los archivos de 2025 y 2026 tienen nombres distintos y siguen la URL del corte vigente del portal. Para manejar esta inconsistencia, se implementó una función `rename_files()` que extrae el año mediante expresión regular y renombra todos los archivos al formato normalizado `DATA_RAW_{YEAR}.zip`. Si no se encuentra año en el nombre, se asigna "2026" como año por defecto.

La descarga utiliza `requests` con `stream=True` y escritura en chunks de 1 MB, lo que evita cargar archivos de varios gigabytes completos en memoria.

**Código:**

```python
def rename_files(datadir):
    normalized = []
    for url in datadir:
        name = Path(url).name
        year = re.search(r"(20\d{2})", name)
        year = year.group(1) if year else "2026"
        normalized.append(f"DATA_RAW_{year}.zip")
    return normalized
```

**Resultado:**

```
DATA_RAW_2020.zip → COVID19MEXICO2020.csv (3,868,396 registros)
DATA_RAW_2021.zip → COVID19MEXICO2021.csv (8,830,345 registros)
DATA_RAW_2022.zip → COVID19MEXICO2022.csv (6,451,944 registros)
DATA_RAW_2023.zip → COVID19MEXICO.csv     (1,222,219 registros)
DATA_RAW_2024.zip → COVID19MEXICO.csv     (177,618 registros)
DATA_RAW_2025.zip → COVID19MEXICO.csv     (157,142 registros)
DATA_RAW_2026.zip → COVID19MEXICO.csv     (217,324 registros)
```

Todos los archivos se validan como ZIPs correctos antes de continuar.

img: captura de la celda de validación mostrando "is a valid zip file" para cada año

---

### Notebook 02 — Ingesta a MongoDB

**Propósito:** Cargar los siete CSV desde los ZIP directamente a colecciones MongoDB separadas por año, sin extraer los archivos a disco.

**Decisiones técnicas:**

La lectura se realiza con `pandas.read_csv()` usando `compression='zip'` para leer directamente desde el ZIP sin descomprimir. Se usa `dtype=str` para que todos los campos se carguen como texto (los códigos numéricos del catálogo como "09", "097" deben conservar sus ceros a la izquierda). El parámetro `keep_default_na=False` evita que valores como "" o "NA" se conviertan silenciosamente a `NaN`.

El `ID_REGISTRO` se usa como `_id` del documento MongoDB, lo que aprovecha el índice primario automático de MongoDB para deduplicación desde el inicio.

Se procesa en chunks de 50,000 registros y se usa `insert_many` con `ordered=False` para maximizar rendimiento. Los errores `BulkWriteError` con código 11000 (clave duplicada) se capturan y contabilizan sin interrumpir la carga.

Se creó un índice en `ENTIDAD_RES` para optimizar las consultas de filtrado por estado.

**Resultado:**

| Colección | Registros insertados |
|---|---|
| raw_2020_data | 3,868,396 |
| raw_2021_data | 8,830,345 |
| raw_2022_data | 6,451,944 |
| raw_2023_data | 1,222,219 |
| raw_2024_data | 177,618 |
| raw_2025_data | 157,142 |
| raw_2026_data | 217,324 |
| **Total** | **20,924,988** |

img: captura del output de ingesta mostrando chunks procesados y total insertado

El notebook también incluye una sección de corrección para el año 2026: dado que inicialmente se cargó con datos de una fecha anterior, se elimina la colección, se re-descarga el archivo más reciente y se vuelve a ingestar. Esto documenta el proceso de actualización incremental del pipeline.

---

### Notebook 03 — Exploración y Discovery

**Propósito:** Entender la distribución geográfica de los datos, el esquema de cada colección y cuantificar los registros disponibles para la CDMX.

**Hallazgos principales:**

La exploración por `ENTIDAD_RES = "09"` (residencia en CDMX) revela la distribución temporal de casos:

```
2020: 1,125,066
2021: 3,023,394
2022: 1,917,807
2023:   300,804
2024:    29,983
2025:    27,376
2026:    37,292
Total: 6,461,722
```

La exploración por `ENTIDAD_UM = "09"` (atención en CDMX) revela un total mayor (7,360,674), lo que indica que hay pacientes que se atienden en CDMX pero no residen ahí. Esta observación motivó el enriquecimiento posterior de la colección.

Se identificó también el cambio de esquema entre colecciones: las colecciones 2020-2023 tienen 41 campos y utilizan `RESULTADO_LAB` y `CLASIFICACION_FINAL`, mientras que 2024-2026 tienen 43 campos con los nuevos campos PCR e Influenza. Esta diferencia es el insumo directo para el diseño de la función de estandarización en el siguiente notebook.

img: captura del output mostrando keys de raw_2020_data vs raw_2024_data con diferencias resaltadas

---

### Notebook 04 — Limpieza y Construcción de Colecciones

**Propósito:** Estandarizar el esquema entre años, unificar en una colección maestra y crear la colección CDMX.

Este notebook es el núcleo del proceso de limpieza. Ejecuta cuatro operaciones secuenciales:

**Operación 1: Migración a covid_unified_data**

La función `standardize_document()` aplica el mapeo de campos inconsistentes y añade `YEAR_ORIGIN`. Se usa `UpdateOne` con `upsert=True` en lugar de insert para deduplicar registros que pudieran aparecer en múltiples años. Se procesa en lotes de 50,000 con `bulk_write`. La operación tomó aproximadamente 1 hora en completarse sobre los 20.9M de registros.

```python
operacion = UpdateOne(
    {"ID_REGISTRO": id_registro},
    {"$set": doc_limpio},
    upsert=True
)
```

**Operación 2: Creación inicial de mexico_city_covid_data**

Se usa un pipeline de agregación con `$match` y `$out` para crear la colección CDMX directamente en MongoDB sin mover datos a Python. El criterio inicial de "dato de Ciudad de México" se define por residencia o nacimiento:

```python
pipeline = [
    {"$match": {"$or": [{"ENTIDAD_RES": "09"}, {"ENTIDAD_NAC": "09"}]}},
    {"$out": "mexico_city_covid_data"}
]
db.covid_unified_data.aggregate(pipeline)
# Resultado: 7,299,605 documentos
```

**Operación 3: Integración de ENTIDAD_UM (enriquecimiento)**

Durante la exploración se identifica que 540,606 registros adicionales tienen `ENTIDAD_UM = "09"` (fueron atendidos en una unidad médica de la CDMX) pero no cumplen los dos filtros anteriores. Dado que estos pacientes tienen una relación directa con el sistema de salud capitalino, se decide incorporarlos como parte de la integración de datos requerida por el proyecto. Se insertan con `insert_many` y `ordered=False`, gestionando duplicados con `BulkWriteError`. Total final: 7,840,211.

**Operación 4: Limpieza de campos y corrección de tipos**

Se elimina `ID_REGISTRO` y `FECHA_ACTUALIZACION`. Se convierte `EDAD` a entero.

img: captura del output de update_many mostrando 7,840,210 documentos modificados

---

### Notebook 05 — Mapeo de Catálogos

**Propósito:** Analizar el esquema del Parquet resultante, descargar los catálogos oficiales y corregir el tipo de dato de EDAD.

El notebook descarga el catálogo oficial (`diccionario_datos_abiertos.zip`) y explora el contenido de cada hoja del Excel para entender qué catálogo aplica a cada columna. Se identifican los casos especiales que requerirán manejo particular en el mapeo final (notebook 09):

- `RESULTADO_PCR` tiene 35 valores posibles que abarcan distintos virus respiratorios, incluyendo SARS-CoV-2.
- El valor "97" en `RESULTADO_PCR` se utiliza como "NO APLICA (CASO SIN MUESTRA)" pero no aparece en la hoja del catálogo, por lo que debe añadirse manualmente.
- `PAIS_ORIGEN = "97"` significa "NO APLICA" sin catálogo formal.
- `MUNICIPIO_RES` requiere una clave compuesta (clave de entidad + clave de municipio) para el mapeo correcto al nombre de la delegación.

Adicionalmente, se corrige el tipo de dato de `EDAD` en MongoDB. Los 540,606 registros incorporados desde la integración de ENTIDAD_UM tenían EDAD como String, por inconsistencia con el resto de la colección. Se normaliza a Int64 mediante un pipeline de agregación con `$convert`.

img: captura de la tabla de verificación mostrando crudo vs mapeado para el primer registro

---

### Notebook 06 — Estadísticas Descriptivas

**Propósito:** Calcular y visualizar estadísticas descriptivas de la variable `EDAD`. El notebook tiene tres secciones que documentan tanto la evolución de la colección como el aprendizaje sobre el uso correcto de Polars.

**Sección 1 — MongoDB + Polars (colección pre-integración, 7,299,605 registros)**

El primer enfoque consulta MongoDB, itera el cursor y carga los resultados en un DataFrame de Polars en memoria. Aunque usa Polars, este patrón reproduce el mismo problema de Pandas: materializar el dataset completo en RAM. El consumo medido fue de aproximadamente 9 GB de memoria disponible consumida. Esta sección sirve como línea base y punto de comparación; sus resultados son correctos, pero el enfoque no escala.

**Sección 2 — Parquet lazy + Polars (colección pre-integración, RES+NAC)**

Tras identificar el problema de arquitectura, los datos se exportan a Parquet y se acceden mediante `scan_parquet` (evaluación lazy). Polars construye un plan de ejecución optimizado y solo carga en memoria lo estrictamente necesario para cada operación. Este es el uso correcto de Polars para datasets grandes. El archivo generado es `mexico_city_covid_data.parquet` con las columnas EDAD e HIPERTENSION, suficientes para las estadísticas requeridas.

**Sección 3 — Parquet lazy + Polars (colección post-integración, RES+NAC+UM)**

Una vez completada la integración de ENTIDAD_UM en notebook 04, se genera un nuevo archivo Parquet (`mexico_res_nac_um.parquet`) que representa el estado final de la colección con 7,840,211 registros, y se calculan las mismas estadísticas sobre él. Esta sección cierra el ciclo: los números de la colección enriquecida son ahora los definitivos del proyecto.

Las tres secciones calculan las mismas métricas: máximo, mínimo, media, varianza, covarianza con HIPERTENSION, y cuartiles (Q1, mediana, Q3, percentil 90). Cada sección incluye un histograma de distribución de edades y un boxplot de cuartiles sobre una muestra del 1%.

**Hallazgos:**

| Estadístico | Valor (EDAD < 105) |
|---|---|
| Edad máxima | 104 años |
| Edad mínima | 0 años |
| Edad promedio | 38.68 años |
| Q1 | 26 años |
| Mediana | 37 años |
| Q3 | 51 años |
| Percentil 90 | 62 años |
| Varianza | 297.6 |
| Covarianza (EDAD, HIPERTENSION) | 0.33 |

Se identificaron 11 registros con edad mayor a 122 años (límite biológico razonable), que corresponden a errores de captura. El análisis se realiza filtrando `EDAD < 105` para excluir estos valores atípicos extremos.

img: histograma de distribución de edades con línea roja de promedio (sección MongoDB)

img: boxplot de cuartiles de edad (sección MongoDB)

img: histograma de distribución de edades (sección Parquet ENTIDAD_RES/NAC)

img: boxplot de cuartiles (sección Parquet ENTIDAD_RES/NAC)

img: histograma de distribución de edades (sección Parquet con ENTIDAD_UM)

img: boxplot de cuartiles (sección Parquet con ENTIDAD_UM)

---

### Notebook 07 — Análisis de Defunciones

**Propósito:** Exportar la colección completa a Parquet con tipos correctos, calcular las variables de tiempo clínico y analizar las defunciones por municipio de residencia.

**Exportación a Parquet**

Se exporta la colección `mexico_city_covid_data` completa (42 columnas) a Parquet usando escritura por chunks de 100,000 documentos con `ParquetWriter` de PyArrow. El uso de `batch_size=100_000` en el cursor MongoDB evita cargar los 7.8M de documentos en RAM simultáneamente.

**Conversión de fechas**

El valor especial `"9999-99-99"` en `FECHA_DEF` se reemplaza por `null` y se convierte a tipo `Date` nativo:

```python
processed_df = df_lazy.with_columns([
    pl.col(col).replace("9999-99-99", None).str.to_date("%Y-%m-%d", strict=False)
    for col in ["FECHA_DEF", "FECHA_INGRESO", "FECHA_SINTOMAS"]
])
```

**Análisis de defunciones por municipio**

De los 7,840,211 registros totales, 59,767 corresponden a defunciones de residentes de CDMX (`ENTIDAD_RES = "09"` con `FECHA_DEF` no nula). La distribución por delegación/alcaldía muestra:

| Municipio | Defunciones |
|---|---|
| IZTAPALAPA (007) | 11,511 |
| GUSTAVO A. MADERO (005) | 9,480 |
| ÁLVARO OBREGÓN (010) | 4,953 |
| CUAUHTÉMOC (003) | 4,094 |
| IZTACALCO (002) | 4,029 |
| ... | ... |
| MILPA ALTA (009) | 704 |

img: tabla completa de defunciones por municipio (output de polars)

---

### Notebook 08 — Tasa de Mortalidad

**Propósito:** Calcular tasas de mortalidad por COVID-19 en la CDMX desagregadas por sexo, usando como denominador la población del Censo INEGI 2020.

**Metodología**

Las defunciones se contabilizan desde el Parquet de fechas, filtrando por `ENTIDAD_RES = "09"` y `FECHA_DEF` no nula. Se agrupa por `SEXO` (código "1" = mujer, "2" = hombre) y se divide entre la población correspondiente del censo.

**Resultados**

| Categoría | Muertes | Población | Tasa de Mortalidad |
|---|---|---|---|
| General | 59,767 | 9,209,944 | 0.6489% |
| Hombres | 37,191 | 4,404,927 | 0.8443% |
| Mujeres | 22,576 | 4,805,017 | 0.4698% |

La tasa de mortalidad masculina (0.84%) es aproximadamente el doble de la femenina (0.47%), resultado consistente con la literatura epidemiológica sobre COVID-19.

img: output del dataframe mostrando conteo de muertes por sexo

---

### Notebook 09 — Generación del CSV Final

**Propósito:** Aplicar el mapeo completo de catálogos al Parquet de fechas y generar el CSV final preprocesado.

**Pipeline de mapeo**

Se construyen 15 diccionarios de Python a partir de las hojas del Excel oficial usando la función `excel_to_dict()`. La columna `MUNICIPIO` requiere una clave compuesta que concatena `ENTIDAD_RES + "_" + MUNICIPIO_RES` (ej. "09_007" → "IZTAPALAPA"); esto se construye con `pl.concat_str()` antes de reemplazar las claves de entidad y municipio.

Todo el pipeline se ejecuta en modo lazy sobre el Parquet de 62.8 MB y se materializa directamente a CSV mediante `sink_csv()`, sin cargar el DataFrame completo en memoria en ningún momento.

**Validación final**

```
Archivo Parquet original:  7,840,211 filas   62.8 MB
Archivo CSV final:         7,840,211 filas  2,936.5 MB
Diferencia de filas:       0
Factor de tamaño:          46.8x
Tiempo de escritura:       28.2 segundos
```

La diferencia de tamaño (46.8x) se debe a que el CSV almacena los textos de catálogo completos ("CASO DE SARS-COV-2 CONFIRMADO" en lugar de "3") y no tiene compresión columnar.

img: output de verificación mostrando tabla comparativa filas/tamaño Parquet vs CSV

---

## Conclusión

El pipeline desarrollado transforma datos crudos de vigilancia epidemiológica en un conjunto de datos limpio, estandarizado y analíticamente utilizable. El proceso abarca desde la descarga automatizada de 20.9 millones de registros nacionales hasta la generación de un CSV final de 7,840,211 registros de la Ciudad de México, con todos los campos codificados traducidos a sus descripciones textuales oficiales.

Las principales contribuciones del proceso de preparación son:

- **Unificación de esquemas heterogéneos**: el cambio de formato SSA introducido en julio de 2024 se resuelve mediante una función de estandarización que hace transparente la diferencia entre años para todos los análisis posteriores.

- **Deduplicación robusta**: el uso de `upsert` sobre un índice único garantiza que el dataset final no contenga duplicados incluso si un paciente aparece en múltiples cortes anuales.

- **Enriquecimiento geográfico**: la incorporación de registros con unidad médica en CDMX agrega 540,606 registros (7.4% del total) que habrían sido excluidos con un filtro simple por residencia.

- **Variables de tiempo clínico**: las columnas `DIAS_SINTOMAS`, `DIAS_ATENCION` y `DIAS_ENFERMO` permiten análisis de progresión de la enfermedad que no estaban disponibles en los datos originales.

- **Arquitectura reproducible**: el uso de Docker, uv y notebooks numerados garantiza que el pipeline completo puede ejecutarse en cualquier entorno con los mismos resultados.

Los análisis estadísticos preliminares muestran que la población afectada tiene una edad promedio de 38.7 años, con una tasa de mortalidad general del 0.65% sobre la población CDMX, siendo los hombres aproximadamente 1.8 veces más propensos a fallecer que las mujeres por esta causa.
