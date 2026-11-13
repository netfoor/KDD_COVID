# COVID-19 KDD — Ciudad de México

Knowledge Discovery from Data pipeline sobre los registros epidemiológicos de COVID-19 publicados por la Secretaría de Salud de México (SSA), con foco en la Ciudad de México (entidad 09).

**Autor:** Fortino Romero Mantilla  
**Institución:** Universidad Tecnológica de Puebla  
**Documentación completa:** [DOC.md](DOC.md)

---

## Descripción

El proyecto descarga, ingesta, limpia, estandariza y enriquece 7.8 millones de registros epidemiológicos de la CDMX provenientes de siete cortes anuales (2020–2026) del portal de Datos Abiertos de la SSA. El resultado es un dataset preprocesado disponible en cuatro formatos: dos colecciones MongoDB y dos archivos CSV (con y sin mapeo de catálogos).

---

## Arquitectura

```
Datos Abiertos SSA (ZIP/CSV)
        |
        v
[01] Descarga y normalización de archivos
        |
        v
[02] Ingesta raw a MongoDB (7 colecciones por año)
        |
        v
[03] Exploración y discovery del esquema
        |
        v
[04] Limpieza, estandarización y construcción de colecciones CDMX
     - Unificación de esquema 2020-2023 vs 2024-2026
     - Colección unificada nacional: covid_unified_data (~20.7M docs)
     - Colección CDMX inicial (RES + NAC): 7,299,605 docs
     - Integracion ENTIDAD_UM: +540,606 docs -> 7,840,211 docs
        |
        v
[05] Exploración de catálogos y corrección de tipos
        |
        v
[06] Estadísticas descriptivas (edad, varianza, cuartiles)
     - MongoDB -> Polars (alto RAM, referencia)
     - Parquet lazy pre-integración
     - Parquet lazy post-integración
        |
        v
[07] Exportación a Parquet + cálculo de variables clínicas
     - DIAS_SINTOMAS, DIAS_ATENCION, DIAS_ENFERMO
     - Análisis de defunciones por municipio
        |
        v
[08] Tasa de mortalidad por sexo (referencia INEGI 2020)
        |
        v
[09] Mapeo de catálogos -> CSV final mapeado (2.9 GB)
        |
        v
[10] CSV sin mapear (946 MB) + carga de ambas versiones a MongoDB
        |
        v
Productos finales:
  - mexico_city_covid_unmapped.csv     (códigos crudos)
  - mexico_city_covid_final_report.csv (descripciones textuales)
  - MongoDB: mexico_city_covid_unmapped
  - MongoDB: mexico_city_covid_mapped
```

---

## Stack tecnológico

| Herramienta | Rol | Justificación |
|---|---|---|
| MongoDB 8.0 (Docker) | Almacén principal de documentos | Sin límites de almacenamiento (Atlas free tier insuficiente para 20M+ docs) |
| Polars + Parquet | Procesamiento analítico | Evaluación lazy, bajo consumo de RAM, 46x más compacto que CSV |
| Pandas | Lectura de Excel y CSVs de ingesta | Soporte nativo para formatos legacy |
| PyArrow | Escritura Parquet en streaming | Permite escribir por chunks sin cargar todo en memoria |
| uv | Gestión de entorno y dependencias | Resolución determinista, instalación significativamente más rápida que pip |
| Docker Compose | Orquestación del contenedor MongoDB | Reproducibilidad y aislamiento del servicio |

---

## Requisitos

- Docker y Docker Compose
- Python 3.12+
- uv

---

## Instalación

```bash
# Clonar el repositorio
git clone <repo-url>
cd covid19

# Crear archivo de contraseña para MongoDB
echo "tu_password" > mongodb_root_password.txt

# Configurar variables de entorno
cp .env.example .env
# Editar .env con MONGO_URI y DB_NAME

# Instalar dependencias
uv sync

# Levantar MongoDB
docker compose up -d
```

---

## Uso

Los notebooks están numerados secuencialmente. Ejecutar en orden desde un entorno Jupyter con el venv activo:

```bash
source .venv/bin/activate
jupyter notebook
```

| Notebook | Acción |
|---|---|
| 01_download | Descarga los 7 archivos ZIP desde la SSA |
| 02_ingestion | Carga los CSV a MongoDB (raw collections) |
| 03_discovery | Exploración del esquema y distribución geográfica |
| 04_clean | Estandarización, unificación y colección CDMX |
| 05_mapping | Exploración de catálogos y corrección de tipos |
| 06_statistics | Estadísticas descriptivas de edad |
| 07_death | Variables clínicas de tiempo y análisis de defunciones |
| 08_mortality_rate | Tasas de mortalidad por sexo |
| 09_csv | Mapeo completo y generación del CSV final |
| 10_load | CSV sin mapear y carga de ambas versiones a MongoDB |

---

## Estructura del proyecto

```
covid19/
├── data/
│   ├── DATA_RAW_2020.zip ... DATA_RAW_2026.zip
│   ├── diccionario_datos_abiertos/
│   ├── polars/
│   │   ├── mexico_city_covid_data.parquet
│   │   ├── mexico_city_covid_data_complete.parquet
│   │   ├── mexico_city_covid_data_dates.parquet
│   │   └── mexico_res_nac_um.parquet
│   ├── mexico_city_covid_unmapped.csv
│   └── mexico_city_covid_final_report.csv
├── notebooks/
│   └── 01_download.ipynb ... 10_load.ipynb
├── src/
│   └── database.py
├── docker-compose.yml
├── pyproject.toml
├── .env.example
└── DOC.md
```

---

## Comandos útiles

```bash
# Levantar MongoDB
docker compose up -d

# Detener MongoDB
docker compose down

# Ver logs del contenedor
docker logs mongodb-covid

# Conectar con MongoDB Compass
# URI: mongodb://admin:<password>@127.0.0.1:27017/?authSource=admin

# Activar entorno virtual
source .venv/bin/activate

# Agregar dependencia
uv add <paquete>
```

---

## Fuentes de datos

- **SSA Datos Abiertos:** registros epidemiológicos COVID-19/SISVER 2020–2026
- **Catálogos oficiales SSA:** `240708 Catalogos.xlsx` — 13 catálogos de valores codificados
- **INEGI Censo 2020:** población CDMX para cálculo de tasas de mortalidad

---

## Resultados

| Métrica | Valor |
|---|---|
| Registros nacionales procesados | 20,924,988 |
| Registros CDMX en dataset final | 7,840,211 |
| Colecciones MongoDB generadas | 9 |
| Variables calculadas añadidas | 4 (YEAR_ORIGIN, DIAS_SINTOMAS, DIAS_ATENCION, DIAS_ENFERMO) |
| Edad promedio de casos CDMX | 38.7 años |
| Tasa de mortalidad general CDMX | 0.65% |
| Defunciones CDMX registradas | 59,767 |

## Licencia

MIT. Consulta el archivo `LICENSE`.
