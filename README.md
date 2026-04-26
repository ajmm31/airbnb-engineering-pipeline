# 🏠 Airbnb Engineering Pipeline - Medallion Architecture

Pipeline completo de ingeniería de datos implementando la arquitectura Medallion (Bronze-Silver-Gold) para análisis de datos de Airbnb.

## 📋 Tabla de Contenidos

- [Descripción del Proyecto](#descripción-del-proyecto)
- [Arquitectura](#arquitectura)
- [Estructura del Proyecto](#estructura-del-proyecto)
- [Requisitos Previos](#requisitos-previos)
- [Instalación y Configuración](#instalación-y-configuración)
- [Ejecución del Pipeline](#ejecución-del-pipeline)
- [Dashboard Ejecutivo](#dashboard-ejecutivo)
- [Tablas y Esquemas](#tablas-y-esquemas)
- [Métricas y KPIs](#métricas-y-kpis)

## 📝 Descripción del Proyecto

Este proyecto implementa un pipeline de datos end-to-end que procesa información de propiedades de Airbnb utilizando la arquitectura Medallion de Databricks:

- **Bronze Layer**: Ingesta de datos raw desde CSV
- **Silver Layer**: Limpieza, transformación y validación de calidad
- **Gold Layer**: Agregaciones analíticas optimizadas para BI

**Dataset**: 957 registros de propiedades Airbnb con información de precios, ubicaciones, ratings y reviews.

## 🏗️ Arquitectura

```
CSV Source (airnb.csv)
    ↓
┌─────────────────────────────────────────────┐
│         BRONZE LAYER (Raw Data)             │
│  - Ingesta desde /Volumes/workspace/...     │
│  - Schema inferido automáticamente          │
│  - Tabla: bronze_airbnb (957 records)       │
└─────────────────────────────────────────────┘
    ↓
┌─────────────────────────────────────────────┐
│    SILVER LAYER (Cleaned & Transformed)     │
│  - Conversión de tipos de datos             │
│  - Extracción de campos compuestos          │
│  - Validación y eliminación de duplicados   │
│  - Tabla: silver_airbnb (910 records)       │
└─────────────────────────────────────────────┘
    ↓
┌─────────────────────────────────────────────┐
│     GOLD LAYER (Business Analytics)         │
│  - 4 tablas agregadas:                      │
│    • gold_property_type_analytics           │
│    • gold_location_analytics                │
│    • gold_price_segments                    │
│    • gold_top_properties                    │
└─────────────────────────────────────────────┘
    ↓
┌─────────────────────────────────────────────┐
│      DASHBOARD (Visualizations)             │
│  - 10 visualizaciones ejecutivas            │
│  - KPIs, charts, tables                     │
└─────────────────────────────────────────────┘
```

## 📁 Estructura del Proyecto

```
airbnb-engineering-pipeline/
│
├── notebooks/
│   ├── 01_bronze.ipynb          # Ingesta de datos raw
│   ├── 02_silver.ipynb          # Limpieza y transformación
│   └── 03_gold.ipynb            # Agregaciones analíticas
│
├── dashboards/
│   ├── airbnb_executive_analytics.lvdash.json
│   └── README.md                # Instrucciones del dashboard
│
├── data/
│   └── airnb.csv                # Dataset fuente (si incluido)
│
└── README.md                    # Este archivo
```

## ⚙️ Requisitos Previos

- **Databricks Workspace** (Community Edition o superior)
- **Unity Catalog** habilitado
- **Serverless Compute** o cluster con DBR 13.0+
- **Volumen en Unity Catalog** para almacenar datos

## 🚀 Instalación y Configuración

### 1. Clonar el Repositorio

```bash
# En tu Databricks Workspace, ve a Repos
# Haz clic en "Add Repo"
# URL: https://github.com/<tu-usuario>/airbnb-engineering-pipeline
```

### 2. Cargar Datos

```python
# Opción A: Subir CSV a un volumen de Unity Catalog
# 1. Crear volumen: CREATE VOLUME workspace.default.data
# 2. Subir airnb.csv al volumen

# Opción B: Usar DBFS (si DBFS root está habilitado)
# Nota: DBFS root puede estar deshabilitado en algunos workspaces
```

### 3. Verificar Ruta de Datos

Abre `notebooks/01_bronze.ipynb` y actualiza la ruta si es necesario:

```python
# Actualizar esta línea con tu ruta
csv_path = "/Volumes/workspace/default/data/airnb.csv"
```

## ▶️ Ejecución del Pipeline

### Orden de Ejecución

Ejecuta los notebooks en orden secuencial:

#### 1. Bronze Layer
```python
# Abrir y ejecutar: notebooks/01_bronze.ipynb
# Resultado: Tabla bronze_airbnb con 957 registros
```

#### 2. Silver Layer
```python
# Abrir y ejecutar: notebooks/02_silver.ipynb
# Resultado: Tabla silver_airbnb con 910 registros limpios
```

#### 3. Gold Layer
```python
# Abrir y ejecutar: notebooks/03_gold.ipynb
# Resultado: 4 tablas analíticas optimizadas para BI
```

### Verificar Tablas Creadas

```sql
-- Ver todas las tablas creadas
SHOW TABLES;

-- Verificar registros
SELECT COUNT(*) FROM bronze_airbnb;  -- 957
SELECT COUNT(*) FROM silver_airbnb;  -- 910
SELECT COUNT(*) FROM gold_property_type_analytics;  -- 43
SELECT COUNT(*) FROM gold_location_analytics;  -- 20
SELECT COUNT(*) FROM gold_price_segments;  -- 4
SELECT COUNT(*) FROM gold_top_properties;  -- 315
```

## 📊 Dashboard Ejecutivo

### Importar Dashboard

1. Ve a **Dashboards** en Databricks
2. Click **Create Dashboard** → **Import**
3. Selecciona `dashboards/airbnb_executive_analytics.lvdash.json`

### Visualizaciones Incluidas

- **KPIs**: Total Properties, Avg Price, Avg Rating, Total Reviews
- **Bar Charts**: Top Property Types, Price Segments
- **Scatter Plot**: Price vs Rating Correlation
- **Tables**: Top Locations, Top Properties
- **Pie Chart**: Price Segment Distribution

Ver más detalles en `dashboards/README.md`

## 🗄️ Tablas y Esquemas

### bronze_airbnb (957 records)
```
- Title: string
- Detail: string
- Date: string
- Price(in dollar): string
- Offer price(in dollar): string
- Review and rating: string
- Number of bed: string
```

### silver_airbnb (910 records)
```
- property_type: string
- property_name: string
- location: string
- date_range: string
- price: double
- offer_price: double
- rating: double
- review_count: integer
- num_beds: integer
- final_price: double
```

### gold_property_type_analytics (43 types)
```
- property_type: string
- total_propiedades: integer
- precio_promedio: double
- precio_final_promedio: double
- rating_promedio: double
- total_reviews: integer
- camas_promedio: double
- pct_con_oferta: double
- descuento_promedio_pct: double
```

### gold_location_analytics (Top 20)
```
- location: string
- total_propiedades: integer
- precio_promedio: double
- precio_minimo: double
- precio_maximo: double
- rating_promedio: double
- total_reviews: integer
- tipos_propiedad_unicos: integer
```

### gold_price_segments (4 segments)
```
- precio_segmento: string
- total_propiedades: integer
- precio_promedio: double
- rating_promedio: double
- porcentaje_total: double
- tipo_mas_comun: string
```

### gold_top_properties (315 properties)
```
- property_type: string
- property_name: string
- location: string
- rating: double (≥ 4.9)
- review_count: integer (≥ 50)
- final_price: double
- num_beds: integer
```

## 📈 Métricas y KPIs

### Resumen General
- **Total Propiedades**: 910
- **Precio Promedio**: $170.28 USD
- **Rango de Precios**: $16 - $1,089
- **Rating Promedio**: 4.86 / 5.0
- **Total Reviews**: 171,421

### Top 3 Tipos de Propiedad
1. **Apartment**: 115 propiedades
2. **Cabin**: 115 propiedades
3. **Condo**: 88 propiedades

### Top 3 Ubicaciones
1. **Kissimmee**: 15 propiedades, $137.07 avg
2. **Kecamatan Ubud, Indonesia**: 15 propiedades, $150.27 avg
3. **Ubud, Indonesia**: 14 propiedades, $177.93 avg

### Segmentos de Precio
- **Budget (<$100)**: 295 propiedades (32.4%)
- **Mid-range ($100-$250)**: 458 propiedades (50.3%)
- **Premium ($250-$500)**: 128 propiedades (14.1%)
- **Luxury (>$500)**: 29 propiedades (3.2%)

### Top Propiedad
**Tiny House Cozy Cabin by Zion, Grand Canyon, Bryce**
- Ubicación: Fredonia
- Rating: 5.0 / 5.0
- Reviews: 611
- Precio: $188

## 🛠️ Transformaciones Clave

### Silver Layer
- ✅ Conversión de precios de string a double con manejo de valores vacíos
- ✅ Extracción de rating y review_count desde formato "4.85 (531)"
- ✅ Parsing de property_type y location desde campo Title
- ✅ Eliminación de 39 duplicados
- ✅ Filtrado de 8 registros con precios inválidos
- ✅ Cálculo de final_price (usa offer_price si existe, sino price)

### Gold Layer
- ✅ Agregaciones por tipo de propiedad con 12 métricas
- ✅ Top 20 ubicaciones con métricas comparativas
- ✅ Segmentación en 4 rangos de precio con tipo más común
- ✅ Identificación de 315 propiedades premium (rating ≥ 4.9, reviews ≥ 50)

## 🤝 Contribuciones

Las contribuciones son bienvenidas:

1. Fork el proyecto
2. Crea una rama para tu feature (`git checkout -b feature/NuevaCaracteristica`)
3. Commit tus cambios (`git commit -m 'Agregar nueva característica'`)
4. Push a la rama (`git push origin feature/NuevaCaracteristica`)
5. Abre un Pull Request

## 📄 Licencia

Este proyecto está bajo la Licencia MIT - ver el archivo `LICENSE` para más detalles.

## 👥 Autor

**Tu Nombre**
- GitHub: [@ajmm31](https://github.com/ajmm31)
- LinkedIn: [alvaro-jose-munoz-murillo](https://www.linkedin.com/in/alvaro-jose-mu%C3%B1oz-murillo-57187184/)

## 🙏 Agradecimientos

- Databricks por la arquitectura Medallion
- Dataset de Airbnb para análisis educacional
- Comunidad de Data Engineering

---

⭐ Si este proyecto te resultó útil, considera darle una estrella en GitHub
