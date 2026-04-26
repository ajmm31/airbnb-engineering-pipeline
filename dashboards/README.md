# Airbnb Analytics Dashboard

## 📊 Dashboard Ejecutivo de Análisis de Airbnb

Este dashboard proporciona visualizaciones ejecutivas y KPIs clave del pipeline de datos de Airbnb.

### Visualizaciones Incluidas

**Sección 1: KPIs Overview**
- Total Properties (910)
- Average Price ($170.28)
- Average Rating (4.86/5.0)
- Total Reviews (171K)

**Sección 2: Property Type Analysis**
- Top 10 Property Types by Volume (bar chart)
- Price vs Rating Correlation (scatter plot)

**Sección 3: Geographic Distribution**
- Top 10 Locations by Property Count (table)

**Sección 4: Price Segmentation**
- Distribution by Price Segment (pie chart)
- Average Price by Segment (bar chart)

**Sección 5: Top Performers**
- Top 15 Highly Rated Properties (table)

### Pre-requisitos

Antes de importar el dashboard, asegúrate de haber ejecutado el pipeline Medallion completo:

1. **Bronze Layer**: `01_bronze.ipynb` - Tabla `bronze_airbnb`
2. **Silver Layer**: `02_silver.ipynb` - Tabla `silver_airbnb`
3. **Gold Layer**: `03_gold.ipynb` - 4 tablas analíticas:
   - `gold_property_type_analytics`
   - `gold_location_analytics`
   - `gold_price_segments`
   - `gold_top_properties`

### Cómo Importar el Dashboard

#### Opción 1: Interfaz Web de Databricks

1. Ve a **Dashboards** en la barra lateral izquierda
2. Haz clic en **Create Dashboard**
3. Selecciona **Import Dashboard**
4. Carga el archivo `airbnb_executive_analytics.lvdash.json`
5. El dashboard se importará con todas las queries y visualizaciones configuradas

#### Opción 2: Databricks CLI

```bash
# Instalar Databricks CLI si no lo tienes
pip install databricks-cli

# Configurar autenticación
databricks configure --token

# Importar dashboard
databricks workspace import \
  ./dashboards/airbnb_executive_analytics.lvdash.json \
  /Users/<tu-usuario>/Airbnb_Executive_Analytics.lvdash.json \
  --language JSON --format AUTO
```

#### Opción 3: API REST

```bash
# Usando curl
curl -X POST \
  https://<workspace-url>/api/2.0/workspace/import \
  -H "Authorization: Bearer <token>" \
  -F path="/Users/<tu-usuario>/Airbnb_Executive_Analytics.lvdash.json" \
  -F content=@dashboards/airbnb_executive_analytics.lvdash.json \
  -F format=AUTO
```

### Estructura del Dashboard

El archivo `.lvdash.json` contiene:
- **Datasets**: 9 queries SQL que alimentan las visualizaciones
- **Widgets**: 10 visualizaciones (counter, bar, scatter, table, pie)
- **Layout**: Grid de 12 columnas con posicionamiento específico
- **Formatting**: Configuración de colores, formatos de moneda y números

### Personalización

Para personalizar el dashboard:

1. **Abrir el dashboard** en modo edición
2. **Modificar queries** si tus nombres de tablas son diferentes
3. **Ajustar visualizaciones** según tus necesidades
4. **Cambiar colores y estilos** en la configuración de cada widget

### Tablas Requeridas

El dashboard consulta las siguientes tablas:
- `silver_airbnb` - Datos limpios y transformados
- `gold_property_type_analytics` - Métricas por tipo de propiedad
- `gold_location_analytics` - Top 20 ubicaciones
- `gold_price_segments` - Segmentos de precio
- `gold_top_properties` - Propiedades altamente calificadas

Si tus tablas tienen nombres diferentes, actualiza los queries en el dashboard después de importarlo.

### Troubleshooting

**Error: "Table not found"**
- Verifica que hayas ejecutado los 3 notebooks del pipeline (Bronze, Silver, Gold)
- Confirma que las tablas existen con: `SHOW TABLES`

**Dashboard vacío o widgets sin datos**
- Refresca los datasets del dashboard
- Verifica que las queries SQL ejecuten correctamente
- Revisa los permisos de acceso a las tablas

**Visualizaciones no se muestran**
- Espera unos segundos para que carguen los datos
- Refresca la página del dashboard
- Verifica que el compute serverless esté activo

### Soporte

Para más información sobre el pipeline de datos, consulta los notebooks en el directorio `notebooks/`:
- `01_bronze.ipynb`
- `02_silver.ipynb`
- `03_gold.ipynb`
