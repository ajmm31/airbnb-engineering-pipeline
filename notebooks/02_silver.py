# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
# DBTITLE 1,Silver Layer Header
# MAGIC %md
# MAGIC # --- CAPA SILVER: LIMPIEZA Y TRANSFORMACIÓN ---
# MAGIC
# MAGIC Esta capa se encarga de:
# MAGIC - Limpiar y estandarizar los datos de la capa Bronze
# MAGIC - Convertir tipos de datos a formatos analíticos apropiados (string → double, integer)
# MAGIC - Separar columnas compuestas en campos individuales (rating + reviews, tipo + ubicación)
# MAGIC - Aplicar validaciones de calidad de datos (duplicados, valores nulos, precios inválidos)
# MAGIC - Enriquecer con columnas calculadas (precio final considerando ofertas)
# MAGIC
# MAGIC **Fuente de datos**: Tabla Delta `bronze_airbnb`
# MAGIC **Formato de salida**: Tabla Delta `silver_airbnb` con datos limpios y estructurados

# COMMAND ----------

# DBTITLE 1,Import libraries
# 1. Importar funciones necesarias de PySpark
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, IntegerType, DoubleType
import re

# COMMAND ----------

# DBTITLE 1,Read bronze table
# 2. Leer la tabla Bronze
df_bronze = spark.table("bronze_airbnb")

# Mostrar schema y contar registros iniciales
print(f"Registros en Bronze: {df_bronze.count()}")
print("\nSchema de Bronze:")
df_bronze.printSchema()

# COMMAND ----------

# DBTITLE 1,Transform and clean data
# 3. Aplicar transformaciones de limpieza y estandarización

df_silver = df_bronze

# 3.1 Convertir precios de string a numérico
# Eliminar formato, manejar vacíos y convertir a double
df_silver = df_silver.withColumn(
    "price_clean",
    F.regexp_replace(F.col("Price(in dollar)"), "[^0-9.]", "")
)
df_silver = df_silver.withColumn(
    "price",
    F.when(F.col("price_clean") != "", F.col("price_clean").cast(DoubleType()))
     .otherwise(F.lit(None).cast(DoubleType()))
).drop("price_clean")

df_silver = df_silver.withColumn(
    "offer_price_clean",
    F.regexp_replace(F.col("Offer price(in dollar)"), "[^0-9.]", "")
)
df_silver = df_silver.withColumn(
    "offer_price",
    F.when(F.col("offer_price_clean") != "", F.col("offer_price_clean").cast(DoubleType()))
     .otherwise(F.lit(None).cast(DoubleType()))
).drop("offer_price_clean")

# 3.2 Mantener rango de fechas como string  
df_silver = df_silver.withColumn(
    "date_range",
    F.col("Date")
)

# 3.3 Separar rating y número de reviews
# Formato: "4.85 (531)" o "5.0 (48)"
df_silver = df_silver.withColumn(
    "rating_str",
    F.regexp_extract(F.col("Review and rating"), "^([0-9.]+)", 1)
)
df_silver = df_silver.withColumn(
    "rating",
    F.when(F.col("rating_str") != "", F.col("rating_str").cast(DoubleType()))
     .otherwise(F.lit(None).cast(DoubleType()))
).drop("rating_str")

df_silver = df_silver.withColumn(
    "review_count_str",
    F.regexp_extract(F.col("Review and rating"), "\\((\\d+)\\)", 1)
)
df_silver = df_silver.withColumn(
    "review_count",
    F.when(F.col("review_count_str") != "", F.col("review_count_str").cast(IntegerType()))
     .otherwise(F.lit(None).cast(IntegerType()))
).drop("review_count_str")

# 3.4 Extraer número de camas como entero
# Formato: "4 beds" o "1 queen bed"
df_silver = df_silver.withColumn(
    "num_beds_str",
    F.regexp_extract(F.col("Number of bed"), "^(\\d+)", 1)
)
df_silver = df_silver.withColumn(
    "num_beds",
    F.when(F.col("num_beds_str") != "", F.col("num_beds_str").cast(IntegerType()))
     .otherwise(F.lit(None).cast(IntegerType()))
).drop("num_beds_str")

# 3.5 Extraer tipo de propiedad desde Title usando regexp_extract
# Formato: "Chalet in Skykomish...", "Cabin in Hancock..."
# Captura todo antes de " in "
df_silver = df_silver.withColumn(
    "property_type",
    F.when(
        F.col("Title").contains(" in "),
        F.trim(F.regexp_extract(F.col("Title"), "^(.+?) in ", 1))
    ).otherwise(F.lit("Unknown"))
)

# 3.6 Extraer ubicación desde Title usando regexp_extract
# Captura todo después de " in "
df_silver = df_silver.withColumn(
    "location",
    F.when(
        F.col("Title").contains(" in "),
        F.trim(F.regexp_extract(F.col("Title"), " in (.+)$", 1))
    ).otherwise(F.lit("Unknown"))
)

# 3.7 Agregar columna de nombre de la propiedad desde Detail
df_silver = df_silver.withColumn(
    "property_name",
    F.trim(F.col("Detail"))
)

print("Transformaciones aplicadas exitosamente.")

# COMMAND ----------

# DBTITLE 1,Data quality checks
# 4. Aplicar validaciones de calidad de datos

# 4.1 Seleccionar solo las columnas transformadas (eliminar las originales)
df_silver = df_silver.select(
    "property_type",
    "property_name",
    "location",
    "date_range",
    "price",
    "offer_price",
    "rating",
    "review_count",
    "num_beds"
)

# 4.2 Eliminar duplicados exactos
registros_antes = df_silver.count()
df_silver = df_silver.dropDuplicates()
registros_despues = df_silver.count()
duplicados_eliminados = registros_antes - registros_despues

print(f"Duplicados eliminados: {duplicados_eliminados}")

# 4.3 Filtrar registros donde price sea nulo o <= 0 (inválidos)
df_silver = df_silver.filter(
    (F.col("price").isNotNull()) & (F.col("price") > 0)
)

print(f"Registros finales después de validaciones: {df_silver.count()}")

# 4.4 Calcular precio final cuando hay oferta
df_silver = df_silver.withColumn(
    "final_price",
    F.when(F.col("offer_price").isNotNull(), F.col("offer_price"))
     .otherwise(F.col("price"))
)

print("\nValidaciones de calidad aplicadas:")
print("- Duplicados eliminados")
print("- Registros con precios inválidos filtrados")
print("- Columna final_price calculada (usa offer_price si existe, si no price)")

# COMMAND ----------

# DBTITLE 1,Save silver table
# 5. Guardar como tabla Delta en la capa Silver
output_table = "silver_airbnb"

(df_silver.write
 .format("delta")
 .mode("overwrite")
 .saveAsTable(output_table))

print(f"¡Éxito! La tabla '{output_table}' ha sido creada en la capa Silver.")

# COMMAND ----------

# DBTITLE 1,Display sample and statistics
# 6. Visualizar muestra de datos limpios con estadísticas clave

print("=" * 80)
print("MUESTRA DE DATOS TRANSFORMADOS (Silver Layer)")
print("=" * 80)

# Mostrar schema final
print("\nSchema Final:")
df_silver.printSchema()

# Mostrar muestra de datos
print("\nMuestra de 10 registros:")
display(df_silver.limit(10))

# Estadísticas descriptivas de precios
print("\n" + "=" * 80)
print("ESTADÍSTICAS CLAVE")
print("=" * 80)

stats = df_silver.select(
    F.count("*").alias("total_propiedades"),
    F.countDistinct("property_type").alias("tipos_propiedad"),
    F.round(F.avg("final_price"), 2).alias("precio_promedio"),
    F.round(F.min("final_price"), 2).alias("precio_minimo"),
    F.round(F.max("final_price"), 2).alias("precio_maximo"),
    F.round(F.avg("rating"), 2).alias("rating_promedio"),
    F.sum("review_count").alias("total_reviews")
).collect()[0]

print(f"\nTotal de propiedades: {stats['total_propiedades']}")
print(f"Tipos de propiedad únicos: {stats['tipos_propiedad']}")
print(f"\nPrecios (USD):")
print(f"  - Promedio: ${stats['precio_promedio']}")
print(f"  - Mínimo: ${stats['precio_minimo']}")
print(f"  - Máximo: ${stats['precio_maximo']}")
print(f"\nRating promedio: {stats['rating_promedio']} / 5.0")
print(f"Total de reviews: {stats['total_reviews']:,}")

# Distribución por tipo de propiedad
print("\n" + "=" * 80)
print("DISTRIBUCIÓN POR TIPO DE PROPIEDAD")
print("=" * 80)
df_silver.groupBy("property_type").count().orderBy(F.desc("count")).show(truncate=False)
