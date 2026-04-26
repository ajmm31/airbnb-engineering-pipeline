# Databricks notebook source
# DBTITLE 1,Gold Layer Header
# MAGIC %md
# MAGIC # --- CAPA GOLD: ANALÍTICAS DE NEGOCIO ---
# MAGIC
# MAGIC Esta capa contiene:
# MAGIC - Tablas agregadas optimizadas para análisis de negocio
# MAGIC - Métricas y KPIs pre-calculados
# MAGIC - Segmentaciones por dimensiones clave (tipo de propiedad, ubicación, precio)
# MAGIC - Datos desnormalizados listos para dashboards y reportes

# COMMAND ----------

# DBTITLE 1,Import libraries
# 1. Importar funciones necesarias de PySpark
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from pyspark.sql.window import Window

# COMMAND ----------

# DBTITLE 1,Read silver table
# 2. Leer la tabla Silver
df_silver = spark.table("silver_airbnb")

print(f"Registros en Silver: {df_silver.count()}")
print("\nColumnas disponibles:")
df_silver.printSchema()

# COMMAND ----------

# DBTITLE 1,Create property type analytics
# 3. TABLA GOLD: Análisis por Tipo de Propiedad

gold_property_type = df_silver.groupBy("property_type").agg(
    F.count("*").alias("total_propiedades"),
    F.round(F.avg("price"), 2).alias("precio_promedio"),
    F.round(F.avg("offer_price"), 2).alias("precio_oferta_promedio"),
    F.round(F.avg("final_price"), 2).alias("precio_final_promedio"),
    F.round(F.min("final_price"), 2).alias("precio_minimo"),
    F.round(F.max("final_price"), 2).alias("precio_maximo"),
    F.round(F.avg("rating"), 2).alias("rating_promedio"),
    F.sum("review_count").alias("total_reviews"),
    F.round(F.avg("num_beds"), 1).alias("camas_promedio"),
    F.sum(F.when(F.col("offer_price").isNotNull(), 1).otherwise(0)).alias("propiedades_con_oferta")
)

# Calcular porcentaje de propiedades con oferta y descuento promedio
gold_property_type = gold_property_type.withColumn(
    "pct_con_oferta",
    F.round((F.col("propiedades_con_oferta") / F.col("total_propiedades")) * 100, 1)
)

gold_property_type = gold_property_type.withColumn(
    "descuento_promedio_pct",
    F.round(
        ((F.col("precio_promedio") - F.col("precio_final_promedio")) / F.col("precio_promedio")) * 100,
        1
    )
)

# Ordenar por total de propiedades
gold_property_type = gold_property_type.orderBy(F.desc("total_propiedades"))

# Guardar como tabla Delta
gold_property_type.write.format("delta").mode("overwrite").saveAsTable("gold_property_type_analytics")

print("✅ Tabla 'gold_property_type_analytics' creada exitosamente.")
print(f"   Tipos de propiedad analizados: {gold_property_type.count()}")

# COMMAND ----------

# DBTITLE 1,Create location analytics
# 4. TABLA GOLD: Análisis por Ubicación (Top 20)

gold_location = df_silver.groupBy("location").agg(
    F.count("*").alias("total_propiedades"),
    F.round(F.avg("final_price"), 2).alias("precio_promedio"),
    F.round(F.min("final_price"), 2).alias("precio_minimo"),
    F.round(F.max("final_price"), 2).alias("precio_maximo"),
    F.round(F.avg("rating"), 2).alias("rating_promedio"),
    F.sum("review_count").alias("total_reviews"),
    F.countDistinct("property_type").alias("tipos_propiedad_unicos")
)

# Filtrar Top 20 ubicaciones por número de propiedades
gold_location = gold_location.orderBy(F.desc("total_propiedades")).limit(20)

# Guardar como tabla Delta
gold_location.write.format("delta").mode("overwrite").saveAsTable("gold_location_analytics")

print("✅ Tabla 'gold_location_analytics' creada exitosamente.")
print(f"   Top ubicaciones analizadas: {gold_location.count()}")

# COMMAND ----------

# DBTITLE 1,Create price segments analytics
# 5. TABLA GOLD: Análisis por Segmentos de Precio

# Crear segmentos de precio
df_with_segments = df_silver.withColumn(
    "precio_segmento",
    F.when(F.col("final_price") < 100, "Budget (<$100)")
     .when((F.col("final_price") >= 100) & (F.col("final_price") < 250), "Mid-range ($100-$250)")
     .when((F.col("final_price") >= 250) & (F.col("final_price") < 500), "Premium ($250-$500)")
     .otherwise("Luxury (>$500)")
)

# Agregar por segmento
gold_price_segments = df_with_segments.groupBy("precio_segmento").agg(
    F.count("*").alias("total_propiedades"),
    F.round(F.avg("final_price"), 2).alias("precio_promedio"),
    F.round(F.avg("rating"), 2).alias("rating_promedio"),
    F.sum("review_count").alias("total_reviews"),
    F.round(F.avg("num_beds"), 1).alias("camas_promedio")
)

# Calcular porcentaje del total
total_properties = df_silver.count()
gold_price_segments = gold_price_segments.withColumn(
    "porcentaje_total",
    F.round((F.col("total_propiedades") / total_properties) * 100, 1)
)

# Encontrar el tipo de propiedad más común por segmento
top_property_by_segment = df_with_segments.groupBy("precio_segmento", "property_type").count()
window_spec = Window.partitionBy("precio_segmento").orderBy(F.desc("count"))
top_property_by_segment = top_property_by_segment.withColumn(
    "rank",
    F.row_number().over(window_spec)
).filter(F.col("rank") == 1).select(
    "precio_segmento",
    F.col("property_type").alias("tipo_mas_comun")
)

# Unir con la tabla principal
gold_price_segments = gold_price_segments.join(
    top_property_by_segment,
    "precio_segmento",
    "left"
)

# Ordenar por precio promedio
gold_price_segments = gold_price_segments.orderBy("precio_promedio")

# Guardar como tabla Delta
gold_price_segments.write.format("delta").mode("overwrite").saveAsTable("gold_price_segments")

print("✅ Tabla 'gold_price_segments' creada exitosamente.")
print(f"   Segmentos de precio: {gold_price_segments.count()}")

# COMMAND ----------

# DBTITLE 1,Create top properties table
# 6. TABLA GOLD: Propiedades Top (Altamente Calificadas)

# Filtrar propiedades con rating >= 4.9 y al menos 50 reviews
gold_top_properties = df_silver.filter(
    (F.col("rating") >= 4.9) & (F.col("review_count") >= 50)
).select(
    "property_type",
    "property_name",
    "location",
    "rating",
    "review_count",
    "final_price",
    "num_beds",
    "date_range"
).orderBy(
    F.desc("rating"),
    F.desc("review_count")
)

# Guardar como tabla Delta
gold_top_properties.write.format("delta").mode("overwrite").saveAsTable("gold_top_properties")

print("✅ Tabla 'gold_top_properties' creada exitosamente.")
print(f"   Propiedades top identificadas: {gold_top_properties.count()}")

# COMMAND ----------

# DBTITLE 1,Display summary
# 7. RESUMEN DE INSIGHTS - Capa Gold

print("=" * 80)
print("RESUMEN DE TABLAS GOLD CREADAS")
print("=" * 80)

print("\n1️⃣ gold_property_type_analytics - Análisis por tipo de propiedad")
print("   Contiene: Métricas agregadas por cada tipo de propiedad")
print("   Uso: Comparar rendimiento entre tipos de propiedades\n")

print("2️⃣ gold_location_analytics - Top 20 ubicaciones")
print("   Contiene: Métricas agregadas de las ubicaciones más populares")
print("   Uso: Identificar mercados clave y oportunidades geográficas\n")

print("3️⃣ gold_price_segments - Análisis por segmento de precio")
print("   Contiene: Segmentación en Budget, Mid-range, Premium, Luxury")
print("   Uso: Estrategias de pricing y análisis de mercado\n")

print("4️⃣ gold_top_properties - Propiedades altamente calificadas")
print("   Contiene: Propiedades con rating ≥ 4.9 y ≥ 50 reviews")
print("   Uso: Benchmarking y mejores prácticas\n")

print("=" * 80)
print("TABLAS LISTAS PARA CONSUMO EN DASHBOARDS Y REPORTES")
print("=" * 80)

# COMMAND ----------

# DBTITLE 1,Display key insights
# 8. VISUALIZAR INSIGHTS CLAVE

print("\n" + "=" * 80)
print("TOP 10 TIPOS DE PROPIEDAD POR VOLUMEN")
print("=" * 80)
display(spark.table("gold_property_type_analytics").limit(10))

print("\n" + "=" * 80)
print("TOP 10 UBICACIONES MÁS POPULARES")
print("=" * 80)
display(spark.table("gold_location_analytics").limit(10))

print("\n" + "=" * 80)
print("DISTRIBUCIÓN POR SEGMENTO DE PRECIO")
print("=" * 80)
display(spark.table("gold_price_segments"))

print("\n" + "=" * 80)
print("TOP 10 PROPIEDADES MEJOR CALIFICADAS")
print("=" * 80)
display(spark.table("gold_top_properties").limit(10))
