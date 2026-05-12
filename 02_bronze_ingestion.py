# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Ingestion Couche Bronze
# MAGIC ## PHASE 1 — Intermédiaire
# MAGIC
# MAGIC **Objectif** : Ingérer les fichiers CSV bruts dans des tables Delta Bronze.
# MAGIC Aucune transformation — on stocke les données telles quelles avec métadonnées d'audit.
# MAGIC
# MAGIC ### Concepts couverts
# MAGIC - Lecture de CSV avec inférence de schema
# MAGIC - Écriture en **Delta Lake** (mode append, overwrite)
# MAGIC - **AutoLoader** (`cloudFiles`) pour l'ingestion incrémentale
# MAGIC - Ajout de colonnes d'audit (`_ingested_at`, `_source_file`)
# MAGIC - **Schema enforcement** vs **Schema evolution**
# MAGIC - `DESCRIBE HISTORY` et **Time Travel**

# COMMAND ----------

dbutils.widgets.text("catalog",        "energy_catalog",    "Catalog Unity Catalog")
dbutils.widgets.text("volume_path",    "/Volumes/energy_catalog/landing_energy/raw_files", "Chemin Volume") 


# COMMAND ----------

# Paramètres (récupérés depuis les widgets si disponibles)
try:
    VOLUME_PATH = dbutils.widgets.get("volume_path")
    CATALOG     = dbutils.widgets.get("catalog")
except:
    VOLUME_PATH = "/Volumes/energy_catalog/landing_energy/raw_files"
    CATALOG     = "energy_catalog"

BRONZE_SCHEMA = f"{CATALOG}.bronze_energy"

print(f"Source      : {VOLUME_PATH}")
print(f"Destination : {BRONZE_SCHEMA}")

# COMMAND ----------

print(dbutils.widgets.get("volume_path"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.1 — Ingestion simple (batch) : table `sites_raw`
# MAGIC
# MAGIC On commence par le référentiel des sites — c'est une petite table statique,
# MAGIC idéale pour pratiquer l'écriture Delta en mode batch.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime


df___test= (spark.read
            .option("header","true")
            .option("inferSchema","true")
            .csv(f"{VOLUME_PATH}/sites.csv")
)


df___test.display()


# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime


VOLUME_PATH = dbutils.widgets.get("volume_path")


df_sites = (spark.read
            .option("header","true")
            .option("inferShema","true")
            .csv(f"{VOLUME_PATH}/sites.csv")
            )  




###df_=spark.read.csv(f"{VOLUME_PATH}/sites.csv", header=True)


print(f" Lignes lues: {df_sites.count()}")

df_sites.printSchema()
df_sites.display()



# COMMAND ----------

# ── Ajout des colonnes d'audit (bonne pratique Bronze) ────────────────────


# Règle d'or Bronze : NE JAMAIS modifier les données sources, uniquement AJOUTER des métadonnées d'ingestion.


df_sites_bronze=df_sites.withColumns({
    "_ingested_at": F.current_timestamp(),
    "_source_file": F.lit("sites.csv"),
    "_batch_id": F.lit(datetime.now().strftime("%Y%m%d_%H%M%S")),

})



df_sites_bronze.printSchema() 
df_sites_bronze.display()

# COMMAND ----------

df_sites_bronze.display()

# COMMAND ----------

#Ecriture en table delta

(
    df_sites_bronze.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema","true")
    .saveAsTable(f"{BRONZE_SCHEMA}.sites_raw")

)

print(f"Table {BRONZE_SCHEMA}.sites_raw créée")


# COMMAND ----------


df__=spark.read.table(f"{BRONZE_SCHEMA}.sites_raw")


df__.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.2 — Ingestion avec schema explicite : `solar_raw`
# MAGIC
# MAGIC Pour les grosses tables, on définit le schema **à la main** plutôt que d'utiliser
# MAGIC `inferSchema` (plus lent car il scanne le fichier entier).
# MAGIC
# MAGIC > 💡 En production : toujours définir le schema explicitement.

# COMMAND ----------

# ── Définition du schema explicite ────────────────────────────────────────
solar_schema = StructType([
    StructField("timestamp",        TimestampType(), nullable=False),
    StructField("site_id",          StringType(),    nullable=False),
    StructField("panel_temp_c",     DoubleType(),    nullable=True),
    StructField("irradiance_w_m2",  DoubleType(),    nullable=True),
    StructField("production_kwh",   DoubleType(),    nullable=True),
    StructField("status",           StringType(),    nullable=True),
])

# ── Lecture ────────────────────────────────────────────────────────────────
df_solar = (
    spark.read
    .option("header", "true")
    .schema(solar_schema)
    .csv(f"{VOLUME_PATH}/solar_production.csv")
)

print(f"📊 Lignes solar : {df_solar.count():,}")

# ── Ajout métadonnées + écriture Bronze ───────────────────────────────────
df_solar_bronze = df_solar.withColumns({
    "_ingested_at": F.current_timestamp(),
    "_source_file": F.col("_metadata.file_path"),         # capture le nom de fichier réel
    "_year":        F.year("timestamp"),          # partition hint pour optimisation
    "_month":       F.month("timestamp"),
})

(
    df_solar_bronze
    .write
    .format("delta")
    .mode("overwrite")
    .partitionBy("_year", "_month")              # partitionnement par date
    .option("overwriteSchema", "true")
    .saveAsTable(f"{BRONZE_SCHEMA}.solar_raw")
)
print(f"✅ Table {BRONZE_SCHEMA}.solar_raw créée avec partitions year/month")

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog energy_catalog;

# COMMAND ----------

#df__dd=spark.read.table(f"{BRONZE_SCHEMA}.solar_raw")
###energy_catalog.bronze_energy.solar_raw
df__dd=spark.read.table("energy_catalog.bronze_energy.solar_raw")
df__dd.show() 


# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime

from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, IntegerType, TimestampType
)

# ── Même chose pour wind_production ───────────────────────────────────────
wind_schema = StructType([
    StructField("timestamp",           TimestampType(), nullable=False),
    StructField("site_id",             StringType(),    nullable=False),
    StructField("wind_speed_ms",       DoubleType(),    nullable=True),
    StructField("wind_direction_deg",  IntegerType(),   nullable=True),
    StructField("production_kwh",      DoubleType(),    nullable=True),
    StructField("status",              StringType(),    nullable=True),
])

df_wind = (
    spark.read
    .option("header", "true")
    .schema(wind_schema)
    .csv(f"{VOLUME_PATH}/wind_production.csv")
    .select("*", "_metadata")
)

df_wind_bronze = df_wind.withColumns({
    "_ingested_at": F.current_timestamp(),
    "_source_file": F.col("_metadata.file_path"),
    "_year":        F.year("timestamp"),
    "_month":       F.month("timestamp"),
    
})


(
    df_wind_bronze
    .write
    .format("delta")
    .mode("overwrite")
    .partitionBy("_year", "_month")
    .option("overwriteSchema", "true")
    .saveAsTable("energy_catalog.bronze_energy.wind_raw")
)
print(f"✅ Table {BRONZE_SCHEMA}.wind_raw créée")

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended energy_catalog.bronze_energy.wind_raw;

# COMMAND ----------

df_wind_bronze.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.3 — AutoLoader (ingestion incrémentale)
# MAGIC
# MAGIC **AutoLoader** (`cloudFiles`) surveille un répertoire et ingère les **nouveaux fichiers**
# MAGIC automatiquement, sans rescanner les fichiers déjà traités.
# MAGIC
# MAGIC C'est la méthode recommandée en production pour l'ingestion continue.
# MAGIC
# MAGIC > ⚠️ AutoLoader nécessite un checkpoint. Ne jamais réutiliser le même checkpoint
# MAGIC > pour des tables différentes !

# COMMAND ----------

# MAGIC %md
# MAGIC  💡 En mode AutoLoader streaming, on utilise `writeStream` avec `trigger(availableNow=True)`
# MAGIC pour traiter tous les fichiers disponibles d'un coup (mode "batch incrémental").
# MAGIC > C'est plus efficace que de laisser tourner le stream en continu pour des pipelines batch.

# COMMAND ----------

###CHECKPOINT_SOLAR = "/tmp/solarwind/checkpoints/solar_autoloader"
#CHECKPOINT_SOLAR = "/Volumes/solarwind/checkpoints/solar_autoloader" 
CHECKPOINT_SOLAR="/Volumes/energy_catalog/bronze_energy/checkpoints/solar"

# AutoLoader en mode streaming (readStream → writeStream)



df_solar_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", f"{CHECKPOINT_SOLAR}/schema")
    .option("header", "true")
    .schema(solar_schema)
    .load(f"{VOLUME_PATH}/")                    # surveille le répertoire entier
)

# Ajout des métadonnées
df_solar_stream_enriched = df_solar_stream.withColumns({
    "_ingested_at": F.current_timestamp(),
    "_source_file": F.col("_metadata.file_path"),   # AutoLoader metadata column
})



# COMMAND ----------

# MAGIC %md
# MAGIC # ⚠️ Ce bloc est en commentaire pour éviter un vrai stream en révision
# MAGIC # Il faut décommenter en production
# MAGIC # 
# MAGIC # query = (
# MAGIC #     df_solar_stream_enriched
# MAGIC #     .writeStream
# MAGIC #     .format("delta")
# MAGIC #     .outputMode("append")
# MAGIC #     .option("checkpointLocation", CHECKPOINT_SOLAR)
# MAGIC #     .trigger(availableNow=True)          # traite les fichiers dispo et s'arrête
# MAGIC #     .toTable(f"{BRONZE_SCHEMA}.solar_raw_stream")
# MAGIC # )
# MAGIC # query.awaitTermination()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME energy_catalog.bronze_energy.checkpoints;

# COMMAND ----------

CHECKPOINT_SOLAR = "/Volumes/energy_catalog/bronze_energy/checkpoints/solar"

query = (
    df_solar_stream_enriched
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_SOLAR)
    .trigger(availableNow=True)
    .toTable("energy_catalog.bronze_energy.solar_raw_stream")
)

query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC 2 types de “streaming” dans Databricks
# MAGIC 1️⃣ Le cas (ce qu'on fait la dans l'example)
# MAGIC .writeStream.toTable(...)
# MAGIC
# MAGIC 👉 Résultat :
# MAGIC
# MAGIC ✅ table Delta classique
# MAGIC ❌ pas de type STREAMING_TABLE
# MAGIC ❌ pas de pipeline ID
# MAGIC
# MAGIC ➡️ Streaming = au niveau du job Spark uniquement
# MAGIC
# MAGIC
# MAGIC 2️⃣ Le cas de la STREAMING_TABLE 
# MAGIC
# MAGIC 👉 Ça vient de :
# MAGIC
# MAGIC ➡️ Delta Live Tables (DLT)
# MAGIC ou
# MAGIC ➡️ Databricks Pipelines (Lakeflow / pipelines UI)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.4 — Time Travel et propriétés Delta
# MAGIC
# MAGIC Delta Lake enregistre **chaque opération** dans son transaction log.
# MAGIC C'est l'une de ses fonctionnalités les plus puissantes.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Historique complet des opérations sur la table Bronze solar
# MAGIC DESCRIBE HISTORY energy_catalog.bronze_energy.solar_raw;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Time Travel : lire la version 0 (état initial)
# MAGIC SELECT COUNT(*) as nb_lignes_v0
# MAGIC FROM energy_catalog.bronze_energy.solar_raw VERSION AS OF 0;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Propriétés de la table Delta
# MAGIC DESCRIBE EXTENDED energy_catalog.bronze_energy.solar_raw;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Voir les partitions créées
# MAGIC SHOW PARTITIONS energy_catalog.bronze_energy.solar_raw;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.5 — Vérifications de qualité Bronze (basiques)
# MAGIC
# MAGIC En Bronze, on ne rejette pas les données mais on **mesure** leur qualité.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Tableau de bord qualité Bronze
# MAGIC SELECT
# MAGIC     'solar_raw'             AS table_name,
# MAGIC     COUNT(*)                AS total_rows,
# MAGIC     COUNT(DISTINCT site_id) AS distinct_sites,
# MAGIC     SUM(CASE WHEN production_kwh IS NULL THEN 1 ELSE 0 END) AS null_production,
# MAGIC     SUM(CASE WHEN production_kwh < 0    THEN 1 ELSE 0 END) AS negative_production,
# MAGIC     SUM(CASE WHEN status = 'OFFLINE'    THEN 1 ELSE 0 END) AS offline_records,
# MAGIC     MIN(timestamp) AS min_ts,
# MAGIC     MAX(timestamp) AS max_ts
# MAGIC FROM energy_catalog.bronze_energy.solar_raw
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC     'wind_raw',
# MAGIC     COUNT(*),
# MAGIC     COUNT(DISTINCT site_id),
# MAGIC     SUM(CASE WHEN production_kwh IS NULL THEN 1 ELSE 0 END),
# MAGIC     SUM(CASE WHEN production_kwh < 0    THEN 1 ELSE 0 END),
# MAGIC     SUM(CASE WHEN status = 'OFFLINE'    THEN 1 ELSE 0 END),
# MAGIC     MIN(timestamp) AS min_ts,
# MAGIC     MAX(timestamp) AS max_ts
# MAGIC FROM energy_catalog.bronze_energy.wind_raw;

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Récapitulatif Bronze
# MAGIC
# MAGIC A l'issue de cet example, nous avons ingéré :
# MAGIC - `bronze.sites_raw` — 6 sites (overwrite, pas de partitions)
# MAGIC - `bronze.solar_raw` — ~12 960 lignes, partitionné par year/month
# MAGIC - `bronze.wind_raw`  — ~12 960 lignes, partitionné par year/month
# MAGIC  
