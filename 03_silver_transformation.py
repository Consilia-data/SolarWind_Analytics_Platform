# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Transformation Couche Silver
# MAGIC ## PHASE 1 — Intermédiaire
# MAGIC
# MAGIC **Objectif** : Transformer les données Bronze en données Silver :
# MAGIC nettoyées, dédupliquées, enrichies, typées correctement, avec contraintes de qualité.
# MAGIC
# MAGIC ### Concepts couverts
# MAGIC
# MAGIC - **Déduplication** avec `dropDuplicates` et fenêtres
# MAGIC - **MERGE INTO** (upsert Delta)
# MAGIC - **Table Constraints** (`NOT NULL`, `CHECK`)
# MAGIC - Union de tables hétérogènes (solar + wind)
# MAGIC - Enrichissement par jointure (lookup vers `sites_raw`)
# MAGIC - Gestion des valeurs aberrantes

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

CATALOG= "energy_catalog"
BRONZE_SCHEMA = f"{CATALOG}.bronze_energy"
SILVER_SCHEMA = f"{CATALOG}.silver_energy"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.1 — Lecture et nettoyage des données Solar

# COMMAND ----------

# Lecture depuis Bronze


df_solar_raw = spark.table(f"{BRONZE_SCHEMA}.solar_raw")

print(f"Bronze Solar:  {df_solar_raw.distinct().count():,} lignes")

df_solar_raw.printSchema()



# COMMAND ----------

# MAGIC %md
# MAGIC ### Étapes de nettoyage Solar
# MAGIC
# MAGIC | Problème | Solution |
# MAGIC |----------|----------|
# MAGIC | Doublons (même timestamp + site_id) | `dropDuplicates` puis window dedup |
# MAGIC | `production_kwh` négatifs | Remplace par `NULL` |
# MAGIC | `status` inconnu | Standardise en `UNKNOWN` |
# MAGIC | Colonnes d'audit Bronze (`_*`) | Supprime pour Silver |

# COMMAND ----------


# ── 1. Supprime les colonnes d'audit Bronze ───────────────────────────────
audit_cols = [c for c in df_solar_raw.columns if c.startswith("_")]

df_solar = df_solar_raw.drop(*audit_cols)

# ── 2. Nettoyage des valeurs ──────────────────────────────────────────────
 
df_solar_clean = (df_solar
    .withColumn("production_kwh",F.when(F.col("production_kwh") < 0, F.lit(None)).otherwise(F.col("production_kwh")))
    # Irradiance négative → NULL
    .withColumn("irradiance_w_m2",F.when(F.col("irradiance_w_m2") < 0, F.lit(None)).otherwise(F.col("irradiance_w_m2")))
    # Standardise les statuts
    .withColumn("status", F.when(F.col("status").isin("OK", "DEGRADED", "OFFLINE"), F.col("status")).otherwise(F.lit("UNKNOWN")))
)


# ── 3. Déduplication par clé naturelle (timestamp + site_id) ────────────── 

w = Window.partitionBy("timestamp", "site_id").orderBy(F.col("production_kwh").desc_nulls_last())

df_solar_deduped = (
    df_solar_clean
    .withColumn("_rn", F.row_number().over(w))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
)

# ── 4. Ajout de colonnes enrichies ────────────────────────────────────────
df_solar_silver = (
    df_solar_deduped
    .withColumn("technology",     F.lit("solar"))
    .withColumn("date",           F.to_date("timestamp"))
    .withColumn("hour",           F.hour("timestamp"))
    .withColumn("is_production",  F.col("production_kwh") > 0)
    # Efficacité simplifiée : ratio production / irradiance normalisée
    .withColumn("efficiency_pct",
        F.when((F.col("irradiance_w_m2") > 0) & F.col("production_kwh").isNotNull(),
               F.round(F.col("production_kwh") / (F.col("irradiance_w_m2") * 0.01), 2))
         .otherwise(F.lit(None).cast("double")))
    # Renomme les colonnes spécifiques solar avec préfixe pour la table unifiée
    .withColumnRenamed("panel_temp_c",    "sensor_metric_1")   # temp panneau
    .withColumnRenamed("irradiance_w_m2", "sensor_metric_2")   # irradiance
)

print(f"Silver solar après nettoyage : {df_solar_silver.count():,} lignes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.2 — Lecture et nettoyage des données Wind

# COMMAND ----------



######Audit colonnes 
df_wind_raw = spark.table(f"{BRONZE_SCHEMA}.wind_raw")
audit_cols  = [c for c in df_wind_raw.columns if c.startswith("_")]
df_wind     = df_wind_raw.drop(*audit_cols)



df_wind_clean = (
    df_wind
    .withColumn("production_kwh",
        F.when(F.col("production_kwh") < 0, F.lit(None))
         .otherwise(F.col("production_kwh")))
    .withColumn("wind_speed_ms",
        F.when(F.col("wind_speed_ms") < 0, F.lit(None))
         .otherwise(F.col("wind_speed_ms")))
    .withColumn("status",
        F.when(F.col("status").isin("OK", "DEGRADED", "OFFLINE"), F.col("status"))
         .otherwise(F.lit("UNKNOWN")))
)

 

w = Window.partitionBy("timestamp", "site_id").orderBy(F.col("production_kwh").desc_nulls_last())

df_wind_silver = (
    df_wind_clean
    .withColumn("_rn", F.row_number().over(w))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
    .withColumn("technology",     F.lit("wind"))
    .withColumn("date",           F.to_date("timestamp"))
    .withColumn("hour",           F.hour("timestamp"))
    .withColumn("is_production",  F.col("production_kwh") > 0)
    .withColumn("efficiency_pct", F.lit(None).cast("double"))  # calculé différemment
    .withColumnRenamed("wind_speed_ms",      "sensor_metric_1")  # vitesse vent
    .withColumnRenamed("wind_direction_deg", "sensor_metric_2")  # direction
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.3 — Union Solar + Wind en table unifiée
# MAGIC
# MAGIC On crée une table Silver **unifiée** `production` qui contient solar ET wind.
# MAGIC C'est un pattern courant : une seule table "faits" avec une colonne discriminante.

# COMMAND ----------

# Sélectionne uniquement les colonnes communes
common_cols = [
    "timestamp", "site_id", "technology",
    "production_kwh", "status",
    "sensor_metric_1", "sensor_metric_2",
    "date", "hour", "is_production", "efficiency_pct"]

df_production = (df_solar_silver.select(common_cols).union(df_wind_silver.select(common_cols)))

print(f"Table unifiée Silver production : {df_production.count():,} lignes")

df_production.groupBy("technology").count().display()

df_production.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.4 — Enrichissement par jointure avec les sites
# MAGIC
# MAGIC On joint avec `sites_raw` pour ajouter les informations métier
# MAGIC (région, capacité installée...) directement dans Silver.

# COMMAND ----------

# df_sites = spark.table(f"{BRONZE_SCHEMA}.sites_raw").select(
#     "site_id", "site_name", "region", "capacity_mw", "latitude", "longitude")

df_sites = spark.table(f"{BRONZE_SCHEMA}.sites_raw").select(
    "site_id",
    "site_name",
    "region",
    F.col("capacity_mw").cast("double").alias("capacity_mw"),
    F.col("latitude").cast("double").alias("latitude"),
    F.col("longitude").cast("double").alias("longitude")
)

# Broadcast join : sites_raw est petite → on la diffuse sur tous les workers
df_production_enriched = ( df_production.join(F.broadcast(df_sites), on="site_id", how="left")
    # Calcule le taux d'utilisation (production / capacité théorique max)
    .withColumn("capacity_factor_pct",
        F.when(F.col("capacity_mw") > 0,
               F.round(F.col("production_kwh") / (F.col("capacity_mw") * 1000) * 100, 2))
         .otherwise(F.lit(None).cast("double")))
)



from pyspark.sql import functions as F

df_jointure_left=(df_production.joint(F.broadcast(df_sites), on="sites_id", how="Left")
                  .withColumn("capacity_factor_pct", F.when(F.col("capacity_mw")>0, 
                                                            F.round(F.col("production_kwh")/F.col("capcity_mw")*1000) *100,2))
                  .otherwise(F.lit(None).cast("double"))
                    
                  
                  )

 


print("Schema final Silver :")
df_production_enriched.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.5 — Écriture Silver avec MERGE INTO
# MAGIC
# MAGIC On utilise **MERGE INTO** pour faire un upsert :
# MAGIC - Si la ligne existe déjà (même `timestamp` + `site_id`) → UPDATE
# MAGIC - Sinon → INSERT
# MAGIC
# MAGIC C'est le pattern idéal pour les retraitements partiels sans doublons.

# COMMAND ----------

# Première exécution : crée la table Silver si elle n'existe pas
(
    df_production_enriched
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("technology", "date")
    .saveAsTable(f"{SILVER_SCHEMA}.production")
)


print(f"✅ Table {SILVER_SCHEMA}.production créée")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pattern MERGE INTO (pour les runs suivants)
# MAGIC
# MAGIC ```python
# MAGIC # Crée une vue temporaire de la nouvelle donnée
# MAGIC df_production_enriched.createOrReplaceTempView("production_updates")
# MAGIC ```

# COMMAND ----------

df_production_enriched.createOrReplaceTempView("production_updates")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- MERGE INTO : upsert idempotent
# MAGIC -- Décommente ce bloc pour les ingestions incrémentales
# MAGIC
# MAGIC MERGE INTO energy_catalog.silver_energy.production AS target
# MAGIC USING production_updates AS source
# MAGIC ON target.timestamp = source.timestamp
# MAGIC AND target.site_id  = source.site_id
# MAGIC
# MAGIC WHEN MATCHED AND source.production_kwh != target.production_kwh THEN
# MAGIC   UPDATE SET *                              -- met à jour toutes les colonnes
# MAGIC
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT *                                  -- insère la nouvelle ligne
# MAGIC
# MAGIC WHEN NOT MATCHED BY SOURCE
# MAGIC AND target.date >= current_date() - INTERVAL 7 DAYS THEN
# MAGIC   DELETE;                                   -- supprime les données récentes absentes
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.6 — Ajout de contraintes de qualité sur la table Silver

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Contrainte : production ne peut pas être négative
# MAGIC ALTER TABLE energy_catalog.silver_energy.production
# MAGIC ADD CONSTRAINT chk_production_positive
# MAGIC CHECK (production_kwh IS NULL OR production_kwh >= 0);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Contrainte : technology doit être 'solar' ou 'wind'
# MAGIC ALTER TABLE energy_catalog.silver_energy.production
# MAGIC ADD CONSTRAINT chk_technology_valid
# MAGIC CHECK (technology IN ('solar', 'wind'));

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Vérifie les contraintes
# MAGIC DESCRIBE EXTENDED energy_catalog.silver_energy.production;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.7 — OPTIMIZE et Z-ORDER
# MAGIC
# MAGIC **Z-ORDER** organise les fichiers Delta physiquement selon les colonnes fréquemment
# MAGIC utilisées en filtre. Améliore drastiquement les performances de lecture sur les
# MAGIC requêtes filtrées.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Optimise la table et Z-ORDER sur les colonnes de filtre les plus fréquentes
# MAGIC OPTIMIZE energy_catalog.silver_energy.production
# MAGIC ZORDER BY (site_id, timestamp);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Vérifie le résultat de l'optimisation
# MAGIC DESCRIBE HISTORY energy_catalog.silver_energy.production LIMIT 3;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.8 — Validation finale de la chaine Silver (architecture médaillon)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     technology,
# MAGIC     COUNT(*)                                        AS total_records,
# MAGIC     COUNT(DISTINCT site_id)                         AS nb_sites,
# MAGIC     ROUND(AVG(production_kwh), 2)                   AS avg_production_kwh,
# MAGIC     ROUND(MAX(production_kwh), 2)                   AS max_production_kwh,
# MAGIC     SUM(CASE WHEN status = 'OFFLINE' THEN 1 END)    AS offline_count,
# MAGIC     SUM(CASE WHEN production_kwh IS NULL THEN 1 END) AS null_production_count
# MAGIC FROM energy_catalog.silver_energy.production
# MAGIC GROUP BY technology
# MAGIC ORDER BY technology;
