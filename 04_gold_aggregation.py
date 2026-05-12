# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Agrégation Couche Gold
# MAGIC ## PHASE 1 — Intermédiaire / Avancé
# MAGIC
# MAGIC **Objectif** : Construire les KPIs métier prêts pour la BI, le reporting et le ML.
# MAGIC
# MAGIC ### Concepts couverts
# MAGIC - **Window Functions** avancées (LAG, LEAD, rolling averages)
# MAGIC - Agrégations Spark SQL complexes
# MAGIC - **Vues matérialisées** (Dynamic Tables)
# MAGIC - **OPTIMIZE** ciblé + statistiques de colonnes
# MAGIC - Écriture Gold optimisée pour la lecture BI

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

CATALOG       = "energy_catalog"
SILVER_SCHEMA = f"{CATALOG}.silver_energy"
GOLD_SCHEMA   = f"{CATALOG}.gold_energy"


df_silver = spark.table(f"{SILVER_SCHEMA}.production")
### df_silver.cache()  # mise en cache car on va l'utiliser plusieurs fois
print(f"Silver : {df_silver.count():,} lignes")


print(f"Le nombre total de lignes est de {df_silver.count()} lignes ")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.1 — Table Gold 1 : KPIs journaliers par site
# MAGIC
# MAGIC Table principale de reporting : une ligne par `(date, site_id)`.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE energy_catalog.gold_energy.daily_kpis
# MAGIC COMMENT "KPIs journaliers de production par site -- gold layer"
# MAGIC TBLPROPERTIES (
# MAGIC   
# MAGIC   'delta.autoOptimize.optimizeWrite'='true' , -- optimise auto à l'écriture
# MAGIC   'delta.autoOptimize.autoCompact'=  'true' -- compacte auto les petits fichiers smalls files
# MAGIC   )
# MAGIC AS SELECT 
# MAGIC p.date,
# MAGIC p.site_id,
# MAGIC p.technology,
# MAGIC s.site_name,
# MAGIC s.region,
# MAGIC CAST(s.capacity_mw AS DOUBLE),
# MAGIC
# MAGIC --production
# MAGIC ROUND(SUM(p.production_kwh),2) AS total_production_kwh,
# MAGIC ROUND(SUM(p.production_kwh)/1000,4) AS total_production_mwh,
# MAGIC ROUND(AVG(p.production_kwh),2) AS avg_hourly_production_kwh,
# MAGIC ROUND(MAX(p.production_kwh),2) AS peak_production_kwh,
# MAGIC
# MAGIC COUNT(*) AS total_hours,
# MAGIC SUM(CASE WHEN p.status='OK' Then 1 else 0 END) AS hours_ok, 
# MAGIC SUM(CASE WHEN p.status='OFFLINE' THEN  1 ELSE 0 END)  AS hours_offline,
# MAGIC SUM(CASE WHEN p.status='DEGRADED' THEN 1 ELSE 0 END) AS hours_degraded,
# MAGIC ROUND(SUM(CASE WHEN p.status != 'OFFLINE' THEN 1 ELSE 0 END) *100.0 / count(*),1) AS availability_pct,
# MAGIC
# MAGIC ROUND(SUM(p.production_kwh)  /(CAST(s.capacity_mw AS DOUBLE) * 1000 *24)https://dbc-c39ce79b-f4ee.cloud.databricks.com/editor/notebooks/42598871485839?o=686281376611008$0*100 , 2) AS daily_capacity_factor_pct,
# MAGIC --Metadonnées
# MAGIC
# MAGIC current_timestamp() AS computed_at
# MAGIC
# MAGIC
# MAGIC FROM energy_catalog.silver_energy.production p 
# MAGIC LEFT JOIN energy_catalog.bronze_energy.sites_raw s 
# MAGIC USING (site_id)
# MAGIC GROUP BY p.date, p.site_id, p.technology, s.site_name, s.region, s.capacity_mw;
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from energy_catalog.gold_energy.daily_kpis

# COMMAND ----------

# MAGIC
# MAGIC
# MAGIC %sql
# MAGIC SELECT * FROM energy_catalog.gold_energy.daily_kpis
# MAGIC ORDER BY date DESC, total_production_mwh DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.2 — Window Functions : rolling averages et comparaisons temporelles
# MAGIC
# MAGIC **Objectif:** Enrichir les KPIs
# MAGIC
# MAGIC
# MAGIC On enrichit les KPIs journaliers avec des indicateurs temporels :
# MAGIC - Moyenne glissante sur 7 jours
# MAGIC - Production vs jour précédent (LAG)
# MAGIC - Rang du site par jour (dense_rank)

# COMMAND ----------

from pyspark.sql import functions as F

GOLD_SCHEMA="energy_catalog.gold_energy"
df_gold = spark.table(f"{GOLD_SCHEMA}.daily_kpis")

from pyspark.sql.window import Window 

# ── Fenêtres temporelles ──────────────────────────────────────────────────


w_site_time = (
    Window.partitionBy("site_id")
    .orderBy("date")
)
########### 

w_site_7d = (
    Window.partitionBy("site_id")
    .orderBy("date")
    .rowsBetween(-6, 0)       # 7 jours glissants
)


##### a reproduire tout seul
######
w_site___7days=Window.partitionBy("site_id").orderBy("date").rowsBetween(-6, 0)



w_daily_rank = (
    Window.partitionBy("date", "technology")
    .orderBy(F.col("total_production_mwh").desc())
)

 
df_gold_enriched = (
    df_gold
    # Variation vs j-1
    .withColumn("prev_day_production_mwh",F.lag("total_production_mwh", 1).over(w_site_time))
    .withColumn("day_over_day_change_pct",
        F.round(
            (F.col("total_production_mwh") - F.col("prev_day_production_mwh"))
            / F.nullif(F.col("prev_day_production_mwh"), F.lit(0)) * 100,
            2
        ))
    # Moyenne glissante 7j
    .withColumn("rolling_7d_avg_mwh",
        F.round(F.avg("total_production_mwh").over(w_site_7d), 4))
    # Rang par jour
    .withColumn("daily_rank_in_technology",
        F.dense_rank().over(w_daily_rank))
    

)

df_gold_enriched.display()




# Sauvegarde enrichie
(
    df_gold_enriched
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("technology")
    .saveAsTable(f"{GOLD_SCHEMA}.daily_kpis_enriched")
)
print(f"✅ {GOLD_SCHEMA}.daily_kpis_enriched créée")


 




# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.3 — Table Gold 2 : Rapport d'efficacité par site (vue consolidée)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE energy_catalog.gold_energy.site_efficiency_report
# MAGIC COMMENT "Rapport d'efficacité mensuel agrégé par site"
# MAGIC AS
# MAGIC SELECT
# MAGIC     site_id,
# MAGIC     site_name,
# MAGIC     region,
# MAGIC     technology,
# MAGIC     capacity_mw,
# MAGIC     YEAR(date)                                              AS year,
# MAGIC     MONTH(date)                                             AS month,
# MAGIC     DATE_FORMAT(date, 'yyyy-MM')                            AS month_label,
# MAGIC
# MAGIC     -- Production mensuelle
# MAGIC     ROUND(SUM(total_production_mwh), 2)                     AS monthly_production_mwh,
# MAGIC     ROUND(AVG(daily_capacity_factor_pct), 2)                AS avg_capacity_factor_pct,
# MAGIC     ROUND(AVG(availability_pct), 1)                         AS avg_availability_pct,
# MAGIC
# MAGIC     -- Jours critiques
# MAGIC     SUM(CASE WHEN hours_offline > 0 THEN 1 ELSE 0 END)      AS days_with_outage,
# MAGIC     MAX(hours_offline)                                       AS max_daily_offline_hours,
# MAGIC
# MAGIC     -- Score de performance composite (0-100)
# MAGIC     ROUND(
# MAGIC         AVG(availability_pct) * 0.4 +
# MAGIC         AVG(daily_capacity_factor_pct) * 0.6,
# MAGIC         1
# MAGIC     )                                                        AS performance_score,
# MAGIC
# MAGIC     current_timestamp()                                      AS computed_at
# MAGIC
# MAGIC FROM energy_catalog.gold_energy.daily_kpis
# MAGIC GROUP BY
# MAGIC     site_id, site_name, region, technology, capacity_mw,
# MAGIC     YEAR(date), MONTH(date), DATE_FORMAT(date, 'yyyy-MM')
# MAGIC ORDER BY year, month, technology, performance_score DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Vue du rapport d'efficacité
# MAGIC SELECT * FROM energy_catalog.gold_energy.site_efficiency_report
# MAGIC ORDER BY month_label, performance_score DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.4 — Anomaly Detection simple (Spark SQL)
# MAGIC
# MAGIC Identifie les journées anormalement basses par rapport à la moyenne 30j.

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH rolling_stats AS (
# MAGIC     SELECT
# MAGIC         date,
# MAGIC         site_id,
# MAGIC         technology,
# MAGIC         total_production_mwh,
# MAGIC         -- Moyenne et écart-type glissants sur 30j
# MAGIC         AVG(total_production_mwh)
# MAGIC             OVER (PARTITION BY site_id ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)
# MAGIC             AS rolling_30d_avg,
# MAGIC         STDDEV(total_production_mwh)
# MAGIC             OVER (PARTITION BY site_id ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)
# MAGIC             AS rolling_30d_std
# MAGIC     FROM energy_catalog.gold_energy.daily_kpis
# MAGIC ),
# MAGIC anomalies AS (
# MAGIC     SELECT *,
# MAGIC         ROUND((total_production_mwh - rolling_30d_avg) / NULLIF(rolling_30d_std, 0), 2) AS z_score
# MAGIC     FROM rolling_stats
# MAGIC )
# MAGIC SELECT
# MAGIC     date, site_id, technology,
# MAGIC     ROUND(total_production_mwh, 3) AS production_mwh,
# MAGIC     ROUND(rolling_30d_avg, 3)      AS avg_30d,
# MAGIC     z_score,
# MAGIC     CASE
# MAGIC         WHEN z_score < -2 THEN '🔴 ANOMALIE BASSE'
# MAGIC         WHEN z_score < -1 THEN '🟡 Production faible'
# MAGIC         WHEN z_score >  2 THEN '🔵 Production exceptionnelle'
# MAGIC         ELSE '✅ Normal'
# MAGIC     END AS alert_level
# MAGIC FROM anomalies
# MAGIC WHERE ABS(z_score) > 1.5
# MAGIC ORDER BY ABS(z_score) DESC
# MAGIC LIMIT 30;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.5 — OPTIMIZE final sur les tables Gold

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE energy_catalog.gold_energy.daily_kpis_enriched
# MAGIC ZORDER BY (date, site_id);

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE energy_catalog.gold_energy.site_efficiency_report
# MAGIC ZORDER BY (month_label, site_id);

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from energy_catalog.gold_energy.site_efficiency_report;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC describe history energy_catalog.gold_energy.site_efficiency_report;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.6 — Inventaire final des tables créées

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT table_catalog, table_schema, table_name, table_type,
# MAGIC        created, last_altered
# MAGIC FROM energy_catalog.information_schema.tables
# MAGIC WHERE table_catalog = 'energy_catalog'
# MAGIC ORDER BY table_schema, table_name;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from energy_catalog.gold_energy.daily_kpis;

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Récapitulatif Gold — Phase 1 complète !
# MAGIC
# MAGIC Nous avons construit une **architecture Data/Medallion complète** :
# MAGIC
# MAGIC ```
# MAGIC Bronze (3 tables) → Silver (1 table unifiée) → Gold (3 tables KPI)
# MAGIC ```
# MAGIC
# MAGIC

# COMMAND ----------

df = spark.createDataFrame([
    (1, 100), (2, 100), (3, 100), (4, 95), (5, 90)
], ["id", "value"])

df.display()
df.printSchema()

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql import functions as F

window_spec=Window.orderBy(F.desc("value"))

df_tmp=df.withColumn("dense_rank", F.dense_rank().over(window_spec))\
    .withColumn("rank", F.rank().over(window_spec))\
    .withColumn("row_number", F.row_number().over(window_spec))


df_tmp.display()



# COMMAND ----------


from pyspark.sql.window import Window 
from pyspark.sql import functions as F

 

window_spec=Window.orderBy(F.desc("value"))


df_resultant=df.withColumn("dense_rank",F.dense_rank().over(window_spec))\
    .withColumn("rank", F.rank().over(window_spec))\
    .withColumn("row_number", F.row_number().over(window_spec))

df_resultant.show()

# COMMAND ----------


