# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Setup Unity Catalog
# MAGIC ## PHASE 1 — Intermédiaire
# MAGIC
# MAGIC **Objectif** : Créer la structure de catalogues et de schémas qui servira de socle
# MAGIC à toute la plateforme SolarWind Analytics.
# MAGIC
# MAGIC ### Ce que tu vas apprendre
# MAGIC - Créer un **catalog** Unity Catalog
# MAGIC - Créer des **schemas** (Bronze / Silver / Gold)
# MAGIC - Créer un **Volume** pour stocker les fichiers sources
# MAGIC - Poser des **tags** de gouvernance dès le départ
# MAGIC
# MAGIC > ⚠️ **Prérequis** : ton workspace doit être activé sur Unity Catalog.
# MAGIC > Vérifie dans *Settings > Admin Console > Unity Catalog*.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.1 — Vérification de l'environnement

# COMMAND ----------

# Vérifie la version de Databricks Runtime


import sys

print(f"Python : {sys.version}")
print(f"Spark  : {spark.version}")

# Vérifie que Unity Catalog est disponible
spark.sql("SHOW CATALOGS").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.2 — Création du Catalog principal
# MAGIC
# MAGIC Un **catalog** est le niveau le plus haut dans Unity Catalog.
# MAGIC Il regroupe tous les schemas (bases de données) du projet.
# MAGIC
# MAGIC Syntaxe : `CREATE CATALOG IF NOT EXISTS <nom>`

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Crée le catalog dédié au projet énergie
# MAGIC CREATE CATALOG IF NOT EXISTS energy_catalog
# MAGIC COMMENT "Catalog principal de la plateforme SolarWind Analytics";

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Pose un tag sur le catalog (bonne pratique de gouvernance)
# MAGIC ALTER CATALOG energy_catalog
# MAGIC SET TAGS ('project' = 'solarwind', 'env' = 'dev', 'team' = 'data-engineering');
# MAGIC
# MAGIC
# MAGIC --ALTER CATALOG energy_catalog SET TAGS ('projects'='solarwind', 'env'='dev', 'team'='data-enginnering');

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE CATALOG EXTENDED energy_catalog;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.3 — Création des Schemas (couches Medallion)
# MAGIC
# MAGIC Le pattern **Medallion** organise les données en 3 couches de qualité croissante :
# MAGIC
# MAGIC | Couche | Rôle | Qualité |
# MAGIC |--------|------|---------|
# MAGIC | **Bronze** | Données brutes, ingestion fidèle | ★☆☆ |
# MAGIC | **Silver** | Données nettoyées, enrichies | ★★☆ |
# MAGIC | **Gold**   | KPIs métier, prêts pour la BI | ★★★ |
# MAGIC
# MAGIC On ajoute aussi un schema `landing` pour accueillir les fichiers CSV bruts.

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG energy_catalog;
# MAGIC
# MAGIC
# MAGIC -- schema landing_energy
# MAGIC CREATE SCHEMA IF NOT EXISTS landing_energy
# MAGIC COMMENT "Zone d'atterrissage des fichiers sources bruts (CSV, JSON...)";
# MAGIC
# MAGIC
# MAGIC -- schema bronze_energy
# MAGIC CREATE SCHEMA IF NOT EXISTS bronze_energy
# MAGIC COMMENT "Couche Bronze : ingestion fidèle des données brutes en Delta";
# MAGIC
# MAGIC
# MAGIC -- schema silver_energy
# MAGIC CREATE SCHEMA IF NOT EXISTS silver_energy
# MAGIC COMMENT "Couche Silver : données nettoyées, dédupliquées, typées";
# MAGIC
# MAGIC
# MAGIC -- schema gold_energy
# MAGIC CREATE SCHEMA IF NOT EXISTS gold_energy
# MAGIC COMMENT "Couche Gold : KPIs agrégés, prêts pour la BI et le ML";

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Applique des tags sur chaque schema
# MAGIC ALTER SCHEMA energy_catalog.bronze_energy SET TAGS ('layer' = 'bronze', 'data_quality' = 'raw');
# MAGIC ALTER SCHEMA energy_catalog.silver_energy SET TAGS ('layer' = 'silver', 'data_quality' = 'cleaned');
# MAGIC ALTER SCHEMA energy_catalog.gold_energy   SET TAGS ('layer' = 'gold',   'data_quality' = 'curated');

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Vérifie que tout est en place
# MAGIC SHOW SCHEMAS IN energy_catalog;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.4 — Création d'un Volume pour les fichiers sources
# MAGIC
# MAGIC Un **Volume Unity Catalog** est un stockage de fichiers géré (comme un dossier S3/ADLS)
# MAGIC mais avec gouvernance, lineage et accès unifié.
# MAGIC
# MAGIC > 💡 Différence Volume vs Table Delta :
# MAGIC > - **Table Delta** = données structurées queryables en SQL
# MAGIC > - **Volume** = fichiers bruts (CSV, JSON, images, modèles ML...)

# COMMAND ----------

# MAGIC %sql
# MAGIC --create volume Catalog.monschema.rawfiles @abfss:/...
# MAGIC
# MAGIC CREATE VOLUME IF NOT EXISTS energy_catalog.landing_energy.raw_files
# MAGIC COMMENT "Fichiers CSV sources : solar_production, wind_production, sites";

# COMMAND ----------

# MAGIC %md
# MAGIC ### 📁 Upload des fichiers CSV
# MAGIC
# MAGIC Deux options pour uploader tes fichiers dans le Volume :
# MAGIC
# MAGIC **Option A — UI Databricks :**
# MAGIC 1. Dans le menu gauche, va sur **Catalog**
# MAGIC 2. Navigue jusqu'à `energy_catalog > landing_energy > raw_files`
# MAGIC 3. Clique sur **"Upload to this volume"**
# MAGIC 4. Dépose tes 3 fichiers CSV
# MAGIC
# MAGIC **Option B — CLI Databricks :**
# MAGIC ```bash
# MAGIC databricks fs cp solar_production.csv  dbfs:/Volumes/energy_catalog/landing_energy/raw_files/
# MAGIC databricks fs cp wind_production.csv   dbfs:/Volumes/energy_catalog/landing_energy/raw_files/
# MAGIC databricks fs cp sites.csv             dbfs:/Volumes/energy_catalog/landing_energy/raw_files/
# MAGIC ```

# COMMAND ----------

# Vérifie que les fichiers sont bien uploadés
volume_path = "/Volumes/energy_catalog/landing_energy/raw_files"

import os

try:
    files=dbutils.fs.ls(volume_path)
    print("✅ Fichiers trouvés dans le Volume :")
    for f in files:
        print(f" {f.name:40s}  {f.size/1024:.1f} KB")
except Exception as e:
    print(f"⚠️  Volume vide ou non accessible : {e}")
    print("   → Uploade les 3 fichiers CSV avant de continuer")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.5 — Configuration des variables globales
# MAGIC
# MAGIC On centralise les chemins dans des widgets Databricks pour éviter
# MAGIC les chemins codés en dur dans chaque notebook.

# COMMAND ----------


# Définit des widgets réutilisables dans le notebook
dbutils.widgets.text("catalog",        "energy_catalog",    "Catalog Unity Catalog")
dbutils.widgets.text("volume_path",    "/Volumes/energy_catalog/landing_energy/raw_files", "Chemin Volume")
dbutils.widgets.text("checkpoint_path", "/tmp/solarwind/checkpoints", "Checkpoint AutoLoader")

# Récupère les valeurs
CATALOG       = dbutils.widgets.get("catalog")
VOLUME_PATH   = dbutils.widgets.get("volume_path")
CHECKPOINT    = dbutils.widgets.get("checkpoint_path")

print(f"CATALOG       = {CATALOG}")
print(f"VOLUME_PATH   = {VOLUME_PATH}")
print(f"CHECKPOINT    = {CHECKPOINT}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Récapitulatif Phase 1.1
# MAGIC
# MAGIC Tu as créé :
# MAGIC - **1 catalog** : `energy_catalog`
# MAGIC - **4 schemas** : `landing_energy`, `bronze_energy`, `silver_energy`, `gold_energy`
# MAGIC - **1 volume** : `energy_catalog.landing_energy.raw_files`
# MAGIC - **Tags** de gouvernance sur chaque objet
# MAGIC  
# MAGIC  
