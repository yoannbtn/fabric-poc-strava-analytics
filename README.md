# 🏃‍♂️ Strava Data Analytics Pipeline on Microsoft Fabric

![Microsoft Fabric](https://img.shields.io/badge/Microsoft%20Fabric-Data%20Engineering-0078D4?style=flat-square&logo=microsoft)
![PySpark](https://img.shields.io/badge/PySpark-3.4-E25A1C?style=flat-square&logo=apachespark)
![Power BI](https://img.shields.io/badge/Power%20BI-Direct%20Lake-F2C811?style=flat-square&logo=powerbi)
![FinOps](https://img.shields.io/badge/FinOps-Azure%20Automation-0089D6?style=flat-square&logo=microsoftazure)

## 📋 Présentation du Projet

Ce POC (Proof of Concept) met en place une plateforme de données moderne de bout en bout pour analyser la préparation sportive (Trail/Vélotaf) et l'usure du matériel. 
L'objectif est d'automatiser l'extraction des données de l'API Strava vers **Microsoft Fabric** en suivant une architecture **Medallion**, tout en intégrant une couche d'optimisation **FinOps** poussée pour opérer de manière rentable sur une capacité minimale (F2).

## 🏗️ Architecture des Données (Medallion)

Le pipeline garantit la qualité, la traçabilité et l'évolutivité des données via trois couches distinctes stockées sur **OneLake** (format Delta/Parquet) :

* **🥉 Couche Bronze (Raw)** : 
  * Ingestion automatisée depuis l'API Strava (REST/OAuth2).
  * Stockage au format JSON brut (`Files/Bronze/Strava/`).
  * Séparation logique : `Activities/` (flux temporel) et `Gears/` (snapshots quotidiens).

* **🥈 Couche Silver (Staging)** : 
  * Traitement via **PySpark** : Nettoyage, typage explicite et dédoublonnage.
  * **Historisation SCD Type 2** : Suivi précis de l'usure du matériel (détection des changements de distance).

* **🥇 Couche Gold (Curated - Star Schema)** : 
  * Modélisation en schéma en étoile optimisée pour le décisionnel.
  * **Faits** : `fct_activities` (métriques transformées comme la VAM) et `fct_gears` (journal d'usure).
  * **Dimensions** : `dim_gears` et `dim_calendar` (avec clés de tri numériques et patterns ISO).

## ⚖️ Expertise FinOps & Performance

Pour viabiliser ce POC sur une petite capacité **Fabric F2**, une stratégie stricte de gestion des coûts et des ressources a été implémentée :

* **Orchestration de la Capacité (Start/Pause)** : Utilisation d'**Azure PowerShell Runbooks** pour allumer et éteindre automatiquement la capacité Fabric, alignant la facturation sur le temps de traitement réel et les fenêtres de consultation.
* **Monitoring Avancé** : Suivi via *Fabric Capacity Metrics* pour gérer les pics de charge et suivre les mécanismes sous-jacents aux capacités (Bursting/Smoothing/Throttling).
* **Optimisation Spark** : 
  * Utilisation de **Starter Pools (Small Nodes)**.
  * Sessions Spark en **High Concurrency** pour éliminer le "Cold Start" entre les notebooks.
  * *Session Timeout* agressif réduit à 5 minutes pour libérer les Capacity Units (CU) immédiatement.

## 💎 Restitution & BI Conversationnelle

* **Direct Lake Mode** : Le modèle sémantique Power BI est connecté en Direct Lake, offrant des performances natives sans latence de rafraîchissement.
* **Data Agent** : Intégration d'une IA conversationnelle avec des *System Instructions* personnalisées (logique de jointure, synonymes métiers) permettant d'explorer les performances en langage naturel.

## 📂 Structure du Repository (Notebooks)

| Ordre | Nom du fichier | Couche | Description |
| :---: | :--- | :---: | :--- |
| **01** | `nb_strava_01_bronze` | Bronze | Gestion OAuth2 (Azure Key Vault), extraction activités et snapshots JSON. |
| **02** | `nb_strava_02_silver_activities` | Silver | Nettoyage, parsing et typage du flux d'activités sportives. |
| **03** | `nb_strava_02_silver_gears` | Silver | Unification des snapshots et historisation SCD Type 2. |
| **04** | `nb_strava_03_gold_activities` | Gold | Calculs métiers (VAM), catégorisation métier et préparation BI. |
| **05** | `nb_strava_04_gold_dim_calendar` | Gold | Génération dynamique du calendrier technique. |
| **06** | `nb_runbook_control` | DevOps | Script Azure Automation (PowerShell) de gestion de la capacité. |

## 📈 Business Logic (Exemples)

**Calcul de la Vitesse Ascensionnelle Moyenne (VAM)** :
```math
VAM = \frac{\text{Elevation Gain (m)}}{\text{Moving Time (h)}}
```

**Catégorisation des sports (Spark SQL)** :
```python
df_with_categories = df_silver.withColumn(
    "SportCategory",
    F.when(F.col("SportType").isin("Run", "TrailRun"), "Running")
     .when(F.col("SportType").isin("Ride", "EBikeRide", "VirtualRide"), "Cycling")
     .otherwise("Others")
)
```

## 🚀 Roadmap & Évolutions Futures

Ce POC est conçu pour évoluer et intégrer les dernières capacités d'Intelligence Artificielle et de gouvernance de l'écosystème Microsoft Fabric :

* **Gouvernance Sémantique (Fabric Ontology)** : Superposition d'une Ontologie au modèle sémantique existant. L'objectif est de modéliser les relations métiers complexes (ex: corrélation entre la VAM, le type de terrain et l'usure prématurée du matériel) pour fournir un contexte beaucoup plus riche à l'IA.
* **Orchestration IA (Copilot Studio + Data Agent)** : Encapsulation du Data Agent actuel au sein d'un Custom Copilot. Cette évolution permettra de passer d'une BI purement *consultative* à une approche *actionnable* (ex: déclenchement d'alertes automatisées sur Teams ou création de routines de maintenance pour le matériel basé sur l'analyse de données).

---
*Projet conçu et développé par **Yoann BETTON** - 2026*
