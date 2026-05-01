# Pipeline NYC Taxi - Architecture Medallion

Projet d'ingénierie de données réalisé sur **Databricks Community Edition**. 
Ce pipeline traite les données des taxis new-yorkais pour analyser les prix moyens par quartier.

## Architecture du Projet
Le projet suit la structure **Medallion** (Bronze, Silver, Gold) :

1.  **01_Ingestion_Bronze** : Lecture des fichiers Parquet bruts, définition du schéma et archivage.
2.  **02_Nettoyage_Silver** : Nettoyage des données (prix > 0), jointure avec le référentiel des zones et renommage des colonnes.
3.  **03_Analyse_Gold** : Agrégation finale pour calculer le prix moyen et le nombre de courses par quartier, avec exclusion des données non identifiées (N/A).

## Orchestration
Le pipeline est orchestré via les **Workflows Databricks** assurant la dépendance logique entre les tâches.

## Visualisation
Le résultat final permet d'identifier les zones les plus coûteuses (comme l'aéroport Newark - EWR).