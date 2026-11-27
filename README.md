# Déploiement 5G en Île-de-France (Paris 75 & Essonne 91)

## Authors
- lucas schwindenhammer
6 aRTHUR bROUCHON

## Prerequisites
- Onyxia service `vscode-pyspark` (4 CPU, 4 GiB RAM) with S3 credentials (`AWS_S3_ENDPOINT`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`) and `KUBERNETES_NAMESPACE` set.
- Python >= 3.13, `uv`, `duckdb` CLI, `aws` CLI, `kubectl`.

## Dataset
- **Name:** Données sur les réseaux mobiles (extrait 5G départements 75 et 91)
- **URL:** https://www.data.gouv.fr/datasets/donnees-sur-les-reseaux-mobiles
- **License:** Licence Ouverte / Open Licence 2.0 (Etalab)
- **Download date:** 2026-11-16
- **Size:** 1 562 245 bytes (1.56 Mo, 6 696 rows, 23 columns)

## Problem statement
Ce projet analyse les stratégies de déploiement 5G des quatre opérateurs nationaux entre un territoire hyper-dense (Paris, 75) et un territoire périurbain (Essonne, 91). Il évalue la part de la bande haute capacité `5G NR 3500` parmi les émetteurs actifs, le taux de concrétisation des projets approuvés et le degré de mutualisation des supports physiques.

## S3 paths
- **Bucket:** `s3://$KUBERNETES_NAMESPACE`
- **Raw data (Bronze):** `s3://$KUBERNETES_NAMESPACE/bronze/only5g-dept-75-91.csv`
- **Processed data (Silver - Iceberg & Quarantine):** `s3://$KUBERNETES_NAMESPACE/warehouse/silver/` and `s3://$KUBERNETES_NAMESPACE/silver/quarantine/`
- **Analytical data (Gold):** `s3://$KUBERNETES_NAMESPACE/gold/`

## Procedure for reproduction
```bash
# 1. Synchronize Python environment
uv sync

# 2. Run Bronze -> Silver PySpark + Iceberg pipeline (to be completed in step 3)
# 3. Run Silver -> Gold dbt + DuckDB models and tests (to be completed in step 4)
# 4. Run independent verification script (to be completed in step 5)
