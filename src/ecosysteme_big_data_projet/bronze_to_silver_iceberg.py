import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType


def main():
    bucket = os.environ["KUBERNETES_NAMESPACE"]
    s3_endpoint = os.environ["AWS_S3_ENDPOINT"]

    spark = (
        SparkSession.builder
        .appName("5g-bronze-to-silver-iceberg")
        .master("local[*]")
        # Configuration S3A (Hadoop 3.5.0 + jeton temporaire Onyxia)
        .config("spark.hadoop.fs.s3a.endpoint", s3_endpoint)
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true")
        .config("spark.hadoop.fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
        .config("spark.hadoop.fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])
        .config("spark.hadoop.fs.s3a.session.token", os.environ.get("AWS_SESSION_TOKEN", ""))
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider",
        )
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        # Configuration du catalogue Apache Iceberg
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.lab", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lab.type", "hadoop")
        .config("spark.sql.catalog.lab.warehouse", f"s3a://{bucket}/warehouse")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    # 1. Schéma explicite (évite le double scan de inferSchema=True)
    raw_schema = StructType([
        StructField("id", StringType(), True),
        StructField("adm_lb_nom", StringType(), True),
        StructField("sup_id", StringType(), True),
        StructField("emr_lb_systeme", StringType(), True),
        StructField("emr_dt", StringType(), True),
        StructField("sta_nm_dpt", StringType(), True),
        StructField("code_insee", StringType(), True),
        StructField("generation", StringType(), True),
        StructField("date_maj", StringType(), True),
        StructField("sta_nm_anfr", StringType(), True),
        StructField("nat_id", StringType(), True),
        StructField("sup_nm_haut", StringType(), True),
        StructField("tpo_id", StringType(), True),
        StructField("adr_lb_lieu", StringType(), True),
        StructField("adr_lb_add1", StringType(), True),
        StructField("adr_lb_add2", StringType(), True),
        StructField("adr_lb_add3", StringType(), True),
        StructField("adr_nm_cp", StringType(), True),
        StructField("com_cd_insee", StringType(), True),
        StructField("coordonnees", StringType(), True),
        StructField("coord", StringType(), True),
        StructField("statut", StringType(), True),
        StructField("list_azimut", StringType(), True),
    ])

    bronze_path = f"s3a://{bucket}/bronze/only5g-dept-75-91.csv"
    df_raw = (
        spark.read
        .option("header", "true")
        .option("sep", ";")
        .option("encoding", "iso-8859-1")
        .schema(raw_schema)
        .csv(bronze_path)
    )

    # 2. Nettoyage, typage et extraction des champs composites
    df_transformed = (
        df_raw
        .withColumn("hauteur_m", F.regexp_replace(F.col("sup_nm_haut"), ",", ".").cast("double"))
        .withColumn("latitude", F.trim(F.split(F.col("coordonnees"), ",").getItem(0)).cast("double"))
        .withColumn("longitude", F.trim(F.split(F.col("coordonnees"), ",").getItem(1)).cast("double"))
        .withColumn("emr_dt", F.to_date(F.col("emr_dt"), "dd/MM/yyyy"))
        .withColumn(
            "nb_secteurs",
            F.size(F.expr("filter(split(list_azimut, '\\\\|'), x -> trim(x) != '' AND trim(x) != 'ND')")),
        )
        .withColumn(
            "anomalie_date_projet",
            (F.trim(F.col("statut")) == F.lit("Projet approuvé")) & F.col("emr_dt").isNotNull(),
        )
    )

    # 3. Séparation données valides vs quarantaine (hauteur <= 0 m)
    df_quarantine = (
        df_transformed
        .filter(F.col("hauteur_m").isNull() | (F.col("hauteur_m") <= 0))
        .withColumn("raison_rejet", F.lit("Hauteur du support <= 0m ou nulle"))
    )
    df_valid = df_transformed.filter(F.col("hauteur_m") > 0).cache()

    raw_count = df_raw.count()
    valid_count = df_valid.count()
    quarantine_count = df_quarantine.count()

    print(f"\n[BILAN QUALITE] Lignes brutes : {raw_count} | Valides : {valid_count} | Quarantaine : {quarantine_count}")
    df_quarantine.select("id", "adm_lb_nom", "sup_id", "sta_nm_dpt", "sup_nm_haut", "raison_rejet").show(truncate=False)

    # 4. Écriture Quarantaine & Silver Parquet (pour consommation directe par dbt/DuckDB)
    df_quarantine.write.mode("overwrite").parquet(f"s3a://{bucket}/silver/quarantine/emetteurs_5g_invalid.parquet")
    df_valid.write.mode("overwrite").parquet(f"s3a://{bucket}/silver/emetteurs_5g.parquet")

    # 5. Écriture en table Apache Iceberg partitionnée par département
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lab.silver")
    spark.sql("DROP TABLE IF EXISTS lab.silver.emetteurs_5g")
    (
        df_valid.writeTo("lab.silver.emetteurs_5g")
        .partitionedBy("sta_nm_dpt")
        .create()
    )

    # Démonstration Iceberg : Evolution de schéma (ALTER TABLE) + 2e snapshot
    spark.sql("ALTER TABLE lab.silver.emetteurs_5g ADD COLUMN densite_secteurs STRING")
    spark.sql("""
        UPDATE lab.silver.emetteurs_5g
        SET densite_secteurs = CASE
            WHEN nb_secteurs >= 3 THEN 'Tri-sectoriel ou plus'
            ELSE 'Mono/Bi-sectoriel'
        END
    """)

    print("\n[ICEBERG METADATA] Snapshots de lab.silver.emetteurs_5g :")
    snapshots_df = spark.sql("""
        SELECT snapshot_id, operation, committed_at
        FROM lab.silver.emetteurs_5g.snapshots
        ORDER BY committed_at
    """)
    snapshots_df.show(truncate=False)

    first_snapshot_id = snapshots_df.collect()[0]["snapshot_id"]
    print(f"[ICEBERG TIME TRAVEL] Verification VERSION AS OF {first_snapshot_id} :")
    spark.sql(f"SELECT count(*) AS nb_lignes_snapshot_1 FROM lab.silver.emetteurs_5g VERSION AS OF {first_snapshot_id}").show()

    print("[ICEBERG METADATA] Fichiers Parquet suivis par les manifestes :")
    spark.sql("""
        SELECT file_path, record_count, file_size_in_bytes
        FROM lab.silver.emetteurs_5g.files
    """).show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
