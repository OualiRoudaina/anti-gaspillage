
import os
import argparse
from datetime import datetime, timezone

import redis

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    to_date,
    sum as spark_sum,
    countDistinct,
    first,
    current_timestamp
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType
)


def main():
    parser = argparse.ArgumentParser(
        description="Calcul des vitesses de vente par lot"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Teste le calcul sans modifier Redis"
    )
    args = parser.parse_args()

    # Spark en mode local : aucune dépendance au worker
    spark = (
        SparkSession.builder
        .appName("CalculVitessesVente")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    # Structure des messages Kafka
    schema = StructType([
        StructField("vente_id", StringType(), True),
        StructField("lot_id", StringType(), True),
        StructField("produit_ref", StringType(), True),
        StructField("magasin", StringType(), True),
        StructField("quantite", IntegerType(), True),
        StructField("horodatage", StringType(), True)
    ])

    try:
        print("Lecture de l'historique Kafka...")

        kafka_df = (
            spark.read
            .format("kafka")
            .option("kafka.bootstrap.servers", "kafka:9092")
            .option("subscribe", "ventes")
            .option("startingOffsets", "earliest")
            .option("endingOffsets", "latest")
            .option("failOnDataLoss", "false")
            .load()
        )

        # Extraction et nettoyage des ventes
        ventes = (
            kafka_df
            .select(
                from_json(
                    col("value").cast("string"),
                    schema
                ).alias("v")
            )
            .select("v.*")
            .filter(
                col("vente_id").isNotNull()
                & col("lot_id").isNotNull()
                & col("quantite").isNotNull()
                & col("horodatage").isNotNull()
            )
            .withColumn(
                "date_vente",
                to_date(to_timestamp(col("horodatage")))
            )
            .filter(col("date_vente").isNotNull())
            # Élimination des doublons
            .dropDuplicates(["vente_id"])
        )

        print("Agrégation des ventes par lot...")

        vitesses = (
            ventes
            .groupBy("lot_id")
            .agg(
                spark_sum("quantite").alias("quantite_vendue"),
                countDistinct("date_vente").alias("jours_actifs"),
                first(
                    "produit_ref",
                    ignorenulls=True
                ).alias("produit_ref"),
                first(
                    "magasin",
                    ignorenulls=True
                ).alias("magasin")
            )
            .withColumn(
                "vitesse_moyenne_jour",
                col("quantite_vendue") / col("jours_actifs")
            )
            .withColumn(
                "date_calcul",
                current_timestamp()
            )
        )

        nombre_lots = vitesses.count()

        print(f"{nombre_lots} lots avec des ventes analysés.")

        # Archive Parquet
        chemin_parquet = "/data/parquet/vitesses_vente"

        (
            vitesses
            .write
            .mode("overwrite")
            .parquet(chemin_parquet)
        )

        print(f"Archive Parquet enregistrée : {chemin_parquet}")

        # Récupération des résultats
        resultats = vitesses.collect()

        if args.dry_run:
            print("Mode dry-run : Redis ne sera pas modifié.")

        else:
            client = redis.Redis(
                host=os.getenv("REDIS_HOST", "redis"),
                port=int(os.getenv("REDIS_PORT", "6379")),
                password=os.getenv("REDIS_PASSWORD"),
                decode_responses=True
            )

            client.ping()

            for ligne in resultats:
                lot_id = ligne["lot_id"]
                cle = f"vitesse:{lot_id}"

                client.hset(
                    cle,
                    mapping={
                        "vitesse_moyenne_jour": str(
                            round(
                                float(ligne["vitesse_moyenne_jour"]),
                                2
                            )
                        ),
                        "quantite_vendue": str(
                            ligne["quantite_vendue"]
                        ),
                        "jours_actifs": str(
                            ligne["jours_actifs"]
                        ),
                        "produit_ref": ligne["produit_ref"] or "",
                        "magasin": ligne["magasin"] or "",
                        "date_calcul": datetime.now(
                            timezone.utc
                        ).isoformat()
                    }
                )

            print(
                f"{len(resultats)} vitesses enregistrées dans Redis."
            )

        print("\nAperçu des 10 premiers résultats :")

        for ligne in resultats[:10]:
            print(
                f"{ligne['lot_id']} | "
                f"Ventes : {ligne['quantite_vendue']} | "
                f"Jours actifs : {ligne['jours_actifs']} | "
                f"Vitesse/jour : "
                f"{ligne['vitesse_moyenne_jour']:.2f}"
            )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
