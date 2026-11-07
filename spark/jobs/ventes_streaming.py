#!/usr/bin/env python3
"""Job Spark Structured Streaming : Kafka ventes -> stock Redis."""

import json
import os
import shutil
import sys

import redis
from risque import recalculer_risque

KAFKA = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PASSWORD = os.environ.get("REDIS_PASSWORD") or None

CHECKPOINT = "/data/checkpoints/ventes"
TTL_VENTE = 7 * 24 * 3600


LUA_VENTE = """
if redis.call('EXISTS', KEYS[2]) == 0 then return -1 end
if redis.call('SET', KEYS[1], ARGV[3], 'NX', 'EX', ARGV[2]) then
  redis.call('HINCRBY', KEYS[2], 'stock_restant', -tonumber(ARGV[1]))
  redis.call('HINCRBY', KEYS[2], 'vendu_total', tonumber(ARGV[1]))
  return 1
end
return 0
"""


def connexion():
    return redis.Redis(
        host=REDIS_HOST,
        port=6379,
        password=REDIS_PASSWORD,
        decode_responses=True
    )


def appliquer_ventes(r, valeurs):
    """Applique les ventes et recalcule les risques des lots modifiés."""
    script = r.register_script(LUA_VENTE)
    pipe = r.pipeline(transaction=False)

    envoyees = illisibles = 0
    lots_traites = []

    for brut in valeurs:
        try:
            v = json.loads(brut)
            cles = [
                f"vente:{v['vente_id']}",
                f"lot:{v['lot_id']}"
            ]
            args = [int(v["quantite"]), TTL_VENTE, v["lot_id"]]
        except (ValueError, KeyError, TypeError):
            illisibles += 1
            continue

        script(keys=cles, args=args, client=pipe)
        lots_traites.append(v["lot_id"])
        envoyees += 1

    resultats = pipe.execute() if envoyees else []

    # Recalcul uniquement si la vente a réellement modifié le stock
    lots_modifies = {
        lot_id
        for lot_id, resultat in zip(lots_traites, resultats)
        if resultat == 1
    }

    if lots_modifies:
        recalculer_risque(r, lots_modifies)

    stats = {
        "appliquees": resultats.count(1),
        "doublons": resultats.count(0),
        "lot_inconnu": resultats.count(-1),
        "illisibles": illisibles
    }

    for nom, nombre in stats.items():
        if nombre:
            r.hincrby("stats:ventes", nom, nombre)

    return stats


def initialiser_lots(spark, r):
    """Initialise les lots depuis Kafka sans écraser les stocks existants."""
    brut = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA)
        .option("subscribe", "lots")
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
        .select("value", "timestamp")
        .collect()
    )

    derniers = {}

    for row in sorted(brut, key=lambda x: x["timestamp"]):
        m = json.loads(row["value"])
        derniers[m["lot_id"]] = m

    crees = 0

    for lot_id, m in derniers.items():
        cle = f"lot:{lot_id}"

        if r.exists(cle):
            continue

        r.hset(
            cle,
            mapping={
                "produit_ref": m["produit_ref"],
                "produit": m["produit"],
                "magasin": m["magasin"],
                "date_limite": m["date_limite"] or "",
                "stock_initial": m["stock_restant"],
                "stock_restant": m["stock_restant"],
                "vendu_total": 0
            }
        )
        crees += 1

    print(
        f"Lots : {crees} créés dans Redis, "
        f"{len(derniers) - crees} existaient déjà",
        flush=True
    )

    # Recalcul après création : nouveaux et anciens lots
    lot_ids = [
        cle.split(":", 1)[1]
        for cle in r.scan_iter(match="lot:*")
    ]

    nombre = recalculer_risque(r, lot_ids)

    print(f"Risque calculé pour {nombre} lots", flush=True)


def main():
    from pyspark.sql import SparkSession

    r = connexion()
    r.ping()

    if "--reinit" in sys.argv:
        for motif in ("lot:*", "vente:*", "stats:ventes"):
            for cle in r.scan_iter(match=motif, count=500):
                r.delete(cle)

        shutil.rmtree(CHECKPOINT, ignore_errors=True)
        print("Redis et checkpoint réinitialisés", flush=True)

    spark = (
        SparkSession.builder
        .appName("ventes-streaming")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    initialiser_lots(spark, r)

    def traiter_lot(df, batch_id):
        valeurs = [
            row["value"]
            for row in df.select("value").collect()
        ]

        if valeurs:
            stats = appliquer_ventes(r, valeurs)
            print(
                f"[micro-lot {batch_id}] "
                f"{len(valeurs)} messages : {stats}",
                flush=True
            )

    (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA)
        .option("subscribe", "ventes")
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", 5000)
        .option("failOnDataLoss", "false")
        .load()
        .writeStream.foreachBatch(traiter_lot)
        .option("checkpointLocation", CHECKPOINT)
        .trigger(processingTime="2 seconds")
        .start()
        .awaitTermination()
    )


if __name__ == "__main__":
    main()