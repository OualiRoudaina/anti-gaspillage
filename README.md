# Plateforme anti-gaspillage (Odoo + Big Data)

Détection en temps réel des lots à risque de péremption :
Odoo -> Kafka -> Spark (streaming + batch) -> Redis / MongoDB -> Odoo.

## Prérequis
- Docker et Docker Compose
- 16 Go de RAM recommandés

## Démarrage
```bash
cp .env.example .env     # puis modifier les mots de passe
make up                  # lance tous les services
make ps                  # vérifier l'état
make topics              # doit afficher : lots, ventes
```

## Accès
| Service | URL / port |
|---|---|
| Odoo | http://localhost:8069 |
| Spark master | http://localhost:8080 |
| Kafka (hôte) | localhost:29092 |
| Redis | localhost:6379 |
| MongoDB | localhost:27017 |

## Arborescence
- `odoo/addons/` : module `lot_waste_risk`
- `generator/` : générateur de ventes
- `spark/jobs/` : jobs streaming et batch
- `scripts/` : export des lots, synchronisation vers Odoo
- `data/parquet/` : archive des ventes (ignorée par Git)

## Workflow Git
Une branche par tâche (`feature/...`), fusionnée dans `main`.

## Traitements Big Data
Commandes à lancer depuis la racine du projet (PowerShell). Le connecteur Kafka de Spark est téléchargé au premier lancement ; l'option `spark.jars.ivy` est nécessaire car l'utilisateur `spark` n'a pas de dossier personnel.

```powershell
# 1. Lots Odoo -> Kafka
py scripts/export_lots.py

# 2. Streaming : ventes -> stock et risque dans Redis (reste actif)
docker compose exec -d spark-master sh -c '/opt/spark/bin/spark-submit --master local[2] --conf spark.jars.ivy=/tmp/.ivy2 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 /opt/jobs/ventes_streaming.py > /tmp/streaming.log 2>&1'

# 3. Ventes simulées
py generator/generateur_ventes.py --count 500

# 4. Batch : vitesses de vente (par lot, et par produit et magasin) + archives Parquet
docker compose exec spark-master sh -c '/opt/spark/bin/spark-submit --master local[2] --conf spark.jars.ivy=/tmp/.ivy2 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 /opt/jobs/vitesses_vente.py'

# 5. Historique du risque dans MongoDB (pip install redis pymongo)
py scripts/historique_mongo.py
py scripts/historique_mongo.py --stats

# 6. Risque -> Odoo
py scripts/sync_risque_odoo.py
```

Tests des fonctions de calcul (sans Docker) : `python -m unittest discover -s tests -v`.
