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
