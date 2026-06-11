# Kafka — Environnement local & Producer eCommerce

Infrastructure locale complète pour l'apprentissage d'Apache Kafka,
avec un producer Python qui stream les événements eCommerce (dataset 2019).

> ⚠️ **Prérequis ressources** : Docker doit avoir au minimum **6 Go de RAM**.
> Le dataset complet fait ~9 Go — prévoir **20 Go d'espace disque libre**.

## Stack

| Service        | Image                          | Port hôte |
|----------------|--------------------------------|-----------|
| Zookeeper      | confluentinc/cp-zookeeper:7.6.1| —         |
| Kafka Broker   | confluentinc/cp-kafka:7.6.1    | **9092**  |
| Kafdrop (UI)   | obsidiandynamics/kafdrop:4.0.2 | **9000**  |
| PostgreSQL 16  | postgres:16-alpine             | **5432**  |
| Elasticsearch  | elasticsearch:8.13.4           | **9200**  |
| Kibana *(opt)* | kibana:8.13.4                  | **5601**  |

---

## Démarrage rapide

```bash
# 1. Cloner / se placer dans le dossier
cd /home/fabgrall/wsl-projects/Kafka

# 2. Démarrer la stack de base
docker compose up -d

# 3. (Optionnel) Démarrer avec Kibana
docker compose --profile kibana up -d

# 4. Vérifier l'état des conteneurs
docker compose ps
```

## Accès aux interfaces

| Interface          | URL                          | Identifiants          |
|--------------------|------------------------------|-----------------------|
| **Kafdrop** (UI)   | http://localhost:9000        | —                     |
| **Elasticsearch**  | http://localhost:9200        | —                     |
| **Kibana**         | http://localhost:5601        | —                     |
| **PostgreSQL**     | `localhost:5432`             | kafka / kafka_secret  |

## Commandes utiles

```bash
# Arrêter la stack (données conservées)
docker compose down

# Arrêter ET supprimer les volumes (reset complet)
docker compose down -v

# Voir les logs d'un service
docker compose logs -f kafka
docker compose logs -f elasticsearch

# Accéder au shell Kafka
docker exec -it kafka bash

# Créer un topic manuellement
docker exec kafka kafka-topics --bootstrap-server localhost:9092 \
  --create --topic mon-topic --partitions 3 --replication-factor 1

# Lister les topics
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list

# Produire un message
docker exec -it kafka kafka-console-producer \
  --bootstrap-server localhost:9092 --topic mon-topic

# Consommer des messages
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 --topic mon-topic --from-beginning
```

## Configuration

Toutes les variables sont dans `.env` :

```
KAFKA_EXTERNAL_PORT=9092
KAFDROP_PORT=9000
POSTGRES_DB=kafkadb
POSTGRES_USER=kafka
POSTGRES_PASSWORD=kafka_secret
ES_PORT=9200
```

## Initialisation PostgreSQL

Placez vos scripts `.sql` dans le dossier `init-sql/`.  
Ils sont exécutés automatiquement au premier démarrage du conteneur PostgreSQL.

## Architecture réseau

Tous les services partagent le réseau Docker `kafka-net`.  
À l'intérieur du réseau, Kafka est accessible via `kafka:29092`.  
Depuis l'hôte, Kafka est accessible via `localhost:9092`.

---

## Module 1.3 — Producer Python

### Prérequis Python

```bash
# Créer le venv et installer les dépendances
uv venv
uv add pandas confluent-kafka python-dotenv tqdm
```

### Dataset

| Fichier | Lignes | Taille |
|---|---|---|
| `archive/2019-Oct.csv` | ~18 M | ~4 Go |
| `archive/2019-Nov.csv` | ~67 M | ~9 Go |

Colonnes : `event_time`, `event_type`, `product_id`, `category_id`, `category_code`, `brand`, `price`, `user_id`, `user_session`

### Lancer le producer

```bash
# Octobre complet (~18 M messages)
.venv/bin/python producer.py

# Novembre complet (~67 M messages)
.venv/bin/python producer.py --source archive/2019-Nov.csv

# Limiter à N messages (test)
.venv/bin/python producer.py --max-rows 100000

# Toutes les options
.venv/bin/python producer.py --help
```

### Options disponibles

| Option | Défaut | Description |
|---|---|---|
| `--source` | `archive/2019-Oct.csv` | Fichier CSV source |
| `--topic` | `ecommerce-events` | Topic Kafka cible |
| `--brokers` | `localhost:9092` | Bootstrap servers |
| `--chunk-size` | `2000` | Taille des chunks pandas |
| `--max-rows` | illimité | Limite le nombre de messages |

### Configuration via `.env`

```env
KAFKA_TOPIC=ecommerce-events
KAFKA_BROKERS=localhost:9092
CHUNK_SIZE=2000
```

### Architecture du producer

```
CSV (chunks pandas)
      │
      ▼
  normalize()          ← types, NaN → None
      │
      ▼
  json.dumps()         ← sérialisation JSON UTF-8
      │
      ├─ key   = user_id  ← partitionnement (ordre garanti par user)
      └─ value = JSON bytes
            │
            ▼
        Kafka topic
    (3 partitions, lz4)
            │
      delivery_callback()  ← ACK / erreur par message
```

### Vérifier les messages dans Kafdrop

Ouvrir [http://localhost:9000](http://localhost:9000) → topic `ecommerce-events`.
