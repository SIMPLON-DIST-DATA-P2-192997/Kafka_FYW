"""
producer.py
───────────
Producer Kafka — Module 1.3

Lit le CSV eCommerce par chunks avec pandas et envoie
chaque événement sérialisé en JSON vers le topic Kafka configuré.

• Lecture CSV par chunks (mémoire constante)
• Sérialisation JSON des messages
• Partitionnement par user_id  → ordre garanti par utilisateur
• Callbacks succès / erreur
• Rapport de progression en temps réel

Usage :
    python producer.py                              # 2019-Oct.csv
    python producer.py --source archive/2019-Nov.csv
    python producer.py --topic ecommerce-events
    python producer.py --chunk-size 5000 --max-rows 100000
"""

import argparse
import json
import logging
import os
import signal
import sys
import time
from pathlib import Path
from typing import Any

import pandas as pd
from confluent_kafka import Producer, KafkaException
from dotenv import load_dotenv
from tqdm import tqdm

# ── Config ──────────────────────────────────────────────────────────────────

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent

DEFAULT_SOURCE     = BASE_DIR / "archive" / "2019-Oct.csv"
DEFAULT_TOPIC      = os.getenv("KAFKA_TOPIC", "ecommerce-events")
DEFAULT_BROKERS    = os.getenv("KAFKA_BROKERS", "localhost:9092")
DEFAULT_CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", "2000"))

logging.basicConfig(
    format="%(asctime)s  %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
    level=logging.INFO,
)
log = logging.getLogger(__name__)


# ── Compteurs globaux ────────────────────────────────────────────────────────

class Stats:
    sent = 0
    acked = 0
    errors = 0
    start: float = 0.0


# ── Callbacks ────────────────────────────────────────────────────────────────

def delivery_callback(err, msg):
    """Appelé par confluent-kafka pour chaque message acquitté (ou en erreur)."""
    if err:
        Stats.errors += 1
        log.error("❌  Erreur livraison | topic=%s partition=%s | %s",
                  msg.topic(), msg.partition(), err)
    else:
        Stats.acked += 1


# ── Sérialisation ────────────────────────────────────────────────────────────

def row_to_bytes(row: dict[str, Any]) -> bytes:
    """Sérialise un enregistrement en JSON UTF-8."""
    return json.dumps(row, ensure_ascii=False, default=str).encode("utf-8")


def partition_key(row: dict[str, Any]) -> bytes | None:
    """
    Clé de partitionnement = user_id (str).
    Kafka hash la clé pour choisir la partition → ordre garanti par user.
    """
    uid = row.get("user_id")
    return str(uid).encode() if uid else None


# ── Lecture source ────────────────────────────────────────────────────────────

def iter_chunks(source: Path, chunk_size: int, max_rows: int | None):
    """
    Génère des DataFrames en chunks depuis un CSV.
    Respecte max_rows si fourni.
    """
    if source.suffix.lower() != ".csv":
        raise ValueError(f"Fichier CSV attendu, reçu : {source.suffix}")

    rows_yielded = 0
    reader = pd.read_csv(source, chunksize=chunk_size, dtype=str,
                         on_bad_lines="skip")

    for chunk in reader:
        if max_rows and rows_yielded >= max_rows:
            break
        if max_rows:
            remaining = max_rows - rows_yielded
            chunk = chunk.iloc[:remaining]
        rows_yielded += len(chunk)
        yield chunk


# ── Producer ─────────────────────────────────────────────────────────────────

def build_producer(brokers: str) -> Producer:
    conf = {
        "bootstrap.servers": brokers,
        # Attendre l'ack du leader (équilibre perf/fiabilité)
        "acks": "1",
        # Compression pour réduire la bande passante
        "compression.type": "lz4",
        # Micro-batching : grouper les messages sur 5 ms
        "linger.ms": 5,
        # Taille max d'un batch
        "batch.size": 131072,          # 128 KB
        # Buffer mémoire total
        "queue.buffering.max.messages": 100_000,
        "queue.buffering.max.kbytes":   65536,   # 64 MB
        # Retry en cas d'erreur réseau transitoire
        "retries": 5,
        "retry.backoff.ms": 300,
    }
    return Producer(conf)


def produce(producer: Producer, topic: str, source: Path,
            chunk_size: int, max_rows: int | None) -> None:

    # Compte total de lignes pour la barre de progression
    total = max_rows
    if total is None:
        log.info("Comptage des lignes source…")
        total = sum(1 for _ in open(source, "rb")) - 1  # -1 pour l'en-tête CSV

    Stats.start = time.monotonic()
    pbar = tqdm(total=total, unit="msg", desc="Producing", dynamic_ncols=True)

    for chunk in iter_chunks(source, chunk_size, max_rows):
        # Convertir le chunk en liste de dicts (NaN → None)
        records: list[dict[str, Any]] = (
            chunk.astype(object).where(pd.notna(chunk), other=None)  # type: ignore[arg-type]
            .to_dict(orient="records")  # type: ignore[assignment]
        )

        for row in records:
            key   = partition_key(row)
            value = row_to_bytes(row)

            # Boucle de back-pressure : si le buffer est plein, on poll
            while True:
                try:
                    producer.produce(topic, key=key, value=value,
                                     on_delivery=delivery_callback)
                    Stats.sent += 1
                    pbar.update(1)
                    break
                except BufferError:
                    producer.poll(0.1)   # libère de la place dans le buffer

        # poll() déclenche les callbacks (sans bloquer longtemps)
        producer.poll(0)

    pbar.close()

    # Vidage final : attend que tous les messages soient acquittés
    log.info("Flush en cours…")
    producer.flush(timeout=60)


# ── Graceful shutdown ─────────────────────────────────────────────────────────

_producer_ref: Producer | None = None

def _sigint_handler(sig, frame):
    log.warning("\n⚠️  Interruption reçue — flush en cours…")
    if _producer_ref:
        _producer_ref.flush(timeout=10)
    _print_report()
    sys.exit(0)


def _print_report():
    elapsed = time.monotonic() - Stats.start if Stats.start else 0
    rate    = Stats.sent / elapsed if elapsed > 0 else 0
    log.info(
        "\n📊  Rapport\n"
        "   ├─ Envoyés   : %d\n"
        "   ├─ Acquittés : %d\n"
        "   ├─ Erreurs   : %d\n"
        "   ├─ Durée     : %.1f s\n"
        "   └─ Débit     : %.0f msg/s",
        Stats.sent, Stats.acked, Stats.errors, elapsed, rate,
    )


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    global _producer_ref

    parser = argparse.ArgumentParser(description="Kafka eCommerce Producer")
    parser.add_argument("--brokers",    default=DEFAULT_BROKERS,
                        help=f"Bootstrap servers (défaut : {DEFAULT_BROKERS})")
    parser.add_argument("--topic",      default=DEFAULT_TOPIC,
                        help=f"Topic cible (défaut : {DEFAULT_TOPIC})")
    parser.add_argument("--source",     type=Path, default=DEFAULT_SOURCE,
                        help=f"Fichier source JSONL ou CSV (défaut : {DEFAULT_SOURCE})")
    parser.add_argument("--chunk-size", type=int,  default=DEFAULT_CHUNK_SIZE,
                        help=f"Taille des chunks pandas (défaut : {DEFAULT_CHUNK_SIZE})")
    parser.add_argument("--max-rows",   type=int,  default=None,
                        help="Limite le nombre de messages envoyés")
    args = parser.parse_args()

    if not args.source.exists():
        log.error("Source introuvable : %s", args.source)
        sys.exit(1)

    log.info("🚀  Démarrage du producer")
    log.info("   Brokers : %s", args.brokers)
    log.info("   Topic   : %s", args.topic)
    log.info("   Source  : %s", args.source)

    producer = build_producer(args.brokers)
    _producer_ref = producer
    signal.signal(signal.SIGINT, _sigint_handler)

    try:
        produce(producer, args.topic, args.source, args.chunk_size, args.max_rows)
    except KafkaException as e:
        log.error("Erreur Kafka : %s", e)
        sys.exit(1)
    finally:
        _print_report()


if __name__ == "__main__":
    main()
