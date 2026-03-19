import asyncio
import json
import logging
import os
from datetime import datetime

import websockets
from dotenv import load_dotenv
from kafka import KafkaProducer

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_RAW", "crypto.raw.trades")

# Paires à suivre
SYMBOLS = [
    "btcusdt", "ethusdt", "bnbusdt",
    "solusdt", "xrpusdt", "dogeusdt",
]

BINANCE_WS_URL = (
    "wss://stream.binance.com:9443/stream?streams="
    + "/".join(f"{s}@trade" for s in SYMBOLS)
)


# ── Kafka Producer ────────────────────────────────────────────────
def create_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        acks="all",
        retries=5,
    )


# ── Message parser ────────────────────────────────────────────────
def parse_trade(raw: dict) -> dict:
    """
    Binance trade stream payload :
    {
      "e": "trade", "E": 123456789,  # event time
      "s": "BTCUSDT",                # symbol
      "p": "0.001",                  # price
      "q": "100",                    # quantity
      "T": 123456785,                # trade time
      "m": true,                     # buyer is market maker
    }
    """
    data = raw["data"]
    return {
        "symbol":     data["s"],
        "price":      float(data["p"]),
        "quantity":   float(data["q"]),
        "trade_time": data["T"],
        "event_time": data["E"],
        "is_buyer_market_maker": data["m"],
        "ingested_at": datetime.utcnow().isoformat(),
    }


# ── Main loop ─────────────────────────────────────────────────────
async def consume(producer: KafkaProducer):
    logger.info(f"Connexion à Binance WebSocket — {len(SYMBOLS)} symboles")
    logger.info(f"Topic Kafka cible : {KAFKA_TOPIC}")

    async for websocket in websockets.connect(BINANCE_WS_URL, ping_interval=20):
        try:
            async for message in websocket:
                raw = json.loads(message)
                trade = parse_trade(raw)

                producer.send(
                    topic=KAFKA_TOPIC,
                    key=trade["symbol"],
                    value=trade,
                )

                logger.info(
                    f"{trade['symbol']:10s} | "
                    f"price={trade['price']:>12.4f} | "
                    f"qty={trade['quantity']:>10.4f}"
                )

        except websockets.ConnectionClosed:
            logger.warning("WebSocket déconnecté, reconnexion...")
            continue
        except Exception as e:
            logger.error(f"Erreur inattendue : {e}")
            await asyncio.sleep(2)
            continue


def main():
    producer = create_producer()
    logger.info("KafkaProducer initialisé")
    try:
        asyncio.run(consume(producer))
    except KeyboardInterrupt:
        logger.info("Arrêt du producer")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()