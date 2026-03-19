import os
import asyncio
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from google.cloud import bigquery
from dotenv import load_dotenv

load_dotenv()

GCP_PROJECT    = os.getenv("GCP_PROJECT",    "crypto-platform-dev-490610")
BQ_DATASET_MART = os.getenv("BQ_DATASET_MART", "crypto_mart")

app = FastAPI(title="Crypto Analytics API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

client = bigquery.Client(project=GCP_PROJECT)


def run_query(sql: str) -> list:
    return [dict(row) for row in client.query(sql).result()]


# ── Routes ────────────────────────────────────────────────────────

@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/top-movers")
def top_movers(limit: int = 10):
    """Retourne les cryptos avec les plus fortes variations."""
    sql = f"""
        SELECT
            symbol,
            ROUND(vwap, 4)             AS vwap,
            ROUND(volume, 4)           AS volume,
            trade_count,
            ROUND(price_change_pct, 4) AS price_change_pct,
            ROUND(price_min, 4)        AS price_min,
            ROUND(price_max, 4)        AS price_max,
            direction,
            ROUND(abs_change_pct, 4)   AS abs_change_pct,
            CAST(window_start AS STRING) AS window_start,
            CAST(window_end   AS STRING) AS window_end
        FROM `{GCP_PROJECT}.{BQ_DATASET_MART}.mart_top_movers`
        ORDER BY abs_change_pct DESC
        LIMIT {limit}
    """
    return run_query(sql)


@app.get("/ohlcv/{symbol}")
def ohlcv(symbol: str, limit: int = 24):
    """Retourne les bougies OHLCV 1h pour un symbole."""
    sql = f"""
        SELECT
            symbol,
            CAST(hour AS STRING)       AS hour,
            trade_count,
            ROUND(volume, 6)           AS volume,
            ROUND(turnover, 6)         AS turnover,
            ROUND(low, 6)              AS low,
            ROUND(high, 6)             AS high,
            ROUND(vwap, 6)             AS vwap
        FROM `{GCP_PROJECT}.{BQ_DATASET_MART}.mart_ohlcv_1h`
        WHERE symbol = '{symbol.upper()}'
        ORDER BY hour DESC
        LIMIT {limit}
    """
    return run_query(sql)


@app.get("/heatmap")
def heatmap(symbol: str = None):
    """Retourne les données de heatmap volume par heure/jour."""
    where = f"WHERE symbol = '{symbol.upper()}'" if symbol else ""
    sql = f"""
        SELECT
            symbol,
            hour_of_day,
            day_of_week,
            trade_count,
            ROUND(volume, 4)    AS volume,
            ROUND(avg_price, 4) AS avg_price
        FROM `{GCP_PROJECT}.{BQ_DATASET_MART}.mart_volume_heatmap`
        {where}
        ORDER BY symbol, day_of_week, hour_of_day
    """
    return run_query(sql)


@app.get("/alerts")
def alerts(threshold: float = 0.5):
    """Retourne les symboles avec variation > threshold%."""
    sql = f"""
        SELECT
            symbol,
            ROUND(price_change_pct, 4) AS price_change_pct,
            ROUND(vwap, 4)             AS vwap,
            direction,
            CAST(window_end AS STRING) AS window_end
        FROM `{GCP_PROJECT}.{BQ_DATASET_MART}.mart_top_movers`
        WHERE ABS(price_change_pct) > {threshold}
        ORDER BY ABS(price_change_pct) DESC
    """
    return run_query(sql)


@app.get("/symbols")
def symbols():
    """Retourne la liste des symboles disponibles."""
    sql = f"""
        SELECT DISTINCT symbol
        FROM `{GCP_PROJECT}.{BQ_DATASET_MART}.mart_top_movers`
        ORDER BY symbol
    """
    return [row["symbol"] for row in run_query(sql)]

# ── WebSocket Manager ─────────────────────────────────────────────
class ConnectionManager:
    def __init__(self):
        self.active: list[WebSocket] = []

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.active.append(ws)

    def disconnect(self, ws: WebSocket):
        self.active.remove(ws)

    async def broadcast(self, data: dict):
        for ws in self.active.copy():
            try:
                await ws.send_json(data)
            except Exception:
                self.active.remove(ws)

manager = ConnectionManager()


@app.websocket("/ws/metrics")
async def ws_metrics(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            # Push toutes les 15 secondes
            data = {
                "top_movers": run_query(f"""
                    SELECT symbol, ROUND(vwap,4) AS vwap,
                           ROUND(price_change_pct,4) AS price_change_pct,
                           direction, trade_count,
                           ROUND(abs_change_pct,4) AS abs_change_pct,
                           CAST(window_end AS STRING) AS window_end
                    FROM `{GCP_PROJECT}.{BQ_DATASET_MART}.mart_top_movers`
                    ORDER BY abs_change_pct DESC
                """),
                "alerts": run_query(f"""
                    SELECT symbol, ROUND(price_change_pct,4) AS price_change_pct,
                           ROUND(vwap,4) AS vwap, direction,
                           CAST(window_end AS STRING) AS window_end
                    FROM `{GCP_PROJECT}.{BQ_DATASET_MART}.mart_top_movers`
                    WHERE ABS(price_change_pct) > 0.5
                    ORDER BY ABS(price_change_pct) DESC
                """),
            }
            await websocket.send_json(data)
            await asyncio.sleep(15)
    except WebSocketDisconnect:
        manager.disconnect(websocket)