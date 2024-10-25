import asyncio
import aiohttp
from fastapi import FastAPI, WebSocket
from datetime import datetime
import json
import logging
import uvicorn
import random
from typing import List, Dict

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Sample data configuration
SAMPLE_DATA = [
    {"symbol": "AAPL", "base_price": 175.34, "volatility": 2.50, "volume_base": 1000000},
    {"symbol": "GOOGL", "base_price": 147.68, "volatility": 3.20, "volume_base": 800000},
    {"symbol": "MSFT", "base_price": 415.32, "volatility": 2.80, "volume_base": 900000},
    {"symbol": "AMZN", "base_price": 178.35, "volatility": 3.50, "volume_base": 750000},
    {"symbol": "META", "base_price": 510.92, "volatility": 4.00, "volume_base": 600000},
    {"symbol": "TSLA", "base_price": 172.63, "volatility": 5.00, "volume_base": 1200000},
    {"symbol": "NVDA", "base_price": 881.65, "volatility": 4.50, "volume_base": 850000},
    {"symbol": "JPM", "base_price": 183.27, "volatility": 1.80, "volume_base": 500000},
    {"symbol": "V", "base_price": 279.85, "volatility": 1.50, "volume_base": 400000},
    {"symbol": "WMT", "base_price": 162.14, "volatility": 1.20, "volume_base": 300000}
]

app = FastAPI(title="Stock Data Broadcasting Service")

# Store active websocket connections
active_connections: List[WebSocket] = []

def generate_realistic_price_movement(base_price: float, volatility: float) -> float:
    movement = random.gauss(0, volatility)
    percentage_change = movement / 100
    new_price = base_price * (1 + percentage_change)
    return round(new_price, 2)

def generate_realistic_volume(base_volume: int) -> int:
    variation = random.uniform(0.7, 1.3)
    return int(base_volume * variation)

async def fetch_stock_data() -> Dict:
    """Generate sample stock data with realistic movements"""
    current_data = {}
    
    for stock in SAMPLE_DATA:
        symbol = stock["symbol"]
        current_price = generate_realistic_price_movement(
            stock["base_price"], 
            stock["volatility"]
        )
        current_volume = generate_realistic_volume(stock["volume_base"])
        
        current_data[symbol] = {
            "price": current_price,
            "volume": current_volume,
            "change": round(((current_price - stock["base_price"]) / stock["base_price"]) * 100, 2)
        }
    
    return current_data

async def broadcast_stock_data(data: Dict):
    """Broadcast stock data to all connected clients"""
    if not active_connections:
        return
    
    message = json.dumps({
        "timestamp": datetime.now().isoformat(),
        "data": data
    })
    
    for connection in active_connections:
        try:
            await connection.send_text(message)
        except Exception as e:
            logger.error(f"Error broadcasting to client: {e}")
            active_connections.remove(connection)

async def stock_data_worker():
    """Worker that generates and broadcasts stock data every 5 minutes"""
    while True:
        try:
            data = await fetch_stock_data()
            await broadcast_stock_data(data)
            logger.info(f"Generated and broadcast data for {len(data)} symbols")
            await asyncio.sleep(60)
        except Exception as e:
            logger.error(f"Error in worker: {e}")
            await asyncio.sleep(10)

@app.on_event("startup")
async def startup():
    # Start the worker
    asyncio.create_task(stock_data_worker())

@app.websocket("/ws/stocks")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    active_connections.append(websocket)
    
    # Send initial data immediately upon connection
    initial_data = await fetch_stock_data()
    await websocket.send_text(json.dumps({
        "timestamp": datetime.now().isoformat(),
        "data": initial_data
    }))
    
    try:
        while True:
            await websocket.receive_text()
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
    finally:
        active_connections.remove(websocket)

if __name__ == "__main__":
    uvicorn.run("broadcaster:app", host="0.0.0.0", port=8001, reload=True)