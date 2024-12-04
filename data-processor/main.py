import asyncio
import json
import websockets
from fastapi import FastAPI
import uvicorn
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Stock Data Consumer Service")

# Store the latest tick data
latest_tick_data = {}

async def connect_to_broadcaster():
    """Connect to the broadcasting service and store latest data"""
    while True:
        try:
            async with websockets.connect('ws://localhost:8000/ws/stocks') as websocket:
                logger.info("Connected to data service successfully")
                
                while True:
                    data = await websocket.recv()
                    parsed_data = json.loads(data)
                    
                    # Log the received data structure
                    logger.info("Received data at %s", parsed_data.get("timestamp"))
                    logger.info("Number of stocks: %d", len(parsed_data.get("data", {})))
                    
                    # Log sample of the data for the first stock
                    if parsed_data.get("data"):
                        first_symbol = next(iter(parsed_data["data"]))
                        logger.info("Sample data for %s: %s", 
                                  first_symbol, 
                                  parsed_data["data"][first_symbol])
                    
                    global latest_tick_data
                    latest_tick_data = parsed_data

        except Exception as e:
            logger.error(f"Connection error: {e}")
            await asyncio.sleep(5)  # Wait before reconnecting

@app.on_event("startup")
async def startup():
    # Start the WebSocket client
    asyncio.create_task(connect_to_broadcaster())

@app.get("/")
async def root():
    """Display the latest tick data"""
    return {
        "timestamp": latest_tick_data.get("timestamp", "No data yet"),
        "stocks": latest_tick_data.get("data", {})
    }

if __name__ == "__main__":
    uvicorn.run("consumer:app", host="0.0.0.0", port=8002, reload=True)