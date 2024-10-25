import asyncio
import json
import websockets
from fastapi import FastAPI
import uvicorn
from datetime import datetime

app = FastAPI(title="Stock Data Consumer Service")

# Store the latest tick data
latest_tick_data = {}

async def connect_to_broadcaster():
    """Connect to the broadcasting service and store latest data"""
    while True:
        try:
            async with websockets.connect('ws://localhost:8001/ws/stocks') as websocket:
                while True:
                    data = await websocket.recv()
                    global latest_tick_data
                    latest_tick_data = json.loads(data)
        except Exception as e:
            print(f"Connection error: {e}")
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