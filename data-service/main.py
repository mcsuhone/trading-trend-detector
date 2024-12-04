import asyncio
import aiohttp
from fastapi import FastAPI, WebSocket
from datetime import datetime
import json
import logging
import uvicorn
import pandas as pd
from typing import List, Dict
import os

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Stock Data Broadcasting Service")

# Store active websocket connections
active_connections: List[WebSocket] = []

# Global variables to store the data
stock_data = None
current_index = 0

# Define allowed stocks
ALLOWED_STOCKS = {'EUPE5.FR', 'IPLEM.FR', 'IUBU9.FR'}

def load_stock_data():
    """Load stock data from CSV file"""
    global stock_data
    csv_path = '../data/extracted_stocks.csv'
    
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"Could not find {csv_path}. Please run data_extractor.py first.")
    
    logger.info(f"Loading stock data from {csv_path}")
    stock_data = pd.read_csv(csv_path)
    
    # Filter only allowed stocks
    stock_data = stock_data[stock_data['ID'].isin(ALLOWED_STOCKS)]
    stock_data = stock_data.sort_values(['Trading time'])
    
    # Verify we have all required stocks
    found_stocks = set(stock_data['ID'].unique())
    missing_stocks = ALLOWED_STOCKS - found_stocks
    if missing_stocks:
        logger.warning(f"Missing data for stocks: {missing_stocks}")
    
    logger.info(f"Loaded {len(stock_data)} records for stocks: {sorted(found_stocks)}")

async def fetch_stock_data() -> Dict:
    """Fetch the next batch of stock data"""
    global current_index, stock_data
    
    if stock_data is None:
        load_stock_data()
    
    # Reset index if we've reached the end
    if current_index >= len(stock_data):
        current_index = 0
        logger.info("Reached end of data, starting over")
    
    # Get all records for the current timestamp
    current_time = stock_data.iloc[current_index]['Trading time']
    current_batch = stock_data[stock_data['Trading time'] == current_time]
    
    # Prepare the data structure
    current_data = {
        "trading_time": current_time,
        "trading_date": current_batch.iloc[0]['Trading date'],
        "stocks": {}
    }
    
    # Add data for allowed stocks
    for stock_id in ALLOWED_STOCKS:
        stock_row = current_batch[current_batch['ID'] == stock_id]
        if not stock_row.empty:
            current_data["stocks"][stock_id] = {
                "price": float(stock_row.iloc[0]['Last']),
                "sec_type": stock_row.iloc[0]['SecType']
            }
        else:
            # If no data for this stock at this timestamp, use previous known price
            last_known = stock_data[
                (stock_data['ID'] == stock_id) & 
                (stock_data['Trading time'] < current_time)
            ].iloc[-1] if not stock_data[
                (stock_data['ID'] == stock_id) & 
                (stock_data['Trading time'] < current_time)
            ].empty else None
            
            if last_known is not None:
                current_data["stocks"][stock_id] = {
                    "price": float(last_known['Last']),
                    "sec_type": last_known['SecType'],
                    "price_type": "last_known"  # Indicate this is not current data
                }
    
    # Move to next timestamp
    current_index += len(current_batch)
    
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
    """Worker that streams stock data every second"""
    while True:
        try:
            data = await fetch_stock_data()
            await broadcast_stock_data(data)
            logger.info(f"Broadcast data for {len(data['stocks'])} stocks at {data['trading_time']}")
            await asyncio.sleep(1)  # Stream data every second
        except Exception as e:
            logger.error(f"Error in worker: {e}")
            await asyncio.sleep(10)

@app.on_event("startup")
async def startup():
    # Load the data first
    load_stock_data()
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
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)