import asyncio
import json
import websockets
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
from datetime import datetime
import logging
from typing import Dict, Optional, Tuple, List, Set

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Stock Data Consumer Service")

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Store active WebSocket connections
active_connections: Set[WebSocket] = set()

# Store the latest tick data
latest_tick_data: Dict = {}
# Store historical prices for each stock
stock_prices: Dict[str, list] = {}
# Store previous EMAs for each stock (both 38 and 100 periods)
previous_emas_38: Dict[str, float] = {}
previous_emas_100: Dict[str, float] = {}
# Store previous crossover states to detect changes
previous_states: Dict[str, str] = {}  # 'bullish', 'bearish', or None

def validate_float(value: float) -> Optional[float]:
    """Validate and sanitize float values for JSON compliance"""
    try:
        if value is None:
            return None
        float_val = float(value)
        # Check for non-finite values
        if float_val in (float('inf'), float('-inf')) or float_val != float_val:  # last condition checks for NaN
            return None
        # Round to reasonable precision to avoid floating point issues
        return round(float_val, 4)
    except (ValueError, TypeError):
        return None

def calculate_ema(stock_id: str, current_price: float, period: int, ema_dict: Dict[str, float]) -> float:
    """
    Calculate EMA according to the formula:
    EMAᵗₛ,ᵢ = (closeₛ,ᵢ * (2/(1+j))) + EMAᵗ⁻¹ₛ,ᵢ * (1 - (2/(1+j)))
    """
    if stock_id not in ema_dict:
        ema_dict[stock_id] = 0  # Initial EMA₀ₛ,ᵢ = 0
    
    # Calculate multiplier (2/(1+j))
    multiplier = 2 / (1 + period)
    
    # Calculate EMA
    ema = (current_price * multiplier) + (ema_dict[stock_id] * (1 - multiplier))
    
    # Store for next calculation
    ema_dict[stock_id] = ema
    
    return ema

def detect_breakout_patterns(stock_id: str, ema38: float, ema100: float) -> Optional[str]:
    """
    Detect breakout patterns based on EMA crossovers.
    Bullish breakout: EMA38 > EMA100 and previous EMA38 <= EMA100
    Bearish breakout: EMA38 < EMA100 and previous EMA38 >= EMA100
    """
    current_state = None
    
    if ema38 > ema100:
        current_state = 'bullish'
    elif ema38 < ema100:
        current_state = 'bearish'
    
    # Get previous state
    prev_state = previous_states.get(stock_id)
    
    # Update state
    previous_states[stock_id] = current_state
    
    # Detect crossovers
    if prev_state != current_state and prev_state is not None:
        if current_state == 'bullish':
            return 'BULLISH_BREAKOUT'
        elif current_state == 'bearish':
            return 'BEARISH_BREAKOUT'
    
    return None

def calculate_statistics(stock_id: str, current_price: float) -> Dict:
    """Calculate statistics for a stock based on its price history"""
    # Validate input price first
    safe_current_price = validate_float(current_price)
    if safe_current_price is None:
        return {
            "current_price": None,
            "price_change": None,
            "price_change_percent": None,
            "ema38": None,
            "ema100": None,
            "samples_collected": len(stock_prices.get(stock_id, []))
        }
    
    if stock_id not in stock_prices:
        stock_prices[stock_id] = []
    
    stock_prices[stock_id].append(safe_current_price)
    
    # Keep only last 100 prices for moving averages
    if len(stock_prices[stock_id]) > 100:
        stock_prices[stock_id] = stock_prices[stock_id][-100:]
    
    try:
        # Calculate both EMAs
        ema38 = calculate_ema(stock_id, safe_current_price, 38, previous_emas_38)
        ema100 = calculate_ema(stock_id, safe_current_price, 100, previous_emas_100)
        
        # Validate EMAs
        safe_ema38 = validate_float(ema38)
        safe_ema100 = validate_float(ema100)
        
        # Initialize statistics with validated values
        stats = {
            "current_price": safe_current_price,
            "ema38": safe_ema38,
            "ema100": safe_ema100,
            "samples_collected": len(stock_prices[stock_id])
        }
        
        # Calculate price change if we have at least 2 samples
        if len(stock_prices[stock_id]) >= 2:
            previous_price = stock_prices[stock_id][-2]
            if previous_price and previous_price != 0:  # Prevent division by zero
                price_change = safe_current_price - previous_price
                price_change_percent = (price_change / previous_price) * 100
                
                # Validate calculated changes
                safe_price_change = validate_float(price_change)
                safe_price_change_percent = validate_float(price_change_percent)
                
                if safe_price_change is not None:
                    stats["price_change"] = safe_price_change
                if safe_price_change_percent is not None:
                    stats["price_change_percent"] = safe_price_change_percent
        
        # Only detect breakout if we have valid EMAs
        if safe_ema38 is not None and safe_ema100 is not None:
            breakout = detect_breakout_patterns(stock_id, safe_ema38, safe_ema100)
            if breakout:
                stats["breakout"] = breakout
        
        return stats
    except Exception as e:
        logger.error(f"Error calculating statistics for stock {stock_id}: {e}")
        return {
            "current_price": safe_current_price,
            "price_change": None,
            "price_change_percent": None,
            "ema38": None,
            "ema100": None,
            "samples_collected": len(stock_prices[stock_id])
        }

async def broadcast_to_clients(data: dict):
    """Broadcast data to all connected WebSocket clients"""
    if not active_connections:
        return
    
    for connection in active_connections.copy():
        try:
            await connection.send_json(data)
        except Exception as e:
            logger.error(f"Error broadcasting to client: {e}")
            active_connections.remove(connection)

async def connect_to_broadcaster():
    """Connect to the broadcasting service and process stock data"""
    while True:
        try:
            async with websockets.connect('ws://localhost:8000/ws/stocks') as websocket:
                logger.info("Connected to data service successfully")
                logger.info("Monitoring for EMA crossover patterns (j₁=38, j₂=100)")
                
                while True:
                    data = await websocket.recv()
                    parsed_data = json.loads(data)
                    
                    trading_data = parsed_data.get("data", {})
                    trading_time = trading_data.get("trading_time")
                    trading_date = trading_data.get("trading_date")
                    stocks_data = trading_data.get("stocks", {})
                    
                    logger.info(f"\nProcessing data for {len(stocks_data)} stocks at {trading_time}")
                    
                    # Process each stock
                    processed_stocks = {}
                    for stock_id, stock_info in stocks_data.items():
                        try:
                            current_price = float(stock_info.get("price")) if stock_info.get("price") is not None else None
                            stats = calculate_statistics(stock_id, current_price)
                            
                            processed_stocks[stock_id] = {
                                **stock_info,  # Include original data
                                **stats,       # Add calculated statistics
                            }
                            
                            # Log EMAs and any breakout patterns
                            log_msg = f"Stock {stock_id}: Price={current_price:.2f}, EMA38={stats['ema38']:.2f}, EMA100={stats['ema100']:.2f}"
                            if 'breakout' in stats:
                                log_msg += f" 🚨 {stats['breakout']} DETECTED! 🚨"
                            logger.info(log_msg)
                            
                        except Exception as e:
                            logger.error(f"Error processing stock {stock_id}: {e}")
                            processed_stocks[stock_id] = {
                                **stock_info,
                                "error": str(e)
                            }
                    
                    # Update latest tick data with processed information
                    global latest_tick_data
                    latest_tick_data = {
                        "timestamp": parsed_data.get("timestamp"),
                        "trading_time": trading_time,
                        "trading_date": trading_date,
                        "stocks": processed_stocks
                    }
                    
                    # Broadcast to WebSocket clients
                    await broadcast_to_clients({
                        "timestamp": latest_tick_data["timestamp"],
                        "stocks": [
                            {
                                "stock_id": stock_id,
                                "current_price": validate_float(info.get("current_price")),
                                "ema38": validate_float(info.get("ema38")),
                                "ema100": validate_float(info.get("ema100")),
                                "breakout": info.get("breakout"),
                                "price_change": validate_float(info.get("price_change")),
                                "price_change_percent": validate_float(info.get("price_change_percent")),
                                "trading_time": trading_time
                            }
                            for stock_id, info in processed_stocks.items()
                        ]
                    })

        except Exception as e:
            logger.error(f"Connection error: {e}")
            await asyncio.sleep(5)  # Wait before reconnecting

@app.on_event("startup")
async def startup():
    # Start the WebSocket client
    asyncio.create_task(connect_to_broadcaster())

@app.get("/")
async def root():
    """Display the latest processed stock data"""
    return latest_tick_data

@app.get("/stock/{stock_id}")
async def get_stock(stock_id: str):
    """Get detailed information for a specific stock"""
    if not latest_tick_data or "stocks" not in latest_tick_data:
        return {"error": "No data available"}
    
    stock_data = latest_tick_data["stocks"].get(stock_id)
    if not stock_data:
        return {"error": f"No data available for stock {stock_id}"}
    
    return {
        "timestamp": latest_tick_data["timestamp"],
        "trading_time": latest_tick_data["trading_time"],
        "trading_date": latest_tick_data["trading_date"],
        "data": stock_data
    }

@app.get("/api/stocks")
async def get_stocks_data():
    """Get current EMA values and prices for all stocks."""
    if not latest_tick_data or "stocks" not in latest_tick_data:
        raise HTTPException(status_code=404, detail="No stock data available")
    
    try:
        stocks_data = []
        for stock_id, stock_info in latest_tick_data["stocks"].items():
            try:
                stock_data = {
                    "stock_id": stock_id,
                    "timestamp": latest_tick_data["timestamp"],
                    "trading_time": latest_tick_data.get("trading_time", ""),
                    "trading_date": latest_tick_data.get("trading_date", ""),
                    "current_price": validate_float(stock_info.get("current_price")),
                    "ema38": validate_float(stock_info.get("ema38")),
                    "ema100": validate_float(stock_info.get("ema100")),
                    "breakout": stock_info.get("breakout"),
                    "price_change": validate_float(stock_info.get("price_change")),
                    "price_change_percent": validate_float(stock_info.get("price_change_percent"))
                }
                # Remove None values to keep response clean
                stock_data = {k: v for k, v in stock_data.items() if v is not None}
                stocks_data.append(stock_data)
            except Exception as e:
                logger.error(f"Error processing stock {stock_id}: {e}")
                continue
        
        return {
            "timestamp": latest_tick_data["timestamp"],
            "stocks": stocks_data
        }
    except Exception as e:
        logger.error(f"Error in get_stocks_data: {e}")
        raise HTTPException(status_code=500, detail="Internal server error processing stock data")

@app.get("/api/stocks/{stock_id}/ema")
async def get_stock_ema(stock_id: str):
    """
    Get detailed EMA information for a specific stock
    Args:
        stock_id: The ID of the stock to get EMA data for
    Returns:
        Detailed EMA and price information for the specified stock
    """
    if not latest_tick_data or "stocks" not in latest_tick_data:
        raise HTTPException(status_code=404, detail="No stock data available")
    
    stock_data = latest_tick_data["stocks"].get(stock_id)
    if not stock_data:
        raise HTTPException(status_code=404, detail=f"No data available for stock {stock_id}")
    
    return JSONResponse(content={
        "stock_id": stock_id,
        "timestamp": latest_tick_data["timestamp"],
        "trading_time": latest_tick_data["trading_time"],
        "trading_date": latest_tick_data["trading_date"],
        "data": {
            "current_price": stock_data.get("current_price"),
            "ema38": stock_data.get("ema38"),
            "ema100": stock_data.get("ema100"),
            "breakout": stock_data.get("breakout"),
            "price_change": stock_data.get("price_change"),
            "price_change_percent": stock_data.get("price_change_percent"),
            "samples_collected": stock_data.get("samples_collected")
        }
    })

@app.get("/api/breakouts")
async def get_breakouts():
    """
    Get all stocks that are currently showing breakout patterns
    Returns:
        List of stocks with active breakout patterns
    """
    if not latest_tick_data or "stocks" not in latest_tick_data:
        raise HTTPException(status_code=404, detail="No stock data available")
    
    breakouts = []
    for stock_id, stock_info in latest_tick_data["stocks"].items():
        if "breakout" in stock_info:
            breakouts.append({
                "stock_id": stock_id,
                "breakout_type": stock_info["breakout"],
                "current_price": stock_info.get("current_price"),
                "ema38": stock_info.get("ema38"),
                "ema100": stock_info.get("ema100"),
                "trading_time": latest_tick_data["trading_time"]
            })
    
    return JSONResponse(content={
        "timestamp": latest_tick_data["timestamp"],
        "breakouts": breakouts
    })

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket endpoint for streaming stock data"""
    await websocket.accept()
    active_connections.add(websocket)
    
    try:
        # Send initial data
        if latest_tick_data:
            await websocket.send_json({
                "timestamp": latest_tick_data["timestamp"],
                "stocks": [
                    {
                        "stock_id": stock_id,
                        "current_price": validate_float(info.get("current_price")),
                        "ema38": validate_float(info.get("ema38")),
                        "ema100": validate_float(info.get("ema100")),
                        "breakout": info.get("breakout"),
                        "price_change": validate_float(info.get("price_change")),
                        "price_change_percent": validate_float(info.get("price_change_percent")),
                        "trading_time": latest_tick_data.get("trading_time")
                    }
                    for stock_id, info in latest_tick_data["stocks"].items()
                ]
            })
        
        # Keep connection alive and handle client messages if needed
        while True:
            data = await websocket.receive_text()
            # Handle any client messages here if needed
            
    except WebSocketDisconnect:
        logger.info("Client disconnected")
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
    finally:
        active_connections.remove(websocket)

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8002, reload=True)