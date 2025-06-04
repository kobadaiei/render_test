from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from pydantic import BaseModel
import uuid
import json
import os
# import httpx
from typing import Optional, Dict, Any, List
from datetime import datetime
import redis
from dotenv import load_dotenv
import asyncio
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

path = '/etc/secrets/.env'
# path = './.env'
load_dotenv(path)
REDIS_URL = os.environ['REDIS_URL']
# Render KV Store configuration
kv_store = redis.from_url(REDIS_URL)

app = FastAPI(title="LLM Query Management API", version="1.0.0")

# WebSocket connection manager
class ConnectionManager:
    def __init__(self):
        # Store active connections by receipt_number
        self.active_connections: Dict[str, List[WebSocket]] = {}
    
    async def connect(self, websocket: WebSocket, receipt_number: str):
        await websocket.accept()
        if receipt_number not in self.active_connections:
            self.active_connections[receipt_number] = []
        self.active_connections[receipt_number].append(websocket)
        logger.info(f"WebSocket connected for receipt: {receipt_number}")
    
    def disconnect(self, websocket: WebSocket, receipt_number: str):
        if receipt_number in self.active_connections:
            if websocket in self.active_connections[receipt_number]:
                self.active_connections[receipt_number].remove(websocket)
            if not self.active_connections[receipt_number]:
                del self.active_connections[receipt_number]
        logger.info(f"WebSocket disconnected for receipt: {receipt_number}")
    
    async def send_answer(self, receipt_number: str, answer_data: dict):
        """Send answer to all clients waiting for this receipt number"""
        if receipt_number in self.active_connections:
            connections_to_remove = []
            for connection in self.active_connections[receipt_number]:
                try:
                    await connection.send_json(answer_data)
                    logger.info(f"Answer sent via WebSocket for receipt: {receipt_number}")
                except Exception as e:
                    logger.error(f"Error sending WebSocket message: {e}")
                    connections_to_remove.append(connection)
            
            # Remove failed connections
            for connection in connections_to_remove:
                self.active_connections[receipt_number].remove(connection)
            
            # Clean up empty connection lists
            if not self.active_connections[receipt_number]:
                del self.active_connections[receipt_number]

manager = ConnectionManager()

class AnswerRequest(BaseModel):
    receipt_number: str
    answer: str

class AnswerResponse(BaseModel):
    receipt_number: str
    answer: str
    timestamp: str
    status: str

class QueryRequest(BaseModel):
    query: str

class QueueItem(BaseModel):
    receipt_number: str
    query: str
    timestamp: str
    status: str

class WebSocketMessage(BaseModel):
    type: str  # "answer", "status", "error"
    receipt_number: str
    data: Dict[str, Any]

async def kv_set(key: str, value: str) -> bool:
    try:
        kv_store.set(key, value)
        return True
    except Exception as e:
        logger.error(f"Error setting key {key}: {e}")
        return False

async def kv_get(key: str) -> Optional[str]:
    try:
        result = kv_store.get(key)
        return result.decode() if result else None
    except Exception as e:
        logger.error(f"Error getting key {key}: {e}")
        return None
    
async def kv_delete(key: str) -> bool:
    try:
        kv_store.delete(key)
        return True
    except Exception as e:
        logger.error(f"Error deleting key {key}: {e}")
        return False

async def get_pending_items() -> Optional[Dict]:
    """Get the oldest pending item from the queue"""
    try:
        oldest_item = None
        oldest_timestamp = None
        oldest_key = None
        
        for key_bytes in kv_store.scan_iter():
            key = key_bytes.decode() if isinstance(key_bytes, bytes) else str(key_bytes)
            try:
                item_data = kv_store.get(key)
                if item_data:
                    item = json.loads(item_data.decode())
                    if item.get("status") == "pending":
                        item_timestamp = datetime.fromisoformat(item["timestamp"])
                        if oldest_timestamp is None or item_timestamp < oldest_timestamp:
                            oldest_item = item
                            oldest_timestamp = item_timestamp
                            oldest_key = key
            except (json.JSONDecodeError, KeyError, ValueError) as e:
                logger.error(f"Error processing item {key}: {e}")
                continue
        
        return (oldest_key, oldest_item) if oldest_item else None
    except Exception as e:
        logger.error(f"Error get_pending_items: {e}")
        return None

# WebSocket endpoint for real-time answer delivery
@app.websocket("/ws/{receipt_number}")
async def websocket_endpoint(websocket: WebSocket, receipt_number: str):
    await manager.connect(websocket, receipt_number)
    
    try:
        # Check if answer is already available
        data_str = await kv_get(receipt_number)
        if data_str:
            data = json.loads(data_str)
            if data["status"] == "completed":
                # Send existing answer immediately
                answer_response = {
                    "type": "answer",
                    "receipt_number": receipt_number,
                    "data": {
                        "answer": data.get("answer", ""),
                        "timestamp": data["timestamp"],
                        "status": data["status"]
                    }
                }
                await websocket.send_json(answer_response)
            else:
                # Send current status
                status_response = {
                    "type": "status",
                    "receipt_number": receipt_number,
                    "data": {
                        "status": data["status"],
                        "timestamp": data["timestamp"]
                    }
                }
                await websocket.send_json(status_response)
        else:
            # Receipt number not found
            error_response = {
                "type": "error",
                "receipt_number": receipt_number,
                "data": {
                    "message": "Receipt number not found"
                }
            }
            await websocket.send_json(error_response)
            return
        
        # Keep connection alive and listen for any messages
        while True:
            try:
                # Wait for any message from client (heartbeat, etc.)
                await asyncio.wait_for(websocket.receive_text(), timeout=30.0)
            except asyncio.TimeoutError:
                # Send heartbeat to keep connection alive
                heartbeat = {
                    "type": "heartbeat",
                    "receipt_number": receipt_number,
                    "data": {"timestamp": datetime.utcnow().isoformat()}
                }
                await websocket.send_json(heartbeat)
            except WebSocketDisconnect:
                break
                
    except WebSocketDisconnect:
        pass
    except Exception as e:
        logger.error(f"WebSocket error for receipt {receipt_number}: {e}")
    finally:
        manager.disconnect(websocket, receipt_number)

# API Endpoints
@app.post("/set_query", response_model=QueueItem)
async def set_query(request: QueryRequest):
    """
    Issue a unique receipt number as a key and spool the query.
    """
    try:
        # Generate unique receipt number
        receipt_number = str(uuid.uuid4())
        timestamp = datetime.utcnow().isoformat()
        
        # Create query data
        query_data = {
            "receipt_number": receipt_number,
            "query": request.query,
            "timestamp": timestamp,
            "status": "pending",
        }
        
        # Store in KV store
        success = await kv_set(receipt_number, json.dumps(query_data))
        
        if not success:
            raise HTTPException(status_code=500, detail="Failed to store query")
        
        return QueueItem(
            receipt_number=receipt_number,
            query=request.query,
            timestamp=timestamp,
            status="queued"
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@app.get("/get_query", response_model=Optional[QueueItem])
async def get_query():
    """
    Return the unprocessed key and query from the spool.
    Returns the oldest pending query and marks it as processing.
    """
    try:
        # Get the oldest pending item
        pending_result = await get_pending_items()
        
        if not pending_result:
            return None
        
        queue_key, queue_item = pending_result

        # Mark the item as processing
        queue_item["status"] = "processing"
        await kv_set(queue_key, json.dumps(queue_item))

        # Notify WebSocket clients about status change
        status_message = {
            "type": "status",
            "receipt_number": queue_item["receipt_number"],
            "data": {
                "status": "processing",
                "timestamp": queue_item["timestamp"]
            }
        }
        await manager.send_answer(queue_item["receipt_number"], status_message)

        return QueueItem(
            receipt_number=queue_item["receipt_number"],
            query=queue_item["query"],
            timestamp=queue_item["timestamp"],
            status="processing",
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@app.post("/set_answer", response_model=Dict[str, str])
async def set_answer(request: AnswerRequest):
    """
    Store the answer for a given receipt number and notify WebSocket clients.
    """
    try:
        receipt_number = request.receipt_number
        timestamp = datetime.utcnow().isoformat()
        
        # Check if query exists
        query_data_str = await kv_get(receipt_number)
        if not query_data_str:
            raise HTTPException(status_code=404, detail="Receipt number not found")
        
        # Create answer data
        answer_data = {
            "answer": request.answer,
            "timestamp": timestamp,
            "status": "completed",
        }
        
        # Store answer
        success = await kv_set(receipt_number, json.dumps(answer_data))
        
        if not success:
            raise HTTPException(status_code=500, detail="Failed to store answer")
        
        # Notify WebSocket clients
        websocket_message = {
            "type": "answer",
            "receipt_number": receipt_number,
            "data": {
                "answer": request.answer,
                "timestamp": timestamp,
                "status": "completed"
            }
        }
        await manager.send_answer(receipt_number, websocket_message)
        
        return {"status": "success", "message": "Answer stored and clients notified"}
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@app.get("/get_answer/{receipt_number}", response_model=AnswerResponse)
async def get_answer(receipt_number: str):
    """
    Return the answer for the key. (Kept for backward compatibility)
    Note: Consider using WebSocket endpoint for real-time updates.
    """
    try:
        data_str = await kv_get(receipt_number)
        
        if not data_str:
            raise HTTPException(
                status_code=404, 
                detail="Receipt number not found"
            )
        
        data = json.loads(data_str)
        
        if data["status"] == "pending":
            raise HTTPException(
                status_code=202, 
                detail=f"Query is {data['status']}, answer not yet available"
            )
        elif data["status"] == "processing":
            raise HTTPException(
                status_code=202, 
                detail=f"Query is {data['status']}, processing"
            )
        elif data["status"] == "completed":
            return AnswerResponse(
                receipt_number=receipt_number,
                answer=data.get("answer", ""),
                timestamp=data["timestamp"],
                status=data["status"]
            )
        else:
            raise HTTPException(
                status_code=500,
                detail=f"Unknown status: {data['status']}"
            )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

# Additional utility endpoints
@app.get("/status/{receipt_number}")
async def get_status(receipt_number: str):
    """Get the status of a query"""
    try:
        data_str = await kv_get(receipt_number)
        
        if not data_str:
            raise HTTPException(status_code=404, detail="Receipt number not found")
        
        data = json.loads(data_str)
        return {
            "receipt_number": receipt_number,
            "status": data["status"],
            "timestamp": data["timestamp"]
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@app.get("/")
async def root():
    """Health check endpoint"""
    return {"message": "LLM Query Management API with WebSocket support is running", "status": "healthy"}

# WebSocket status endpoint for debugging
@app.get("/websocket/status")
async def websocket_status():
    """Get current WebSocket connection status"""
    return {
        "active_connections": {
            receipt_number: len(connections) 
            for receipt_number, connections in manager.active_connections.items()
        },
        "total_receipts_with_connections": len(manager.active_connections)
    }
