from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from starlette.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import uuid
import json
import os
# import httpx
from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta
import redis
from dotenv import load_dotenv
import asyncio
import logging
import time
import requests

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
# **** 本番 *****
path = '/etc/secrets/.env'
# **** ローカル *****
# path = './.env'

load_dotenv(path)
# **** 本番 *****
REDIS_URL = os.environ['REDIS_URL']
# **** ローカル *****
# REDIS_URL = "rediss://***"
kv_store = redis.from_url(REDIS_URL)

# Microsoft Graph API の設定
TENANT_ID = os.environ['TENANT_ID']
CLIENT_ID = os.environ['CLIENT_ID']
CLIENT_KEY = os.environ['CLIENT_KEY']
UPLOAD_ITEM_ID = os.environ['UPLOAD_ITEM_ID']

app = FastAPI(title="LLM Query Management API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Pydantic model for request validation
class GoodButtonRequest(BaseModel):
    receipt_number: str
    status: str
    content: str

# Pydantic model for response
class GoodButtonResponse(BaseModel):
    success: bool
    message: str
    filename: Optional[str] = None

def getToken() -> str:
    """Microsoft Graph API のアクセストークンを取得"""
    headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/x-www-form-urlencoded'
    }

    payload = {
        'client_id': CLIENT_ID,
        'scope': 'https://graph.microsoft.com/.default',
        'grant_type': 'client_credentials',
        'client_secret': CLIENT_KEY
    }

    TokenGet_URL = "https://login.microsoftonline.com/" + TENANT_ID + "/oauth2/v2.0/token"

    response = requests.post(
        TokenGet_URL,
        headers=headers,
        data=payload
    )
    
    if response.status_code != 200:
        raise Exception(f"Token request failed: {response.status_code} - {response.text}")
    
    jsonObj = json.loads(response.text)
    token = jsonObj.get("access_token", None)
    if token is None:
        raise Exception("token is None")
    return token

def upload_json_to_onedrive(token: str, json_data: dict, upload_item_id: str, filename: str) -> bool:
    """JSONデータをOneDriveにアップロード"""
    try:
        url = f"https://graph.microsoft.com/v1.0/sites/26ac19c0-6b34-4ba6-b66d-1901dda55bb1/drive/items/{upload_item_id}:/{filename}:/content"
        headers = {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json'
        }
        
        response = requests.put(url, headers=headers, data=json.dumps(json_data, ensure_ascii=False))
        
        if response.status_code != 200 and response.status_code != 201:
            print(f"Upload failed: {response.status_code} - {response.text}")
            return False
            
        return True
        
    except Exception as e:
        print(f"Upload error: {str(e)}")
        return False

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

            status = answer_data["data"]["status"]
            if status == "completed":
                await kv_delete(receipt_number)
                logger.info(f"Receipt number {receipt_number} deleted")

    async def send_status(self, receipt_number: str, status: str, timestamp: str = None):
        """Send status update to all clients waiting for this receipt number"""
        if timestamp is None:
            timestamp = datetime.utcnow().isoformat()
        
        status_data = {
            "type": "status",
            "receipt_number": receipt_number,
            "data": {
                "status": status,
                "timestamp": timestamp
            }
        }

        if receipt_number in self.active_connections:
            connections_to_remove = []
            for connection in self.active_connections[receipt_number]:
                try:
                    await connection.send_json(status_data)
                    logger.info(f"Status update sent via WebSocket for receipt: {receipt_number}")
                except Exception as e:
                    logger.error(f"Error sending WebSocket status message: {e}")
                    connections_to_remove.append(connection)
            
            # Remove failed connections
            for connection in connections_to_remove:
                self.active_connections[receipt_number].remove(connection)
            
            # Clean up empty connection lists
            if not self.active_connections[receipt_number]:
                del self.active_connections[receipt_number]

class LLMConnectionManager:
    """Manages WebSocket connections for LLM servers"""
    def __init__(self):
        # Store active LLM server connections
        self.active_connections: List[WebSocket] = []
        self.connection_info: Dict[WebSocket, Dict[str, Any]] = {}
    
    async def connect(self, websocket: WebSocket, server_id: str = None):
        await websocket.accept()
        self.active_connections.append(websocket)
        self.connection_info[websocket] = {
            "server_id": server_id or f"llm_server_{len(self.active_connections)}",
            "connected_at": datetime.utcnow().isoformat(),
            "queries_sent": 0
        }
        logger.info(f"LLM server connected: {self.connection_info[websocket]['server_id']}")
    
    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            server_info = self.connection_info.get(websocket, {})
            server_id = server_info.get("server_id", "unknown")
            self.active_connections.remove(websocket)
            if websocket in self.connection_info:
                del self.connection_info[websocket]
            logger.info(f"LLM server disconnected: {server_id}")
    
    async def send_query_to_available_server(self, query_data: dict) -> bool:
        """Send query to an available LLM server using round-robin"""
        if not self.active_connections:
            logger.warning("No LLM servers available to process query")
            return False
        
        # Simple round-robin selection (you can implement more sophisticated load balancing)
        connection = self.active_connections[0]
        # Move the connection to the end for round-robin
        self.active_connections.append(self.active_connections.pop(0))
        
        try:
            await connection.send_json({
                "type": "query",
                "data": query_data
            })
            
            # Update statistics
            if connection in self.connection_info:
                self.connection_info[connection]["queries_sent"] += 1
            
            logger.info(f"Query sent to LLM server: {query_data['receipt_number']}")
            return True
        except Exception as e:
            logger.error(f"Error sending query to LLM server: {e}")
            # Remove the failed connection
            self.disconnect(connection)
            
            # Try with next available server if any
            if self.active_connections:
                return await self.send_query_to_available_server(query_data)
            return False

manager = ConnectionManager()
llm_manager = LLMConnectionManager()

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

class UpdateStatusRequest(BaseModel):
    receipt_number: str
    status: str

class QueueItem(BaseModel):
    receipt_number: str
    query: str
    timestamp: str
    status: str

# 新しいレスポンスモデルを追加
class QueueItemWithWaitCount(BaseModel):
    receipt_number: str
    query: str
    timestamp: str
    status: str
    wait_count: int  # 自分以外の"queued", "processing"の数

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
    
async def get_pending_count():
    """Redisを使用してpending状態のアイテム数を効率的に取得"""
    try:
        count = 0
        for key in kv_store.scan_iter(match="*"):
            data = kv_store.get(key)
            if data and json.loads(data)["status"] == "pending":
                count += 1
        return count
    except Exception as e:
        logger.error(f"Error getting pending count: {e}")
        return 0

async def get_pending_items() -> Optional[Dict]:
    """Get the oldest pending item from the queue"""
    try:
        oldest_item = None
        oldest_timestamp = None
        oldest_key = None
        pending_count = 0
        
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
                            pending_count += 1
            except (json.JSONDecodeError, KeyError, ValueError) as e:
                logger.error(f"Error processing item {key}: {e}")
                continue
        
        return (oldest_key, oldest_item, pending_count) if oldest_item else None
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

# WebSocket endpoint for LLM servers
@app.websocket("/llm_ws/{server_id}")
async def llm_websocket_endpoint(websocket: WebSocket, server_id: str):
    global pending_count
    
    await llm_manager.connect(websocket, server_id)
    try:
        while True:
            try:
                # Listen for messages from LLM serverd
                message = await websocket.receive_json()

                # Get the oldest pending item
                if pending_count > 0:
                    pending_result = await get_pending_items()
                else:
                    pending_result = None

                if None != pending_result:
                    queue_key, queue_item, count = pending_result
                    await set_pending_count(count)

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
                    await websocket.send_json(status_message)
                    await sub_pending_count()
                    return
                else:
                    await set_pending_count(0)

                # 各処理の後に短い待機を追加
                await asyncio.sleep(1)  # 100ミリ秒の待機
                    
            except WebSocketDisconnect:
                logger.info(f"LLM server {server_id} disconnected")
                break
            except Exception as e:
                logger.error(f"LLM WebSocket error for server {server_id}: {e}")
                await asyncio.sleep(1)  # エラー時は長めの待機
    finally:
        llm_manager.disconnect(websocket)

async def get_wait_count(exclude_receipt_number: str = None) -> int:
    """自分以外の"queued", "processing"の数を取得"""
    try:
        count = 0
        for key_bytes in kv_store.scan_iter():
            key = key_bytes.decode() if isinstance(key_bytes, bytes) else str(key_bytes)
            try:
                item_data = kv_store.get(key)
                if item_data:
                    item = json.loads(item_data.decode())

                    # timestampを取得（Noneの場合はスキップ）
                    timestamp_str = item.get("timestamp")
                    if not timestamp_str:
                        logger.warning(f"No timestamp found for item {key}, skipping")
                        continue
                    
                    # timestampをdatetimeオブジェクトに変換
                    try:
                        item_timestamp = datetime.fromisoformat(timestamp_str)
                    except ValueError as e:
                        logger.error(f"Invalid timestamp format for item {key}: {timestamp_str}, skipping")
                        continue
                    
                    # timestampが60分以上前の場合はカウントしない
                    cutoff_time = datetime.utcnow() - timedelta(minutes=60)
                    if item_timestamp < cutoff_time:
                        continue

                    status = item.get("status", "")
                    receipt_number = item.get("receipt_number", "")
                    
                    # 自分以外の "pending", "queued", "processing" をカウント
                    if (status in ["pending", "queued", "processing"] and 
                        receipt_number != exclude_receipt_number):
                        count += 1
            except (json.JSONDecodeError, KeyError, ValueError) as e:
                logger.error(f"Error processing item {key}: {e}")
                continue
        
        return count
    except Exception as e:
        logger.error(f"Error getting wait count: {e}")
        return 0
    
# API Endpoints
@app.post("/set_query", response_model=QueueItemWithWaitCount)
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
            "status": "queued",  # "pending"から"queued"に変更
        }
        
        # Store in KV store
        success = await kv_set(receipt_number, json.dumps(query_data))
        
        if not success:
            raise HTTPException(status_code=500, detail="Failed to store query")
        
        # Immediately try to send to an available LLM server
        query_sent = await llm_manager.send_query_to_available_server(query_data)
        
        if query_sent:
            # Update status to processing
            query_data["status"] = "processing"
            await kv_set(receipt_number, json.dumps(query_data))
            status = "sent_to_llm"
        else:
            query_data["status"] = "pending"
            await kv_set(receipt_number, json.dumps(query_data))
            status = "queued_no_llm_available"
            await add_pending_count()
            logger.warning(f"No LLM servers available for query: {receipt_number}")
        
        # 待ち人数を計算（自分以外の"queued", "processing"の数）
        # 少し待ってからカウントを取得（データベースの更新を待つ）
        await asyncio.sleep(0.1)
        wait_count = await get_wait_count(receipt_number)
        
        return QueueItemWithWaitCount(
            receipt_number=receipt_number,
            query=request.query,
            timestamp=timestamp,
            status="queued",
            wait_count=wait_count
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
        
        print(f"pending_result: {pending_result}")
        queue_key, queue_item, pending_count = pending_result

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
        # 非同期でWebSocketクライアントに通知
        asyncio.create_task(manager.send_answer(queue_item["receipt_number"], status_message))

        # APIレスポンスとしてクエリ情報を返す（LLMサーバー用）
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
            await sub_pending_count()
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

# 受付番号キューを削除する
@app.get("/delete_query/{receipt_number}")
async def delete_query(receipt_number: str):
    """Delete a query"""
    data_str = await kv_get(receipt_number)
    
    if not data_str:
        raise HTTPException(
            status_code=404, 
            detail="Receipt number not found"
        )
    await kv_delete(receipt_number)
    return {"message": f"Query {receipt_number} deleted"}

# 受付番号キューをキャンセルする (status が "pending", "queued", "processing" の場合削除する)
@app.get("/cancel_query/{receipt_number}")
async def cancel_query(receipt_number: str):
    """Cancel a query"""
    data_str = await kv_get(receipt_number)
        
    if not data_str:
        raise HTTPException(
            status_code=404, 
            detail="Receipt number not found"
        )
    try:
        data = json.loads(data_str)
    except json.JSONDecodeError:
        raise HTTPException(
            status_code=400, 
            detail="cancel_query: Invalid JSON data"
        )
        
    if data["status"] in ["pending", "queued", "processing"]:
        await kv_delete(receipt_number)
        return {"message": "Query cancelled"}
    else:
        raise HTTPException(
            status_code=400, 
            detail="Query is not pending, queued, or processing"
        )
    
@app.post("/update_status", response_model=Dict[str, str])
async def update_status(request: UpdateStatusRequest):
    """
    Update the status of a query and notify WebSocket clients.
    """
    try:
        receipt_number = request.receipt_number
        new_status = request.status
        timestamp = datetime.utcnow().isoformat()
        
        # Check if query exists
        query_data_str = await kv_get(receipt_number)
        if not query_data_str:
            raise HTTPException(status_code=404, detail="Receipt number not found")
        
        # Parse existing data
        try:
            query_data = json.loads(query_data_str)
        except json.JSONDecodeError:
            raise HTTPException(status_code=400, detail="Invalid JSON data in store")
        
        # Update status
        query_data["status"] = new_status
        query_data["timestamp"] = timestamp
        
        # Store updated data
        success = await kv_set(receipt_number, json.dumps(query_data))
        
        if not success:
            raise HTTPException(status_code=500, detail="Failed to update status")
        
        # Notify WebSocket clients
        await manager.send_status(receipt_number, new_status, timestamp)
        
        return {"status": "success", "message": f"Status updated to {new_status} and clients notified"}
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")
    
@app.post("/good-button", response_model=GoodButtonResponse)
async def submit_good_button(request: GoodButtonRequest):
    """
    Good buttonの結果を受け付けてOneDriveに保存
    
    - **receipt_number**: 受付番号
    - **status**: ステータス
    - **content**: 内容
    """
    try:
        # 現在時刻を取得
        now = datetime.now()
        datetime_str = now.strftime("%Y-%m-%d %H:%M:%S")
        
        # ファイル名を生成（受付番号）
        filename = f"good_button_{request.receipt_number}.json"
        
        # JSONデータを構築
        good_button_json = {
            "receipt_number": request.receipt_number,
            "datetime": datetime_str,
            "status": request.status,
            "content": request.content,
        }
        token = getToken()
        if token is None:
            raise HTTPException(status_code=500, detail="Failed to get token")
        # OneDriveにアップロード
        success = upload_json_to_onedrive(token, good_button_json, UPLOAD_ITEM_ID, filename)
        
        if success:
            return GoodButtonResponse(
                success=True,
                message="Good button result saved successfully",
                filename=filename
            )
        else:
            raise HTTPException(status_code=500, detail="Failed to upload to OneDrive")
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

# 登録してある keyvalue を全て削除
@app.get("/delete_all_keyvalue")
async def delete_all_keyvalue():
    """Delete all keyvalue"""
    for key in kv_store.scan_iter():
        kv_store.delete(key)
    return {"message": "All keyvalue deleted"}

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

# asyncioのロックを使用
lock = asyncio.Lock()
pending_count = 0
async def add_pending_count():
    global pending_count
    async with lock:
        pending_count += 1

async def sub_pending_count():
    global pending_count
    async with lock:
        if pending_count > 0:
            pending_count -= 1

async def set_pending_count(count: int):
    global pending_count
    async with lock:
        pending_count = count