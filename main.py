from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from typing import Dict, List
import json
import logging
import os
import uvicorn

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("nexuscue")

app = FastAPI(title="NexusCue WebSocket Server")


class ConnectionManager:
    def __init__(self):
        # In-memory room storage: suitable for a single server instance/demo.
        self.active_rooms: Dict[str, List[WebSocket]] = {}

    async def connect(self, websocket: WebSocket, room_id: str):
        await websocket.accept()
        self.active_rooms.setdefault(room_id, []).append(websocket)
        logger.info(
            "Device connected to %s. Total users: %s",
            room_id,
            len(self.active_rooms[room_id]),
        )

    def disconnect(self, websocket: WebSocket, room_id: str):
        room = self.active_rooms.get(room_id)
        if room and websocket in room:
            room.remove(websocket)
            logger.info(
                "Device disconnected from %s. Total users: %s",
                room_id,
                len(room),
            )
            if not room:
                del self.active_rooms[room_id]
                logger.info("Room %s deleted.", room_id)

    async def broadcast_to_room(
        self, message: str, room_id: str, sender: WebSocket
    ):
        room = self.active_rooms.get(room_id, [])
        dead_connections = []

        for connection in list(room):
            if connection is sender:
                continue
            try:
                await connection.send_text(message)
            except Exception:
                logger.exception("Failed to send message in room %s", room_id)
                dead_connections.append(connection)

        for connection in dead_connections:
            self.disconnect(connection, room_id)


manager = ConnectionManager()


@app.get("/")
async def root():
    return {"status": "online", "service": "NexusCue WebSocket Server"}


@app.get("/health")
async def health():
    return {"status": "healthy"}


@app.websocket("/chat/{room_id}")
async def chat_endpoint(websocket: WebSocket, room_id: str):
    await manager.connect(websocket, room_id)

    join_message = json.dumps({
        "type": "system",
        "username": "System",
        "text": "A peer has joined the room.",
    })
    await manager.broadcast_to_room(join_message, room_id, sender=websocket)

    try:
        while True:
            data = await websocket.receive_text()
            logger.info("Message received in room %s", room_id)
            await manager.broadcast_to_room(data, room_id, sender=websocket)

    except WebSocketDisconnect:
        logger.info("WebSocket disconnected from room %s", room_id)
    except Exception:
        logger.exception("Unexpected WebSocket error in room %s", room_id)
    finally:
        manager.disconnect(websocket, room_id)
        leave_message = json.dumps({
            "type": "system",
            "username": "System",
            "text": "A peer has left the room.",
        })
        await manager.broadcast_to_room(leave_message, room_id, sender=websocket)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8000"))
    uvicorn.run(app, host="0.0.0.0", port=port)
