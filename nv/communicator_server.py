import asyncio
import json
import uuid
from typing import List

import uvicorn
from fastapi import FastAPI, WebSocket
from starlette.websockets import WebSocketDisconnect

DELIVERY_TIMEOUT_SECONDS = 15


class ActiveConnection:
    def __init__(self, websocket: WebSocket, client_id: str):
        self.websocket = websocket
        self.client_id = client_id


class ConnectionManager:
    def __init__(self):
        self.active_connections: List[ActiveConnection] = []

    async def connect(self, websocket: WebSocket, client_id: str):
        await websocket.accept()
        self.active_connections.append(ActiveConnection(websocket, client_id))

    def disconnect(self, websocket: WebSocket):
        for connection in self.active_connections:
            if websocket == connection.websocket:
                self.active_connections.remove(connection)

    async def _send_message_by_websocket(self, message: str, websocket: WebSocket):
        await websocket.send_text(message)

    async def send_message_by_client_id(self, message: str, client_id: str):
        for connection in self.active_connections:
            if client_id == connection.client_id:
                await self._send_message_by_websocket(message, connection.websocket)


class DeliveryTask:
    def __init__(self, task_id, source, destination, payload):
        self.task_id = task_id
        self.source_id = source
        self.destination_id = destination
        self.payload = payload

        self.result_to_server_from_destination = ""
        self.result_to_destination_from_server = ""
        self.result_to_source = ""

        self.timeout_handle: asyncio.Task | None = None


def run_communicator_server(port: int = 8765, delivery_timeout_seconds: int = DELIVERY_TIMEOUT_SECONDS):
    global DELIVERY_TIMEOUT_SECONDS
    print("Starting up...")

    DELIVERY_TIMEOUT_SECONDS = delivery_timeout_seconds

    uvicorn.run("main:engine.app", host="0.0.0.0", port=port, reload=False)


class CommunicatorServer:
    def __init__(self):

        self.app = FastAPI()
        self.manager = ConnectionManager()

        self.app.websocket("/{client_id}")(self.ws_handler)

        self.active_tasks: List[DeliveryTask] = []

        print("Communicator server ready!")
        pass

    async def parse_data(self, data, client_id):
        if "destination" in data:
            current_task = DeliveryTask(str(uuid.uuid4()), client_id, data["destination"], data["payload"])

            has_client = False
            for connection in self.manager.active_connections:
                if connection.client_id == data["destination"]:
                    has_client = True
                    break
            if not has_client:
                current_task.result_to_source = "ERR_SRV__NO_ACTIVE_CLIENT_WITH_CURRENT_DESTINATION_ID"
                await self.send_result_to_source(current_task)
                return

            self.active_tasks.append(current_task)
            current_task.timeout_handle = asyncio.create_task(self._watch_timeout(current_task))
            await self.send_payload_to_destination(current_task)
        if "result" in data:
            current_task: DeliveryTask | None = None
            for task in self.active_tasks:
                if task.task_id == data["id"]:
                    current_task = task

            if current_task is None:
                stale_task = DeliveryTask(data["id"], "None", client_id, "")
                stale_task.result_to_destination_from_server = "ERR_SRV__NO_DELIVERY_TASK_WITH_CURRENT_ID"
                await self.send_result_to_destination(stale_task)
                return

            self._finish_task(current_task)

            current_task.result_to_destination_from_server = "ok"
            await self.send_result_to_destination(current_task)

            current_task.result_to_server_from_destination = data["result"]

            if current_task.result_to_server_from_destination == "ok":
                current_task.result_to_source = "ok"
            else:
                current_task.result_to_source = "ERR_DESTINATION__" + current_task.result_to_server_from_destination
            await self.send_result_to_source(current_task)

    async def _watch_timeout(self, task: DeliveryTask):
        try:
            await asyncio.sleep(DELIVERY_TIMEOUT_SECONDS)
        except asyncio.CancelledError:
            return

        if task not in self.active_tasks:
            return

        self.active_tasks.remove(task)
        task.result_to_source = "ERR_SRV__DESTINATION_TIMEOUT"
        await self.send_result_to_source(task)

    def _finish_task(self, task: DeliveryTask):
        if task.timeout_handle is not None:
            task.timeout_handle.cancel()
        if task in self.active_tasks:
            self.active_tasks.remove(task)

    async def _handle_disconnect(self, client_id: str):
        affected = [task for task in self.active_tasks if
                    task.destination_id == client_id or task.source_id == client_id]

        for task in affected:
            self._finish_task(task)

            if task.destination_id == client_id:
                task.result_to_source = "ERR_SRV__DESTINATION_DISCONNECTED"
                await self.send_result_to_source(task)

    async def ws_handler(self, websocket: WebSocket, client_id: str):
        await self.manager.connect(websocket, client_id)
        try:
            while True:
                data = await websocket.receive_text()
                await self.parse_data(json.loads(data), client_id)
        except WebSocketDisconnect:
            self.manager.disconnect(websocket)
            await self._handle_disconnect(client_id)

    async def send_payload_to_destination(self, task: DeliveryTask):
        await self.manager.send_message_by_client_id(
            json.dumps({"id": task.task_id, "source": task.source_id, "payload": task.payload}), task.destination_id)

    async def send_result_to_destination(self, task: DeliveryTask):
        await self.manager.send_message_by_client_id(
            json.dumps({"id": task.task_id, "result": task.result_to_destination_from_server}), task.destination_id)

    async def send_result_to_source(self, task: DeliveryTask):
        await self.manager.send_message_by_client_id(json.dumps({"id": task.task_id, "result": task.result_to_source}),
            task.source_id)
