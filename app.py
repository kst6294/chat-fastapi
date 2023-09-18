from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
import asyncio

from broadcaster import Broadcast

broadcast = Broadcast("redis://localhost:6379")


async def channel_ws_receiver(websocket, channel):
    async for message in websocket.iter_text():
        await broadcast.publish(channel=channel, message=message)

async def channel_ws_sender(websocket, channel):
    async with broadcast.subscribe(channel=channel) as subscriber:
        async for event in subscriber:
            await websocket.send_text(event.message)


app = FastAPI()

    
@app.websocket("/channel/{channel}")
async def chatroom_ws(websocket: WebSocket, channel):
    await websocket.accept()
    try:
        while True:
            receive_message_task = asyncio.create_task(channel_ws_receiver(websocket, channel))
            send_message_task = asyncio.create_task(channel_ws_sender(websocket, channel))
            done, pending = await asyncio.wait({receive_message_task, send_message_task}, return_when=asyncio.FIRST_COMPLETED)
            for task in pending:
                task.cancel()
            for task in done:
                task.result()
    except WebSocketDisconnect:
        await websocket.close()

@app.on_event("startup")
async def startup():
    print("connect")
    await broadcast.connect()

@app.on_event("shutdown")
async def shutdown():
    print("disconnect")
    await broadcast.disconnect()

