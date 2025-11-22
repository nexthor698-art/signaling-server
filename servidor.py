import asyncio
import websockets

laptop_clients = set()
pc_clients = set()

async def handler(websocket, path):
    try:
        role = await websocket.recv()  # "laptop" o "pc"
        if role == "laptop":
            laptop_clients.add(websocket)
        elif role == "pc":
            pc_clients.add(websocket)
        else:
            await websocket.close()
            return

        async for message in websocket:
            # reenviar a todos los PCs conectados
            for pc in pc_clients:
                try:
                    await pc.send(message)
                except:
                    pass

    finally:
        laptop_clients.discard(websocket)
        pc_clients.discard(websocket)

start_server = websockets.serve(handler, "0.0.0.0", 8765)
asyncio.get_event_loop().run_until_complete(start_server)
print("Servidor WebSocket corriendo en puerto 8765")
asyncio.get_event_loop().run_forever()
