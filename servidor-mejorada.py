# server.py
import asyncio
import websockets
import os
import json
import uuid

LAPTOPS = {}
VIEWERS = set()

async def send_json(ws, obj):
    await ws.send(json.dumps(obj))

async def handler(ws, path):
    print("Nueva conexión:", ws.remote_address)
    try:
        first = await ws.recv()
        if first == "laptop":
            client_id = uuid.uuid4().hex[:8]
            LAPTOPS[client_id] = ws
            print("Laptop conectada:", client_id)
            await send_json(ws, {"type": "ASSIGN_ID", "id": client_id})
            await broadcast_viewers({"type": "LAPTOP_LIST", "ids": list(LAPTOPS.keys())})

            async for raw in ws:
                try:
                    obj = json.loads(raw)
                    obj["from"] = client_id
                    await broadcast_viewers(obj)
                except Exception:
                    await broadcast_viewers_raw(client_id, raw)

        elif first == "viewer":
            VIEWERS.add(ws)
            print("Viewer conectado")
            await send_json(ws, {"type": "LAPTOP_LIST", "ids": list(LAPTOPS.keys())})

            async for message in ws:
                try:
                    obj = json.loads(message)
                except:
                    continue

                if obj.get("type") == "CONTROL":
                    target = obj.get("target")
                    if target in LAPTOPS:
                        try:
                            await LAPTOPS[target].send(json.dumps(obj))
                        except websockets.exceptions.ConnectionClosed:
                            print("Laptop desconectada enviando CONTROL")
                    else:
                        await send_json(ws, {"type":"ERROR","msg":"target not connected"})
                elif obj.get("type") == "REQUEST_LAPTOP_LIST":
                    await send_json(ws, {"type":"LAPTOP_LIST", "ids": list(LAPTOPS.keys())})

        else:
            print("Rol desconocido")
            await ws.close()

    except websockets.exceptions.ConnectionClosed:
        print("Conexión cerrada")

    finally:
        dead_id = None
        for cid, sock in list(LAPTOPS.items()):
            if sock is ws:
                dead_id = cid
                break

        if dead_id:
            del LAPTOPS[dead_id]
            print("Laptop desconectada:", dead_id)
            await broadcast_viewers({"type":"LAPTOP_LIST", "ids": list(LAPTOPS.keys())})

        if ws in VIEWERS:
            VIEWERS.remove(ws)
            print("Viewer desconectado")

async def broadcast_viewers(obj):
    data = json.dumps(obj)
    dead = set()
    for v in VIEWERS:
        try:
            await v.send(data)
        except websockets.exceptions.ConnectionClosed:
            dead.add(v)

    for d in dead:
        VIEWERS.remove(d)

async def broadcast_viewers_raw(client_id, raw):
    obj = {"type":"FRAME_RAW", "from": client_id, "data": raw}
    await broadcast_viewers(obj)

async def main():
    port = int(os.environ.get("PORT", 8765))
    print("Servidor escuchando en 0.0.0.0:", port)
    async with websockets.serve(handler, "0.0.0.0", port):
        await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())
