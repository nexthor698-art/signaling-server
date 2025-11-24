# server.py
import asyncio
import websockets
import os
import json
import uuid

LAPTOPS = {}    # client_id -> websocket
VIEWERS = set() # websockets of viewer apps

async def send_json(ws, obj):
    await ws.send(json.dumps(obj))

async def handler(ws, path):
    print("Nueva conexión:", ws.remote_address)
    try:
        first = await ws.recv()
        # first message must be a simple role identifier
        if first == "laptop":
            client_id = uuid.uuid4().hex[:8]
            LAPTOPS[client_id] = ws
            print("Laptop conectada:", client_id)
            # send assigned id
            await send_json(ws, {"type":"ASSIGN_ID", "id": client_id})
            # notify viewers list update
            await broadcast_viewers({"type":"LAPTOP_LIST", "ids": list(LAPTOPS.keys())})
            async for raw in ws:
                # pass through frames and status messages to viewers
                # expect raw messages to be JSON for control/status or "FRAME" prefixed base64 data strings
                try:
                    obj = json.loads(raw)
                    # forward status/control notifications to viewers
                    if obj.get("type") in ("STATUS",):
                        # include origin id
                        obj["from"] = client_id
                        await broadcast_viewers(obj)
                    else:
                        # unknown JSON from laptop: forward anyway
                        obj["from"] = client_id
                        await broadcast_viewers(obj)
                except Exception:
                    # non-JSON: treat as raw frame with client id in a custom format
                    # To keep compatibility, forward as-is but prefix with client_id
                    await broadcast_viewers_raw(client_id, raw)
        elif first == "viewer":
            VIEWERS.add(ws)
            print("Viewer conectado")
            # send current laptop list
            await send_json(ws, {"type":"LAPTOP_LIST", "ids": list(LAPTOPS.keys())})
            async for message in ws:
                # messages from viewer are JSON commands (control) or requests
                try:
                    obj = json.loads(message)
                except Exception:
                    continue
                # If it's a control to laptop, forward to target laptop's ws
                if obj.get("type") == "CONTROL":
                    target = obj.get("target")
                    if target in LAPTOPS:
                        try:
                            await LAPTOPS[target].send(json.dumps(obj))
                        except websockets.exceptions.ConnectionClosed:
                            print("Laptop desconectada al intentar mandar control", target)
                    else:
                        await send_json(ws, {"type":"ERROR","msg":"target not connected","target":target})
                elif obj.get("type") == "REQUEST_LAPTOP_LIST":
                    await send_json(ws, {"type":"LAPTOP_LIST", "ids": list(LAPTOPS.keys())})
        else:
            print("Rol desconocido, cerrando")
            await ws.close()
    except websockets.exceptions.ConnectionClosed:
        print("Conexión cerrada")
    finally:
        # cleanup
        to_remove = None
        for cid, s in list(LAPTOPS.items()):
            if s is ws:
                to_remove = cid
                break
        if to_remove:
            del LAPTOPS[to_remove]
            print("Laptop desconectada (cleanup):", to_remove)
            await broadcast_viewers({"type":"LAPTOP_LIST", "ids": list(LAPTOPS.keys())})
        if ws in VIEWERS:
            VIEWERS.remove(ws)
            print("Viewer desconectado (cleanup)")

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
    # forward raw frames but wrap them in JSON for viewers
    obj = {"type":"FRAME_RAW", "from": client_id, "data": raw}
    await broadcast_viewers(obj)

async def main():
    port = int(os.environ.get("PORT", 8765))
    print("Servidor escuchando en 0.0.0.0:", port)
    async with websockets.serve(handler, "0.0.0.0", port):
        await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())
