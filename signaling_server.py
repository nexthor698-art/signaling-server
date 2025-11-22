# signaling_server.py
# Servidor de señalización para WebRTC
# No transmite video. Solo conecta ambos clientes.

import json
import websockets
import asyncio

# Lista de clientes conectados
connected = set()

async def handler(websocket):
    # Agregar cliente a la lista
    connected.add(websocket)
    print("Cliente conectado")

    try:
        async for message in websocket:
            # Cuando recibimos un mensaje, lo mandamos a todos los demás
            for client in connected:
                if client != websocket:
                    await client.send(message)

    except:
        pass

    # Cuando se desconecta
    connected.remove(websocket)
    print("Cliente desconectado")

# Ejecutar el servidor WebSocket
async def main():
    async with websockets.serve(handler, "0.0.0.0", 10000):
        print("Servidor de señalización WebRTC activo en puerto 10000")
        await asyncio.Future()  # Mantener servidor activo

asyncio.run(main())
