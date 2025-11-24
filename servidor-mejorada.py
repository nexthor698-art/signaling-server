import asyncio
import websockets
import os
import uuid
import json # Por si quieres enviar comandos más complejos en el futuro

# Estructura para almacenar clientes conectados:
# {websocket: {"role": "laptop"/"pc", "id": unique_id, "target_id": None}}
clients = {} 
laptop_data = {} # {laptop_id: websocket_de_laptop}

async def handler(websocket, path):
    print(f"Nueva conexión desde {websocket.remote_address}")
    client_info = {"role": None, "id": str(uuid.uuid4()), "target_id": None}
    
    try:
        # Esperamos el primer mensaje que nos diga quién es ("laptop:ID" o "pc")
        role_msg = await websocket.recv()
        
        if ":" in role_msg: 
            role, unique_id = role_msg.split(":", 1)
            # Usar un ID proporcionado por la laptop (ej: nombre de máquina)
            client_info["id"] = unique_id 
        else:
             role = role_msg
        
        client_info["role"] = role
        clients[websocket] = client_info

        if role == "laptop":
            print(f"Laptop conectada (Emisor) con ID: {client_info['id']}")
            laptop_data[client_info["id"]] = websocket 

            # Notificar a los visores que hay una nueva laptop
            message = f"NEW_LAPTOP_ID:{client_info['id']}"
            for ws, info in clients.items():
                if info["role"] == "pc":
                    await ws.send(message)

        elif role == "pc":
            print(f"PC conectada (Visor) con ID: {client_info['id']}")
            # Cuando un visor se conecta, le enviamos las IDs de todas las laptops
            available_laptops = ",".join(laptop_data.keys())
            if available_laptops:
                await websocket.send(f"LAPTOP_LIST:{available_laptops}")
        else:
            await websocket.close()
            return

        # Bucle principal de retransmisión y recepción de comandos
        async for message in websocket:
            if client_info["role"] == "laptop":
                # Video de Laptop: "HEADER:DATA" -> Reenviar como "LAPTOP_ID:HEADER:DATA"
                full_message = f"{client_info['id']}:{message}"

                # Enviar a todos los visores
                disconnected_pcs = set()
                for ws, info in clients.items():
                    if info["role"] == "pc":
                        try:
                            await ws.send(full_message)
                        except websockets.exceptions.ConnectionClosed:
                            disconnected_pcs.add(ws)
                
                # Limpiar clientes visores desconectados
                for ws in disconnected_pcs:
                    del clients[ws]
            
            elif client_info["role"] == "pc":
                # Comando de Visor: Formato esperado: "TARGET_ID:COMMAND"
                if ":" in message:
                    target_id, command = message.split(":", 1)
                    if target_id in laptop_data:
                        laptop_ws = laptop_data[target_id]
                        try:
                            await laptop_ws.send(command) # Enviar el comando a la laptop
                        except websockets.exceptions.ConnectionClosed:
                            print(f"Error al enviar comando. Laptop {target_id} desconectada.")
                            pass
                    else:
                        print(f"ID de Laptop no encontrada para comando: {target_id}")


    except websockets.exceptions.ConnectionClosed:
        print(f"Conexión cerrada: {client_info['role']} (ID: {client_info['id']})")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        # Limpieza final al desconectarse
        if websocket in clients:
            if clients[websocket]["role"] == "laptop":
                # Notificar a los visores que esta laptop se fue
                message = f"LAPTOP_GONE_ID:{clients[websocket]['id']}"
                for ws, info in clients.items():
                    if info["role"] == "pc":
                        try:
                            await ws.send(message)
                        except websockets.exceptions.ConnectionClosed:
                            # Ignoramos si el visor ya estaba cerrando
                            pass
                
                if clients[websocket]["id"] in laptop_data:
                    del laptop_data[clients[websocket]["id"]]

            del clients[websocket]

async def main():
    port = int(os.environ.get("PORT", 8765))
    print(f"Iniciando servidor en puerto {port}...")
    
    async with websockets.serve(handler, "0.0.0.0", port):
        await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())
