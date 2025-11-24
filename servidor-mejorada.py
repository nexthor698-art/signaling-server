import asyncio
import websockets
import os
import uuid # Usaremos esto para generar IDs únicas

# Estructuras para guardar los clientes conectados
# { laptop_id: websocket }
laptop_clients = {}
# { websocket: role }
pc_clients = set()

# Función para enviar un comando a una laptop específica
async def send_command_to_laptop(laptop_id, command):
    """Busca la laptop por ID y le envía el comando."""
    if laptop_id in laptop_clients:
        laptop_ws = laptop_clients[laptop_id]
        try:
            print(f"Ruteando comando '{command}' a laptop ID: {laptop_id}")
            await laptop_ws.send(f"CMD:{command}")
            return True
        except websockets.exceptions.ConnectionClosed:
            print(f"Laptop {laptop_id} desconectada. No se pudo enviar el comando.")
            # Eliminamos la laptop si está desconectada
            del laptop_clients[laptop_id]
            return False
    else:
        print(f"Error: Laptop con ID '{laptop_id}' no encontrada.")
        return False

async def handler(websocket, path):
    print(f"Nueva conexión desde {websocket.remote_address}")
    role = None
    client_id = None
    
    try:
        # 1. Esperamos el primer mensaje para saber el rol y la ID
        # Formato: "laptop:<UUID>" o "pc"
        initial_message = await websocket.recv()
        
        if initial_message.startswith("laptop:"):
            role = "laptop"
            # El cliente nos envía su ID única (UUID)
            client_id = initial_message.split(":", 1)[1]
            if client_id in laptop_clients:
                 # Si el ID ya existe, generamos uno nuevo para esta sesión (esto es un parche,
                 # pero idealmente el cliente debe asegurar su unicidad)
                 client_id = str(uuid.uuid4())
            laptop_clients[client_id] = websocket
            print(f"Laptop conectada (ID: {client_id})")
        elif initial_message == "pc":
            role = "pc"
            pc_clients.add(websocket)
            print("PC conectada (Visor)")
        else:
            await websocket.close()
            return

        # 2. Bucle principal de retransmisión
        async for message in websocket:
            if role == "laptop":
                # Si viene de la laptop (ej: "CAM:base64data"),
                # la prefijamos con la ID y la enviamos a TODOS los visores
                message_with_id = f"{client_id}:{message}"
                
                if not pc_clients:
                    continue 

                disconnected_pcs = set()
                for pc in pc_clients:
                    try:
                        await pc.send(message_with_id)
                    except websockets.exceptions.ConnectionClosed:
                        disconnected_pcs.add(pc)
                
                pc_clients.difference_update(disconnected_pcs)

            elif role == "pc":
                # Si viene del Visor (PC), esperamos comandos del tipo:
                # "FREEZE:<laptop_id>" o "KILLAPP:<laptop_id>:<app_name>"
                if ":" in message:
                    target_id, command_data = message.split(":", 1)
                    await send_command_to_laptop(target_id, command_data)
                
    except websockets.exceptions.ConnectionClosed:
        print(f"Conexión cerrada: {role} (ID: {client_id})")
    except Exception as e:
        print(f"Error en handler: {e}")
    finally:
        # 3. Limpieza final al desconectarse
        if role == "laptop" and client_id in laptop_clients:
            del laptop_clients[client_id]
        elif role == "pc" and websocket in pc_clients:
            pc_clients.discard(websocket)
        print(f"Clientes activos: Laptops={len(laptop_clients)}, Visores={len(pc_clients)}")

async def main():
    port = int(os.environ.get("PORT", 8765))
    print(f"Iniciando servidor en puerto {port}...")
    
    async with websockets.serve(handler, "0.0.0.0", port):
        await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())
