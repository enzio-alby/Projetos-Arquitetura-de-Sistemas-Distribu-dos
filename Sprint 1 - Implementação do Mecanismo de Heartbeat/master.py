import socket
import json
import uuid
import threading

HOST = '10.62.206.35'
PORT = 5000
MASTER_UUID = f"MASTER-Enzio-{uuid.uuid4().hex[:4].upper()}"

def handle_worker(conn, addr):
    """
    Função que será executada em uma thread separada para cada Worker.
    """
    try:
        data = conn.recv(1024).decode('utf-8')
        if not data:
            return
        
        raw_payload = data.strip().split('\n')[0]
        payload = json.loads(raw_payload)

        if payload.get("TASK") == "HEARTBEAT":
            worker_id = payload.get("SERVER_UUID", "Desconhecido")
            print(f" [+] Heartbeat recebido de: {worker_id} [{addr[0]}]")

            response_payload = {
                "SERVER_UUID": MASTER_UUID,
                "TASK": "HEARTBEAT",
                "RESPONSE": "ALIVE"
            }

            response_json = json.dumps(response_payload) + "\n"
            conn.sendall(response_json.encode('utf-8'))
            print(f" [->] Resposta 'ALIVE' enviada para {worker_id}.")
        
    except json.JSONDecodeError:
        print(f" ⚠️ Erro ao decodificar JSON de {addr}")
    except Exception as e:
        print(f" ❌ Erro no processamento de {addr}: {e}")
    finally:
        conn.close()

def start_master():
    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    
    try:
        server_sock.bind((HOST, PORT))
        server_sock.listen()
        
        print(f"=== Master {MASTER_UUID} Online ===")
        print(f"IP: {HOST} | Porta: {PORT}")
        print("Aguardando conexões múltiplas...\n")

        while True:
            conn, addr = server_sock.accept()
            
            client_thread = threading.Thread(target=handle_worker, args=(conn, addr))
            client_thread.daemon = True
            client_thread.start()
            

    except Exception as e:
        print(f"❌ Erro fatal no servidor: {e}")
    finally:
        server_sock.close()

if __name__ == "__main__":
    start_master()