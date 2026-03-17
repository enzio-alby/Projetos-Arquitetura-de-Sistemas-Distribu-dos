import socket
import json
import uuid
import time

MASTER_IP = '10.62.206.35' 
MASTER_PORT = 5000

class Worker:
    def __init__(self):
        self.uuid = f"WORKER-Marlon-{uuid.uuid4().hex[:4].upper()}"

    def send_heartbeat(self):
        while True:
            sock = None
            try:
                sock = socket.create_connection((MASTER_IP, MASTER_PORT), timeout=3)
                payload = {
                    "SERVER_UUID": self.uuid,
                    "TASK": "HEARTBEAT"
                }
                sock.sendall((json.dumps(payload) + "\n").encode('utf-8'))
                
                data = sock.recv(1024).decode('utf-8')
                if data:
                    resp = json.loads(data.strip())
                    print(f"[STATUS] Master respondeu: {resp.get('RESPONSE')}")
            
            except Exception as e:
                print(f"[AVISO] Tentando reconectar... Erro: {e}")
            
            finally:
                if sock:
                    print('Conexão encerrada')
                    sock.close()
            
            time.sleep(5)

if __name__ == "__main__":
    w = Worker()
    print(f"Iniciando {w.uuid}...")
    w.send_heartbeat()
