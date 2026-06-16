# Sistema P2P Distribuído — Sprint 4

Sistema de balanceamento de carga dinâmico peer-to-peer com descoberta automática, negociação Master-to-Master e monitoramento de métricas em tempo real via supervisor externo.

---

## Sprints

| Sprint | Funcionalidade |
|--------|---------------|
| 1 | Heartbeat bidirecional Worker ↔ Master |
| 2 | Ciclo de tarefas (ALIVE / QUERY / NO_TASK / STATUS / ACK) + Discovery UDP |
| 3 | Negociação Master-to-Master + redirecionamento dinâmico de Workers |
| **4** | **Métricas de desempenho via TCP + dashboard do supervisor** |

---

## Arquitetura

```
                ┌─────────────────────────────────────┐
                │         Supervisor (professor)       │
                │       10.62.206.206 : 8002           │
                └──────────────┬──────────────────────┘
                               │  TCP / JSON (a cada 10s)
          ┌────────────────────┼────────────────────┐
          │                   │                    │
    ┌─────┴──────┐     ┌──────┴──────┐     ┌──────┴──────┐
    │  MASTER_8  │─M2M─│  MASTER_9  │─M2M─│  MASTER_N  │
    │ :7011 TCP  │     │ :7011 TCP  │     │ :7011 TCP  │
    └─────┬──────┘     └──────┬──────┘     └─────────────┘
          │ UDP :5000          │ UDP :5000
     ┌────┴────┐          ┌───┴────┐
     │Worker 1 │          │Worker A│
     │Worker 2 │          │Worker B│
     │Worker N │          │Worker C│
     └─────────┘          └────────┘
```

**Fluxo de mensagens por sprint:**

- **Sprint 1:** `HEARTBEAT` → `ALIVE`
- **Sprint 2:** `WORKER: ALIVE` → `TASK: QUERY` / `NO_TASK` → `STATUS: OK/NOK` → `ACK`
- **Sprint 3:** `request_help` ↔ `response_accepted/rejected` → `command_redirect` → `command_release`
- **Sprint 4:** `performance_report` (JSON) enviado ao supervisor a cada 10s

---

## Requisitos

- Python 3.10+
- `psutil` (opcional, mas recomendado para métricas reais de CPU/RAM/Disco)

```bash
pip install psutil
```

---

## Execução

### Master

```bash
# Básico
python master.py

# Com nome e vizinho
python master.py --master-name MASTER_8 --neighbors MASTER_9@10.62.206.50:7011

# Com capacidade customizada
python master.py --master-name MASTER_8 --capacity 10 --neighbors MASTER_9@10.62.206.50:7011
```

| Argumento | Padrão | Descrição |
|-----------|--------|-----------|
| `--master-name` | `MASTER_8` | Identificador do master (usado no dashboard) |
| `--port` | `7011` | Porta TCP para Workers |
| `--capacity` | `10` | Número de tarefas antes de acionar saturação |
| `--neighbors` | — | Masters vizinhos no formato `ID@ip:porta` |

### Worker

```bash
# Discovery automático (busca masters na rede)
python worker.py

# Conexão direta
python worker.py --master-host 10.62.206.44

# Com porta customizada
python worker.py --master-host 10.62.206.44 --master-port 7011
```

---

## Portas utilizadas

| Porta | Protocolo | Uso |
|-------|-----------|-----|
| 7011 | TCP | Comunicação Master ↔ Workers e Master ↔ Master |
| 5000 | UDP | Discovery broadcast (Workers encontram Masters) |
| 8002 | TCP | Envio de métricas ao supervisor do professor |

---

## Sprint 4 — Métricas e Supervisor

### Funcionamento

A cada **10 segundos**, o Master coleta métricas do sistema e da farm e envia via TCP para o supervisor em `10.62.206.206:8002`. A conexão é aberta, o JSON enviado e a conexão fechada — sem HTTP, sem resposta aguardada.

### Schema do payload (`sprint4-monitor`)

```json
{
  "server_uuid":     "MASTER-P2-9014",
  "hostname":        "MASTER_8",
  "role":            "master",
  "task":            "performance_report",
  "timestamp":       "2026-06-15T23:58:03Z",
  "message_id":      "a1b2c3d4-...",
  "payload_version": "sprint4-monitor",
  "performance": {
    "system": {
      "uptime_seconds":  3600,
      "load_average_1m": 1.25,
      "load_average_5m": 0.98,
      "cpu": {
        "usage_percent":  14.6,
        "count_logical":  8,
        "count_physical": 4
      },
      "memory": {
        "total_mb":     16384.0,
        "available_mb": 8192.0,
        "percent_used": 50.0,
        "memory_used":  8192.0
      },
      "disk": {
        "total_gb":    512.0,
        "free_gb":     250.0,
        "percent_used": 45.0
      }
    },
    "farm_state": {
      "workers": {
        "total_registered":          4,
        "workers_utilization":       2,
        "workers_alive":             4,
        "workers_idle":              2,
        "workers_borrowed":          0,
        "workers_received":          0,
        "workers_failed":            0,
        "workers_home":              4,
        "workers_available_capacity": 8,
        "borrowed_workers": []
      },
      "tasks": {
        "tasks_pending":     0,
        "tasks_running":     2,
        "tasks_completed":  42,
        "tasks_failed":      1,
        "oldest_task_age_s": 0.0
      }
    },
    "config_thresholds": {
      "max_task":            10,
      "warn_cpu_percent":    85,
      "warn_memory_percent": 85,
      "release_task":         6
    },
    "neighbors": [
      {
        "server_uuid":    "MASTER_9",
        "status":         "available",
        "last_heartbeat": "2026-06-15T23:58:00Z"
      }
    ]
  }
}
```

### Campos do dashboard

| Campo no dashboard | Campo no payload |
|--------------------|-----------------|
| CPU % | `performance.system.cpu.usage_percent` |
| MEM % | `performance.system.memory.percent_used` |
| Load 1m/5m | `performance.system.load_average_1m/5m` |
| Memória Tot/Disp/Use | `memory.total_mb / available_mb / memory_used` |
| Disco Tot/Liv/% | `disk.total_gb / free_gb / percent_used` |
| Tarefas P/R | `farm_state.tasks.tasks_pending / tasks_running` |
| Tarefas C/F | `farm_state.tasks.tasks_completed / tasks_failed` |
| Workers T/A/I/R/F | `total_registered / utilization / idle / received / borrowed` |
| Uptime | `performance.system.uptime_seconds` |
| Threshold | `config_thresholds.max_task` |
| Vizinhos | `performance.neighbors[]` |

---

## Sprint 3 — Negociação Master-to-Master

Quando a fila de tarefas ultrapassa `CAPACITY`, o master envia `request_help` aos vizinhos. O vizinho que aceitar redireciona workers ociosos via `command_redirect`. Quando a carga normaliza (abaixo de `RELEASE_THRESHOLD`), os workers são devolvidos via `command_release`.

```
MASTER_8 (saturado)          MASTER_9 (disponível)
     │                              │
     │──── request_help ───────────>│
     │<─── response_accepted ───────│  (oferece N workers)
     │                              │
     │  [WORKER envia ALIVE]        │
     │──── command_redirect ──────> Worker
     │                              │
     Worker ──── register_temporary_worker ──> MASTER_9
     Worker ──── ALIVE ─────────────────────> MASTER_9
```

---

## Sprint 2 — Discovery UDP e Eleição

Workers iniciam sem IP configurado. Enviam `DISCOVERY` via broadcast UDP (`255.255.255.255:5000`). Masters que respondem com `DISCOVERY_REPLY` são candidatos. O worker elege o master com **menor nome lexicográfico** e confirma com `ELECTION_ACK`.

---

## Comandos do terminal (Master)

Durante a execução do master, estão disponíveis:

| Comando | Ação |
|---------|------|
| `test` | Adiciona uma tarefa à fila |
| `status` | Exibe dashboard local (CPU, RAM, Disco, Workers, Tarefas) |
| `Ctrl+C` | Encerra o master com sumário final |

---

## Testado em sala

- Ambiente: rede local do laboratório (10.62.206.0/24)
- Supervisor: `10.62.206.206:8002` (servidor do professor)
- Masters rodando: MASTER_8 (`10.62.206.44`) + MASTER_9 (`10.62.206.50`)
- Workers: 4 instâncias simultâneas conectadas via discovery UDP
- Dashboard: MASTER_8 aparecendo com CPU, MEM, Workers e Tarefas em tempo real
- Negociação M2M: `request_help` ↔ `response_rejected` observados em saturação com `--capacity 1`

---

## Estrutura do projeto

```
Projeto P2P - Sprint_4/
├── master.py     # Servidor master (Sprint 1–4 completo)
└── worker.py     # Cliente worker (Sprint 1–3 completo)
```
