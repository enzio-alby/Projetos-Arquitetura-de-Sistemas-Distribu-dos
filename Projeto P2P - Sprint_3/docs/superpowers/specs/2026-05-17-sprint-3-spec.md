# Sprint 3 — Especificação de Design: Protocolo M2M e Redirecionamento Dinâmico de Workers (2026-05-17)

**Tópico:** Implementar negociação Master-to-Master (M2M) com empréstimo e devolução de Workers, satisfazendo os objetivos O4, O5 e O6 do plano do projeto.

**Resumo/Objetivo:** Master saturado detecta sobrecarga, solicita Workers emprestados a Master vizinho via protocolo JSON/TCP, redireciona o Worker para o Master saturado, e devolve quando a carga normaliza. O sistema deve ser interoperável com implementações de outras equipes usando apenas os payloads definidos.

---

## Contexto do projeto

- Arquivos centrais: `masterp2.py` (porta TCP 7011, discovery UDP 5000), `workerp2.py`.
- Arquivos legados (ignorar): `master/master.py`, `worker/worker.py`.
- Sprints 1 e 2 já operacionais: heartbeat, ALIVE/QUERY/NO_TASK/STATUS/ACK, discovery UDP, eleição de temp master.
- Protocolo base: mensagens JSON terminadas em `\n` via TCP.

---

## 1) Protocolo M2M — Estrutura padrão

Toda mensagem M2M usa a estrutura genérica abaixo. O campo `"type"` é **minúsculo** (distingue de campos `"TYPE"` maiúsculo das Sprints 1/2):

```json
{
  "type":       "TIPO_DA_MENSAGEM",
  "request_id": "uuid-v4-único",
  "payload":    { }
}
```

### 1.1 Mensagens definidas

**`request_help`** (Master A → Master B)
```json
{
  "type": "request_help",
  "request_id": "<uuid4>",
  "payload": {
    "master_id":      "MASTER_A",
    "master_address": "ip_master_A:porta",
    "current_load":   12,
    "capacity":       10,
    "workers_needed": 2
  }
}
```
- `master_address` é extensão necessária para que Master B saiba onde instruir os Workers a se reconectar.
- Campos adicionais devem ser ignorados por parsers de outras equipes (strict parsing com tolerância).

**`response_accepted`** (Master B → Master A)
```json
{
  "type": "response_accepted",
  "request_id": "<mesmo uuid4 do request_help>",
  "payload": {
    "workers_offered": 2,
    "worker_details": [
      { "id": "WORKER-X", "address": "ip_worker:5001" }
    ]
  }
}
```

**`response_rejected`** (Master B → Master A)
```json
{
  "type": "response_rejected",
  "request_id": "<mesmo uuid4>",
  "payload": {
    "reason": "high_load | no_workers_available | refused"
  }
}
```

**`command_redirect`** (Master B → Worker B1, via resposta ao ALIVE)
```json
{
  "type": "command_redirect",
  "request_id": "<novo uuid4>",
  "payload": {
    "new_master_address": "ip_master_A:porta"
  }
}
```

**`register_temporary_worker`** (Worker B1 → Master A)
```json
{
  "type": "register_temporary_worker",
  "request_id": "<novo uuid4>",
  "payload": {
    "worker_id":               "WORKER-B1",
    "original_master_address": "ip_master_B:porta"
  }
}
```

**`command_release`** (Master A → Worker B1, via resposta ao ALIVE)
```json
{
  "type": "command_release",
  "request_id": "<novo uuid4>",
  "payload": {
    "original_master_address": "ip_master_B:porta"
  }
}
```

**`notify_worker_returned`** (Master A → Master B)
```json
{
  "type": "notify_worker_returned",
  "request_id": "<novo uuid4>",
  "payload": {
    "worker_id": "WORKER-B1"
  }
}
```

---

## 2) Regras de parsing e compatibilidade

| Regra | Detalhe |
|-------|---------|
| Strict Parsing | Campos desconhecidos são ignorados; ausência de campos obrigatórios gera log de erro sem derrubar o processo |
| Case Sensitivity | Valores de `type` em minúsculas exatamente como definidos; campos da Sprint 1/2 (`TASK`, `WORKER`) permanecem maiúsculos |
| `request_id` | UUID v4 gerado por `uuid.uuid4()`. `response_accepted`/`response_rejected` reutilizam o `request_id` da requisição original; demais mensagens geram novo UUID |
| Timeout M2M | Master solicitante aguarda resposta por 5 segundos antes de considerar vizinho indisponível |
| Delimitador | `\n` ao final de cada JSON, igual às Sprints anteriores |

---

## 3) Detecção de saturação e histerese

| Parâmetro | Padrão | Configurável via CLI |
|-----------|--------|----------------------|
| `CAPACITY` (saturação) | 10 | `--capacity N` |
| `RELEASE_THRESHOLD` (liberação) | 60% de CAPACITY | automático |

- Quando `task_queue.qsize() > CAPACITY`: dispara `request_help`.
- Quando `task_queue.qsize() < RELEASE_THRESHOLD` E há `borrowed_workers`: agenda `command_release`.
- **Histerese:** RELEASE_THRESHOLD < CAPACITY evita empréstimo e devolução imediatos do mesmo Worker (efeito ping-pong).

---

## 4) Fluxo de empréstimo — passo a passo

```
1. saturation_monitor detecta qsize > CAPACITY
2. Master A abre TCP para Master B, envia request_help
3. Master B avalia carga própria e known_workers (vistos <60s, não cedidos)
   → Tem capacidade: adiciona Workers a redirect_targets, responde response_accepted
   → Sem capacidade: responde response_rejected
4. Próximo ALIVE do Worker B1 em Master B: Master B detecta redirect_targets[worker_uuid]
   → Responde command_redirect com new_master_address = Master A
5. Worker B1 recebe command_redirect em _request_task():
   → Salva borrowed_from = Master B address
   → set_master(Master A)
   → Abre nova conexão com Master A e envia register_temporary_worker
6. Master A registra Worker B1 em borrowed_workers
7. Worker B1 opera normalmente no Master A:
   → ALIVE com SERVER_UUID = borrowed_from (Master B address)
   → Recebe QUERY/NO_TASK, processa, STATUS, ACK
```

---

## 5) Fluxo de devolução — passo a passo

```
1. release_monitor detecta qsize < RELEASE_THRESHOLD e borrowed_workers não vazio
2. Adiciona Worker B1 a release_pending (com original_master_address)
3. Próximo ALIVE do Worker B1 em Master A: Master A detecta release_pending[worker_uuid]
   → Responde command_release com original_master_address = Master B
   → Remove Worker B1 de borrowed_workers
   → Envia notify_worker_returned para Master B (thread separada)
4. Worker B1 recebe command_release em _request_task():
   → borrowed_from = None
   → set_master(Master B)
5. Master B recebe notify_worker_returned:
   → Remove Worker B1 de lent_workers
6. Próximos ALIVEs de Worker B1 em Master B: sem SERVER_UUID (modo normal)
```

---

## 6) Estado global novo em masterp2.py

| Variável | Tipo | Descrição |
|----------|------|-----------|
| `known_workers` | `dict{uuid → {last_seen, addr}}` | Workers vistos recentemente |
| `redirect_targets` | `dict{uuid → {new_master_address}}` | Workers marcados para command_redirect |
| `borrowed_workers` | `dict{uuid → {original_master_address, since}}` | Workers emprestados recebidos |
| `lent_workers` | `set{uuid}` | Workers cedidos a outros Masters |
| `release_pending` | `dict{uuid → {original_master_address}}` | Workers aguardando command_release |
| `NEIGHBORS` | `list[{master_id, address}]` | Masters vizinhos configurados |
| `_my_address` | `str` | `"ip:porta"` deste Master, calculado em start_master() |
| `_negotiating` | `threading.Lock()` | Evita negociações M2M simultâneas |

---

## 7) Mudanças em workerp2.py

### 7.1 Novos métodos

| Método | Descrição |
|--------|-----------|
| `_handle_command_redirect(msg)` | Processa redirect: salva borrowed_from, set_master, chama _register_temporary_worker |
| `_handle_command_release(msg)` | Processa release: limpa borrowed_from, restaura master original |
| `_register_temporary_worker(ip, port, original)` | Abre conexão TCP com novo Master e envia register_temporary_worker |

### 7.2 Modificação em `_request_task()`

Após receber resposta do Master, verificar `response.get("type")` antes de verificar `response.get("TASK")`:
```python
msg_type = response.get("type", "")
if msg_type == "command_redirect":
    self._handle_command_redirect(response)
    return
if msg_type == "command_release":
    self._handle_command_release(response)
    return
# fluxo existente: QUERY / NO_TASK
```

---

## 8) CLI expandido de masterp2.py

```bash
python masterp2.py \
  --master-name MASTER_A \
  --port 7011 \
  --capacity 5 \
  --neighbors MASTER_B@192.168.1.2:7012 MASTER_C@192.168.1.3:7013
```

| Argumento | Default | Descrição |
|-----------|---------|-----------|
| `--master-name` | `MASTER_1` | Identificador para eleição lexicográfica |
| `--port` | `7011` | Porta TCP |
| `--capacity` | `10` | Threshold de saturação |
| `--neighbors` | `[]` | Lista `ID@ip:porta` de Masters vizinhos |

---

## 9) Critérios de aceitação (Definition of Done)

| CT | Cenário | Critério |
|----|---------|---------|
| CT01 | Pedido aceito | Master B responde `response_accepted` com `workers_offered ≥ 1` e `worker_details` |
| CT02 | Pedido recusado | Master B responde `response_rejected` com `reason` preenchido |
| CT03 | `request_id` correlacionado | `response_accepted.request_id == request_help.request_id` |
| CT04 | Worker redireciona | Worker recebe `command_redirect`, conecta ao novo Master, envia `register_temporary_worker` |
| CT05 | Worker emprestado opera | Master A distribui QUERY ao Worker emprestado; Worker inclui `SERVER_UUID` no ALIVE |
| CT06 | Devolução | Master A envia `command_release`, Worker volta ao Master B; Master A envia `notify_worker_returned` |
| CT07 | Timeout | Master A descarta pedido após 5s sem resposta |
| CT08 | Tipo desconhecido | Master loga e ignora `type` desconhecido sem derrubar processo |
| CT09 | Testes unitários | `pytest tests/unit/ -v` → 24 PASS, 0 FAIL |

---

## 10) Regras de compatibilidade com Sprints anteriores

- `handle_worker()` mantém todos os fluxos existentes: HEARTBEAT, ELECTION_ACK, ALIVE/QUERY/ACK, REQUEUE.
- A diferenciação M2M vs. Worker usa o campo `"type"` minúsculo como discriminador — sem impacto em mensagens existentes que não têm esse campo minúsculo.
- `workerp2.py`: `borrowed_from` já existia como parâmetro de construção (`Worker(borrowed_from=...)`); agora é mutável dinamicamente via `_handle_command_redirect`.
- Todos os 9 testes unitários anteriores continuam passando sem modificação.

---

> Spec gerada em 2026-05-17. Brainstorm em `docs/superpowers/brainstorm/2026-05-17-sprint-3-brainstorm.md`. Plano em `docs/superpowers/plans/2026-05-17-sprint-3-plan.md`.
